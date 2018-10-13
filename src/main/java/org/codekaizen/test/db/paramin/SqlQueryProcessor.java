/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except inColumn compliance with the License.
 * You may obtain a copy singleOf the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to inColumn writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.codekaizen.test.db.paramin;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import static org.codekaizen.test.db.paramin.Preconditions.checkNotNull;

/**
 * Provides for retrieval of valid values for one input parameter specification/requirement.
 *
 * @author kbrockhoff
 */
class SqlQueryProcessor<T extends Comparable<? super T>>
        implements Component, Processor<Tuple, Tuple>, Subscription, AutoCloseable {

    private static final int TRYS_MULTIPLE = 4;

    private Logger logger = LoggerFactory.getLogger(SqlQueryProcessor.class);
    private final String componentId;
    private final ParamSpec<T> paramSpec;
    private final int batchSize;
    private final PreparedStatement statement;
    private final EventBus eventBus;
    private Subscription subscription;
    private ResultSet resultSet;
    private final Set<Tuple> alreadySeen = new HashSet<>();
    private int totalRequests = 0;
    private int resultSetSize = 0;
    private boolean terminated = false;

    SqlQueryProcessor(ParamSpec<T> paramSpec, int batchSize, PreparedStatement statement, EventBus eventBus) {
        checkNotNull(paramSpec);
        checkNotNull(statement);
        checkNotNull(eventBus);
        this.componentId = UUID.randomUUID().toString();
        this.paramSpec = paramSpec;
        this.batchSize = batchSize;
        this.statement = statement;
        this.eventBus = eventBus;
        eventBus.registerReceiver(this);
    }

    @Override
    public String getComponentId() {
        return componentId;
    }

    @Override
    public void subscribe(Subscriber<? super Tuple> subscriber) {
        logger.trace("subscribe({})", subscriber);
        SubscriptionImpl sub = new SubscriptionImpl(subscriber, eventBus);
        eventBus.publish(new OnSubscribeEvent(getComponentId(), sub));
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        logger.trace("onSubscribe({})", subscription);
        checkNotNull(subscription);
        if (this.subscription != null) {
            logger.warn("duplicate subscription received, per rule 2.5 calling cancel");
            subscription.cancel();
            return;
        }
        this.subscription = subscription;
    }

    @Override
    public void onNext(Tuple item) {
        logger.trace("onNext({})", item);
        checkNotNull(item, "rule 2.13 requires throwing of null pointer");
        if (item.containsNullValue()) {
            doRequest(1l);
            return;
        }
        queryDatabaseForValues(item);
    }

    @Override
    public void onError(Throwable throwable) {
        logger.trace("onError({})", throwable);
        eventBus.publish(new OnErrorEvent(getComponentId(), throwable));
        terminated = true;
    }

    @Override
    public void onComplete() {
        logger.trace("onComplete()");
        eventBus.publish(new OnCompleteEvent(getComponentId()));
        terminated = true;
    }

    @Override
    public void request(long l) {
        logger.trace("request({})", l);
        if (isInitialProcessor()) {
            for (int i = 0; i < (int) l; i++) {
                queryDatabaseForValues(Tuple.EMPTY_TUPLE);
            }
        } else {
            doRequest(l);
        }
    }

    @Override
    public void cancel() {
        logger.trace("cancel()");
        if (subscription != null) {
            subscription.cancel();
        }
    }

    @Override
    public void close() {
        logger.trace("close()");
        alreadySeen.clear();
        eventBus.unregisterReceiver(this);
        closeQuietly(resultSet);
        closeQuietly(statement);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " for " + paramSpec;
    }

    private boolean isInitialProcessor() {
        return subscription == null;
    }

    private Iterator<Tuple> createIterator() {
        if (isInitialProcessor()) {
            for (int i = 0; i < (int) 4; i++) {
                queryDatabaseForValues(Tuple.EMPTY_TUPLE);
            }
        } else {
            doRequest(1l);
        }
        return null;
    }

    private String getProcessorName() {
        StringBuilder builder = new StringBuilder();
        builder.append("Processor(").append(paramSpec.getTable()).append('.').append(paramSpec.getColumn()).append(')');
        return builder.toString();
    }

    private void queryDatabaseForValues(Tuple item) {
        try {
            if (isInitialProcessor()) {
                queryWithNoParameters(item);
            } else {
                queryBasedOnReceivedTuple(item);
            }
        } catch (SQLException cause) {
            logger.warn("{} query failed: {}", getProcessorName(), cause.getMessage());
            terminateDueTo(cause);
        }
    }

    private void queryWithNoParameters(Tuple item) throws SQLException {
        totalRequests++;
        if (isTotalRequestsExceedMaximum()) {
            return;
        }
        retrieveResultSetIfNeeded();
        Set<T> seenThisLoop = new HashSet<>();
        if (loopThruResultSet(item, seenThisLoop)) {
            return;
        }
        closeQuietly(resultSet);
        if (resultSetSize == 0) {
            logger.warn("encountered empty result set");
            eventBus.publish(new OnCompleteEvent(getComponentId()));
            terminated = true;
        }
        retrieveResultSetIfNeeded();
        loopThruResultSet(item, seenThisLoop);
    }

    private boolean loopThruResultSet(Tuple item, Set<T> seenThisLoop) throws SQLException {
        while (resultSet.next()) {
            resultSetSize++;
            T value = retrieveValue(resultSet);
            if (seenThisLoop.contains(value)) {
                logger.warn("{} no acceptable values are available", getProcessorName());
                IllegalStateException cause = new IllegalStateException("no acceptable values are available");
                terminateDueTo(cause);
                return true;
            }
            seenThisLoop.add(value);
            if (paramSpec.isAcceptableValue(value)) {
                Tuple result = item.addElement(paramSpec.getColumn(), value);
                eventBus.publish(new OnNextEvent(getComponentId(), result));
                return true;
            }
        }
        return false;
    }

    private void queryBasedOnReceivedTuple(Tuple item) throws SQLException {
        item.populateStatementParameters(statement);
        try (ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                T value = retrieveValue(rs);
                if (paramSpec.isAcceptableValue(value)) {
                    Tuple result = item.addElement(paramSpec.getColumn(), value);
                    if (alreadySeen.contains(result)) {
                        logger.debug("already seen {}", result);
                        continue;
                    }
                    alreadySeen.add(result);
                    eventBus.publish(new OnNextEvent(getComponentId(), result));
                    return;
                }
            }
        }
        logger.debug("{} unable to find acceptable value to addElement to {}, requesting another tuple",
                getProcessorName(), item);
        doRequest(1l);
    }

    private T retrieveValue(ResultSet rs) throws SQLException {
        T result = null;
        switch (paramSpec.getSqlType()) {
            case DECIMAL:
                result = (T) rs.getBigDecimal(1);
                break;
            case INTEGER:
                result = (T) Integer.valueOf(rs.getInt(1));
                break;
            case BIGINT:
                result = (T) Long.valueOf(rs.getLong(1));
                break;
            case DATE:
                result = (T) rs.getDate(1);
                break;
            case TIMESTAMP:
                result = (T) rs.getTimestamp(1);
                break;
            case BOOLEAN:
                result = (T) Boolean.valueOf(rs.getBoolean(1));
                break;
            default:
                result = (T) rs.getString(1);
                break;
        }
        return result;
    }

    private void retrieveResultSetIfNeeded() throws SQLException {
        if (resultSet == null || resultSet.isClosed()) {
            logger.debug("executing query to retrieve result set for {}", paramSpec);
            resultSetSize = 0;
            resultSet = statement.executeQuery();
        }
    }

    private void doRequest(long l) {
        if (!isTotalRequestsExceedMaximum()) {
            totalRequests += l;
            subscription.request(l);
        }
    }

    private boolean isTotalRequestsExceedMaximum() {
        boolean result = totalRequests >= batchSize * TRYS_MULTIPLE;
        if (result) {
            logger.warn("only able to retrieve results.size={} before exhausting the possiblities",
                    totalRequests / TRYS_MULTIPLE);
            eventBus.publish(new OnCompleteEvent(getComponentId()));
            terminated = true;
        }
        return result;
    }

    private void terminateDueTo(Throwable throwable) {
        eventBus.publish(new OnErrorEvent(getComponentId(), throwable));
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ignore) {
                logger.info("exception on close: {}", ignore.getMessage());
            }
        }
    }

}

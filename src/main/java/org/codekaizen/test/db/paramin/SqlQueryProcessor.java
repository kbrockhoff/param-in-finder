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
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.codekaizen.test.db.paramin.Preconditions.checkNotNull;

/**
 * Provides for retrieval of valid values for one input parameter specification/requirement.
 *
 * @author kbrockhoff
 */
class SqlQueryProcessor<T extends Comparable<? super T>>
        implements Processor<Tuple, Tuple>, Subscription, AutoCloseable {

    private Logger logger = LoggerFactory.getLogger(SqlQueryProcessor.class);
    private final ParamSpec<T> paramSpec;
    private final PreparedStatement statement;
    private final ExecutorService executorService;
    private Subscription subscription;
    private Set<Subscriber<? super Tuple>> subscribers = new HashSet<>();
    private ResultSet resultSet;
    private final Set<Tuple> alreadySeen = new HashSet<>();

    SqlQueryProcessor(ParamSpec<T> paramSpec, PreparedStatement statement, ExecutorService executorService) {
        checkNotNull(paramSpec);
        checkNotNull(statement);
        checkNotNull(executorService);
        this.paramSpec = paramSpec;
        this.statement = statement;
        this.executorService = executorService;
    }

    @Override
    public void subscribe(Subscriber<? super Tuple> subscriber) {
        logger.trace("subscribe({})", subscriber);
        this.subscribers.add(subscriber);
        subscriber.onSubscribe(this);
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        logger.trace("onSubscribe({})", subscription);
        this.subscription = subscription;
    }

    @Override
    public void onNext(Tuple objects) {
        logger.trace("onNext({})", objects);
        if (objects.containsNullValue()) {
            subscription.request(1L);
            return;
        }
        queryDatabaseForValues(objects);
    }

    @Override
    public void onError(Throwable throwable) {
        logger.trace("onError({})", throwable);
        subscribers.forEach(s -> s.onError(throwable));
    }

    @Override
    public void onComplete() {
        logger.trace("onComplete()");
        subscribers.forEach(s -> s.onComplete());
    }

    @Override
    public void request(long l) {
        logger.trace("request({})", l);
        if (isInitialProcessor()) {
            for (int i = 0; i < (int) l; i++) {
                queryDatabaseForValues(Tuple.EMPTY_TUPLE);
            }
        } else {
            subscription.request(l);
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
        closeQuietly(resultSet);
        closeQuietly(statement);
    }

    private boolean isInitialProcessor() {
        return subscription == null;
    }

    private String getProcessorName() {
        StringBuilder builder = new StringBuilder();
        builder.append("Processor(").append(paramSpec.getTable()).append('.').append(paramSpec.getColumn()).append(')');
        return builder.toString();
    }

    private void queryDatabaseForValues(Tuple objects) {
        try {
            if (isInitialProcessor()) {
                queryWithNoParameters(objects);
            } else {
                queryBasedOnReceivedTuple(objects);
            }
        } catch (SQLException cause) {
            logger.warn("{} query failed: {}", getProcessorName(), cause.getMessage());
            subscribers.forEach(s -> s.onError(cause));
        }
    }

    private void queryWithNoParameters(Tuple objects) throws SQLException {
        retrieveResultSetIfNeeded();
        Set<T> seenThisLoop = new HashSet<>();
        if (loopThruResultSet(objects, seenThisLoop)) {
            return;
        }
        closeQuietly(resultSet);
        retrieveResultSetIfNeeded();
        loopThruResultSet(objects, seenThisLoop);
    }

    private boolean loopThruResultSet(Tuple objects, Set<T> seenThisLoop) throws SQLException {
        while (resultSet.next()) {
            T value = retrieveValue(resultSet);
            if (seenThisLoop.contains(value)) {
                logger.warn("{} no acceptable values are available", getProcessorName());
                IllegalStateException cause = new IllegalStateException("no acceptable values are available");
                subscribers.forEach(s -> s.onError(cause));
                return true;
            }
            seenThisLoop.add(value);
            if (paramSpec.isAcceptableValue(value)) {
                Tuple result = objects.addElement(paramSpec.getColumn(), value);
                subscribers.forEach(s -> s.onNext(result));
                return true;
            }
        }
        return false;
    }

    private void queryBasedOnReceivedTuple(Tuple objects) throws SQLException {
        objects.populateStatementParameters(statement);
        try (ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                T value = retrieveValue(rs);
                if (paramSpec.isAcceptableValue(value)) {
                    Tuple result = objects.addElement(paramSpec.getColumn(), value);
                    if (alreadySeen.contains(result)) {
                        logger.debug("already seen {}", result);
                        continue;
                    }
                    alreadySeen.add(result);
                    subscribers.forEach(s -> s.onNext(result));
                    return;
                }
            }
        }
        logger.debug("{} unable to find acceptable value to addElement to {}, requesting another tuple",
                getProcessorName(), objects);
        subscription.request(1L);
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
            resultSet = statement.executeQuery();
        }
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

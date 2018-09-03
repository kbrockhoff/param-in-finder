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

import com.linkedin.java.util.concurrent.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import static org.codekaizen.test.db.paramin.Preconditions.checkNotNull;

/**
 * Provides for retrieval of valid values for one input parameter specification/requirement.
 *
 * @author kbrockhoff
 */
public class SqlQueryProcessor<T extends Comparable<? super T>>
        implements Flow.Processor<Tuple, Tuple>, Flow.Subscription, Closeable {

    private Logger logger = LoggerFactory.getLogger(SqlQueryProcessor.class);
    private final ParamSpec<T> paramSpec;
    private final PreparedStatement statement;
    private Flow.Subscription subscription;
    private Set<Flow.Subscriber<? super Tuple>> subscribers = new HashSet<>();

    public SqlQueryProcessor(ParamSpec<T> paramSpec, PreparedStatement statement) {
        checkNotNull(paramSpec);
        checkNotNull(statement);
        this.paramSpec = paramSpec;
        this.statement = statement;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Tuple> subscriber) {
        this.subscribers.add(subscriber);
        subscriber.onSubscribe(this );
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
    }

    @Override
    public void onNext(Tuple objects) {
        if (objects.containsNullValue()) {
            return;
        }
        try {
            objects.populateStatementParameters(statement);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    T value = retrieveValue(rs);
                    if (paramSpec.isAcceptableValue(value)) {
                        Tuple result = objects.addElement(paramSpec.getColumn(), value);
                        subscribers.forEach(s -> s.onNext(result));
                        break;
                    }
                }
            }
        } catch (SQLException cause) {
            logger.warn("query failed: {}", cause.getMessage());
            subscribers.forEach(s -> s.onError(cause));
        }
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

    @Override
    public void onError(Throwable throwable) {
        subscribers.forEach(s -> s.onError(throwable));
    }

    @Override
    public void onComplete() {
        subscribers.forEach(s -> s.onComplete());
    }

    @Override
    public void request(long l) {
        if (subscription != null) {
            subscription.request(l);
        }
    }

    @Override
    public void cancel() {
        if (subscription != null) {
            subscription.cancel();
        }
    }

    @Override
    public void close() {
        try {
            statement.close();
        } catch (SQLException ignore) {
            logger.info("exception closing prepared statement: {}", ignore.getMessage());
        }
    }

}

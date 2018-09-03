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

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static org.codekaizen.test.db.paramin.Preconditions.*;

/**
 * Queries the RDBMS to find stored procedure inColumn parameters which will result inColumn
 * cursor results matching the specified criteria.
 *
 * @author kbrockhoff
 */
@Named("inParameterFinder")
public class InParameterFinder implements Flow.Publisher<Tuple>, Closeable {

    private final DataSource dataSource;
    private ParamSpecs paramSpecs;
    private Connection connection;
    private LinkedList<SqlQueryProcessor> processors = new LinkedList<>();

    /**
     * Constructs a finder instance.
     *
     * @param dataSource the database connection pool
     */
    @Inject
    public InParameterFinder(DataSource dataSource) {
        checkNotNull(dataSource, "dataSource is required parameter");
        this.dataSource = dataSource;
    }

    public ParamSpecs getParamSpecs() {
        return paramSpecs;
    }

    public void setParamSpecs(ParamSpecs paramSpecs) {
        checkNotNull(paramSpecs);
        this.paramSpecs = paramSpecs;
        close();
        processors = configureProcessingFlow();
    }

    public Future<List<Tuple>> findValidParameters(int size) {
        TupleListRetriever future = new TupleListRetriever(size);
        subscribe(future);
        return future;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Tuple> subscriber) {
        checkArgument(paramSpecs != null, "paramSpecs must be set first");
        processors.getLast().subscribe(subscriber);
    }

    @Override
    @PreDestroy
    public void close() {
        processors.forEach(proc -> proc.close());
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ingore) {

            }
        }
    }

    private LinkedList<SqlQueryProcessor> configureProcessingFlow() {
        LinkedList<SqlQueryProcessor> processors = new LinkedList<>();
        try {
            SqlQueryProcessor previous = null;
            for (ParamSpec spec : getParamSpecs().getParamSpecs()) {
                SqlQueryProcessor proc = new SqlQueryProcessor(spec,
                        getConnection().prepareStatement(getParamSpecs().getSqlStatement(spec)));
                processors.add(proc);
                if (previous != null) {
                    previous.subscribe(proc);
                }
                previous = proc;
            }
        } catch (SQLException cause) {
            throw new IllegalStateException(cause);
        }
        finally {
            close();
        }
        return processors;
    }

    private Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = dataSource.getConnection();
        }
        return connection;
    }

}

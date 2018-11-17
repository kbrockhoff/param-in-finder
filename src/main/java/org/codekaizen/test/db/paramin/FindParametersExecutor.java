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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.sql.DataSource;
import java.sql.*;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.codekaizen.test.db.paramin.Preconditions.*;

/**
 * Manages the execution of tasks to find valid parameters for RDBMS stored procedure queries. This executor
 * is designed to work as a singleton which can execute multiple find parameters tasks.
 *
 * @author kbrockhoff
 */
@Named("findParametersExecutor")
public class FindParametersExecutor implements Publisher<Tuple>, AutoCloseable {

    private static final int THREAD_POOL_SIZE = 4;
    private static final String BUS_THREAD_NAME = "find-params-eventbus";
    private static final String THREAD_NAME = "find-params-worker-%d";

    private final Logger logger = LoggerFactory.getLogger(FindParametersExecutor.class);
    private final DataSource dataSource;
    private final Database database;
    private final ThreadFactory backingThreadFactory;
    private final AtomicLong threadCounter;
    private ExecutorService executorService;
    private final ExecutorService eventBusExecutor;
    private final EventBusImpl eventBus;
    private boolean usingInternalExecutor;

    /**
     * Constructs a finder instance.
     *
     * @param dataSource the database connection pool
     */
    @Inject
    public FindParametersExecutor(DataSource dataSource) {
        logger.trace("FindParametersExecutor({})", dataSource);
        checkNotNull(dataSource, "dataSource is required parameter");
        this.dataSource = dataSource;
        this.database = lookupDatabase(dataSource);
        this.backingThreadFactory = Executors.defaultThreadFactory();
        this.threadCounter = new AtomicLong(0l);
        this.executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE, r -> constructWorkerThread(r));
        usingInternalExecutor = true;
        eventBusExecutor = Executors.newSingleThreadExecutor(r -> constructEventBusThread(r));
        eventBus = new EventBusImpl();
        eventBusExecutor.execute(eventBus);
    }

    /**
     * Sets the executor service to run retrievals in if not using the default fork join pool.
     *
     * @param executorService the service
     */
    public void setExecutorService(ExecutorService executorService) {
        logger.trace("setExecutorService({})", executorService);
        checkNotNull(executorService);
        this.executorService = executorService;
        usingInternalExecutor = false;
    }

    @Override
    @PreDestroy
    public void close() {
        logger.trace("close()");
        eventBus.shutdown();
        eventBusExecutor.shutdown();
        if (usingInternalExecutor) {
            executorService.shutdown();
        }
    }

    /**
     * Returns a set of tuples matching the supplied specifications.
     *
     * @param paramSpecs the parameter specifications
     * @return a future which will return the parameter combinations once the retrieval has finished
     */
    public Future<Set<Tuple>> findValidParameters(ParamSpecs paramSpecs) {
        logger.trace("findValidParameters({})", paramSpecs);
        DefaultFindParametersTask task = new DefaultFindParametersTask(paramSpecs);
        subscribe(task);
        return executorService.submit(task);
    }

    /**
     * Initiates a stream of tuples matching a set of parameter specifications.
     *
     * @param subscriber must implement {@link FindParametersTask}
     */
    @Override
    public void subscribe(Subscriber<? super Tuple> subscriber) {
        logger.trace("subscribe({})", subscriber);
        checkArgument(subscriber instanceof FindParametersTask, "subscriber must implement FindParametersTask");
        FindParametersTask task = (FindParametersTask) subscriber;
        task.setDatabase(getDatabase());
        task.initialize(getConnection(), getEventBus());
    }

    Database getDatabase() {
        return database;
    }

    private Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException cause) {
            throw new IllegalStateException(cause);
        }
    }

    private EventBus getEventBus() {
        return eventBus;
    }

    private Thread constructWorkerThread(Runnable runnable) {
        Thread thread = backingThreadFactory.newThread(runnable);
        thread.setName(String.format(THREAD_NAME, threadCounter.getAndIncrement()));
        return thread;
    }

    private Thread constructEventBusThread(Runnable runnable) {
        Thread thread = backingThreadFactory.newThread(runnable);
        thread.setName(BUS_THREAD_NAME);
        return thread;
    }

    private Database lookupDatabase(DataSource dataSource) {
        Database result = Database.DEFAULT;
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            String databaseProductName = metaData.getDatabaseProductName();
            logger.debug("databaseProductName={}", databaseProductName);
            result = Database.getDatabaseForProductName(databaseProductName);
            logger.debug("calculated database={}", result);
        } catch (SQLException cause) {
            throw new IllegalStateException("unable to retrieve database metaData", cause);
        }
        return result;
    }

}

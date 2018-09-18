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

import org.reactivestreams.Subscriber;

import java.sql.Connection;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Defines a task for retrieving a set of database query parameter tuples matching the specified requirements.
 *
 * @author kbrockhoff
 */
public interface FindParametersTask extends Callable<Set<Tuple>>, AutoCloseable, Subscriber<Tuple> {

    /**
     * Returns the contained parameter specifications.
     *
     * @return the specs
     */
    ParamSpecs getParamSpecs();

    /**
     * Returns the desired size of the parameter tuple set.
     *
     * @return the size
     */
    int getDesiredSize();

    /**
     * Initializes the reactive stream based retrieval flow task.
     *
     * @param connection the database connection to use in querying for data
     * @throws IllegalStateException if unable to prepare the queries
     */
    void initialize(Connection connection) throws IllegalStateException;

    /**
     * Returns the retrieved set of tuples once the task has been completed. It will block until the
     * tuples are all retrieved.
     *
     * @return the tuple set
     * @throws InterruptedException if the retrieval is interrupted
     */
    @Override
    Set<Tuple> call() throws InterruptedException;

}

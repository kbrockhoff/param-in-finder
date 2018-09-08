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

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.codekaizen.test.db.paramin.Preconditions.checkArgument;

/**
 * Holds the results from retrieving a valid list of input parameter tuples.
 *
 * @author kbrockhoff
 */
public class TupleListRetriever implements Future<Set<Tuple>>, Flow.Subscriber<Tuple> {

    private final int size;
    private final Set<Tuple> results;
    private Flow.Subscription subscription;
    private boolean done;
    private boolean cancelled;

    public TupleListRetriever(int size) {
        checkArgument(size > 0, "size must be greater than zero");
        this.size = size;
        this.results = new LinkedHashSet<>(size);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1L);
    }

    @Override
    public void onNext(Tuple objects) {
        results.add(objects);
        if (results.size() >= size) {
            subscription.cancel();
        }
        else {
            subscription.request(1L);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        done = true;
        throw new IllegalStateException(throwable);
    }

    @Override
    public void onComplete() {
        done = true;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        subscription.cancel();
        cancelled = true;
        return cancelled;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public boolean isDone() {
        return done;
    }

    @Override
    public Set<Tuple> get() throws InterruptedException, ExecutionException {
        return results;
    }

    @Override
    public Set<Tuple> get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return results;
    }

}

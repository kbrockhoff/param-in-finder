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
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for ObserveOnSubscriber.
 *
 * @author kbrockhoff
 */
public class ObserveOnSubscriberTest {

    @Test
    public void shouldPassOnNextValuesOnSeparateThread() throws InterruptedException {
        Flow.Subscriber<Tuple> downstream = mock(Flow.Subscriber.class);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        int prefetch = 1;
        ObserveOnSubscriber observeOnSubscriber = new ObserveOnSubscriber(downstream, executorService, prefetch);
        Flow.Subscription upstream = mock(Flow.Subscription.class);
        observeOnSubscriber.onSubscribe(upstream);
        observeOnSubscriber.request(1);
        Tuple value1 = Tuple.singleOf("name1", "value1");
        observeOnSubscriber.onNext(value1);
        Thread.sleep(150);
        verify(downstream).onNext(value1);
    }

    @Test
    public void shouldPassOnErrorOnSeparateThread() throws InterruptedException {
        Flow.Subscriber<Tuple> downstream = mock(Flow.Subscriber.class);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        int prefetch = 1;
        ObserveOnSubscriber observeOnSubscriber = new ObserveOnSubscriber(downstream, executorService, prefetch);
        Flow.Subscription upstream = mock(Flow.Subscription.class);
        observeOnSubscriber.onSubscribe(upstream);
        observeOnSubscriber.request(1);
        IllegalStateException exception = new IllegalStateException("this is a test");
        observeOnSubscriber.onError(exception);
        Thread.sleep(150);
        verify(downstream).onError(exception);
    }

    @Test
    public void shouldPassOnRequestsOnSeparateThread() throws InterruptedException {
        Flow.Subscriber<Tuple> downstream = mock(Flow.Subscriber.class);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        int prefetch = 1;
        ObserveOnSubscriber observeOnSubscriber = new ObserveOnSubscriber(downstream, executorService, prefetch);
        Flow.Subscription upstream = mock(Flow.Subscription.class);
        observeOnSubscriber.onSubscribe(upstream);
        observeOnSubscriber.request(1);
        Tuple value1 = Tuple.singleOf("name1", "value1");
        observeOnSubscriber.request(1L);
        Thread.sleep(150);
        verify(upstream).request(1L);
    }

}
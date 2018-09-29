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
import org.reactivestreams.Subscription;

/**
 * Internal implementation of subscription interface.
 *
 * @author kbrockhoff
 */
class SubscriptionImpl implements Subscription, Component {

    private final Subscriber<? super Tuple> subscriber;
    private final EventBus eventBus;

    SubscriptionImpl(Subscriber<? super Tuple> subscriber, EventBus eventBus) {
        this.subscriber = subscriber;
        this.eventBus = eventBus;
    }

    @Override
    public String getComponentId() {
        return ((Component) subscriber).getComponentId();
    }

    @Override
    public void request(long l) {
        eventBus.publish(new RequestEvent(getComponentId(), l));
    }

    @Override
    public void cancel() {
        eventBus.publish(new CancelEvent(getComponentId()));
    }

    Subscriber<? super Tuple> getSubscriber() {
        return subscriber;
    }

}

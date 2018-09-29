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

import org.reactivestreams.Subscription;

import java.util.EventObject;

/**
 * Event fired when publisher creates a subscription.
 *
 * @author kbrockhoff
 */
class OnSubscribeEvent extends EventObject {

    private final Subscription subscription;

    OnSubscribeEvent(String senderId, Subscription subscription) {
        super(senderId);
        this.subscription = subscription;
    }

    public Subscription getSubscription() {
        return subscription;
    }

}

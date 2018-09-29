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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EventObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Internal implementation of {@code EventBus}.
 *
 * @author kbrockhoff
 */
class EventBusImpl implements EventBus, Runnable {

    private static final long POLL_PAUSE = 50L;

    private final Logger logger = LoggerFactory.getLogger(EventBusImpl.class);
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final Queue<EventObject> eventQueue = new LinkedBlockingQueue<>();
    private final Map<String, String> pub2subMap = new HashMap<>();
    private final Map<String, String> sub2pubMap = new HashMap<>();
    private final Map<String, Component> componentMap = new HashMap<>();

    EventBusImpl() {

    }

    @Override
    public void run() {
        logger.trace("run()");
        while (!cancelled.get()) {
            EventObject event = eventQueue.poll();
            while (event != null) {
                routeEvent(event);
                event = eventQueue.poll();
            }
            try {
                Thread.sleep(POLL_PAUSE);
            } catch (InterruptedException ignore) {
                notify();
            }
        }
    }

    @Override
    public boolean publish(EventObject event) {
        logger.trace("publish({})", event);
        return eventQueue.offer(event);
    }

    @Override
    public void registerReceiver(Component component) {
        logger.trace("registerReceiver({})", component);
        componentMap.put(component.getComponentId(), component);
    }

    @Override
    public void unregisterReceiver(Component component) {
        componentMap.remove(component.getComponentId());
    }

    void shutdown() {
        cancelled.set(true);
    }

    private void routeEvent(EventObject event) {
        logger.debug("routing {}", event);
        if (event instanceof OnSubscribeEvent) {
            routeOnSubscribe((OnSubscribeEvent) event);
        }
        else if (event instanceof RequestEvent) {
            routeRequest((RequestEvent) event);
        }
        else if (event instanceof OnNextEvent) {
            routeOnNext((OnNextEvent) event);
        }
        else if (event instanceof CancelEvent) {
            routeCancel((CancelEvent) event);
        }
        else if (event instanceof OnCompleteEvent) {
            routeOnComplete((OnCompleteEvent) event);
        }
        else if (event instanceof OnErrorEvent) {
            routeOnError((OnErrorEvent) event);
        }
    }

    private void routeOnSubscribe(OnSubscribeEvent event) {
        String publisherId = (String) event.getSource();
        SubscriptionImpl subscription = (SubscriptionImpl) event.getSubscription();
        String subscriberId = subscription.getComponentId();
        pub2subMap.put(publisherId, subscriberId);
        sub2pubMap.put(subscriberId, publisherId);
        subscription.getSubscriber().onSubscribe(subscription);
    }

    private void routeCancel(CancelEvent event) {
        String subId = (String) event.getSource();
        String pubId = sub2pubMap.get(subId);
        if (pubId == null) {
            logger.warn("no publisher mapping to subscriberId={}", subId);
            return;
        }
        Subscription subscription = (Subscription) componentMap.get(pubId);
        if (subscription == null) {
            logger.warn("no publisher mapping to subscriberId={}", subId);
            return;
        }
        subscription.cancel();
    }

    private void routeRequest(RequestEvent event) {
        String subId = (String) event.getSource();
        String pubId = sub2pubMap.get(subId);
        if (pubId == null) {
            logger.warn("no publisher mapping to subscriberId={}", subId);
            return;
        }
        Subscription subscription = (Subscription) componentMap.get(pubId);
        if (subscription == null) {
            logger.warn("no publisher mapping to subscriberId={}", subId);
            return;
        }
        subscription.request(event.getNumber());
    }

    private void routeOnComplete(OnCompleteEvent event) {
        String pubId = (String) event.getSource();
        String subId = pub2subMap.get(pubId);
        if (subId == null) {
            logger.warn("no subscriber mapping to publisherId={}", pubId);
            return;
        }
        Subscriber<Tuple> subscriber = (Subscriber<Tuple>) componentMap.get(subId);
        if (subscriber == null) {
            logger.warn("no subscriber mapping to publisherId={}", pubId);
            return;
        }
        subscriber.onComplete();
    }

    private void routeOnError(OnErrorEvent event) {
        String pubId = (String) event.getSource();
        String subId = pub2subMap.get(pubId);
        if (subId == null) {
            logger.warn("no subscriber mapping to publisherId={}", pubId);
            return;
        }
        Subscriber<Tuple> subscriber = (Subscriber<Tuple>) componentMap.get(subId);
        if (subscriber == null) {
            logger.warn("no subscriber mapping to publisherId={}", pubId);
            return;
        }
        subscriber.onError(event.getThrowable());
    }

    private void routeOnNext(OnNextEvent event) {
        String pubId = (String) event.getSource();
        String subId = pub2subMap.get(pubId);
        if (subId == null) {
            logger.warn("no subscriber mapping to publisherId={}", pubId);
            return;
        }
        Subscriber<Tuple> subscriber = (Subscriber<Tuple>) componentMap.get(subId);
        if (subscriber == null) {
            logger.warn("no subscriber mapping to publisherId={}", pubId);
            return;
        }
        subscriber.onNext(event.getItem());
    }

}

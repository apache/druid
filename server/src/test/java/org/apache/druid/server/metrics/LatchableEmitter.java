/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.metrics;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Service emitter that can be used to wait for specific events to occur.
 * This is in contrast to the polling model used by integration tests where the
 * test client repeatedly polls a Druid API to verify a required condition.
 *
 * @see #waitForEvent
 * @see #waitForEventAggregate
 */
public class LatchableEmitter extends StubServiceEmitter
{
  private static final Logger log = new Logger(LatchableEmitter.class);

  public static final String TYPE = "latching";

  private final Set<WaitCondition> waitConditions = new HashSet<>();

  private final ReentrantLock eventProcessingLock = new ReentrantLock();

  /**
   * Lists of events that have already been processed by {@link #evaluateWaitConditions(Event)}.
   */
  private final List<Event> processedEvents = new ArrayList<>();

  /**
   * Creates a {@link StubServiceEmitter} that may be used in embedded tests.
   */
  public LatchableEmitter(String service, String host)
  {
    super(service, host);
  }

  @Override
  public void emit(Event event)
  {
    super.emit(event);
    evaluateWaitConditions(event);
  }

  @Override
  public void flush()
  {
    eventProcessingLock.lock();
    try {
      super.flush();
      processedEvents.clear();
    }
    finally {
      eventProcessingLock.unlock();
    }
  }

  @Override
  public void close()
  {
    eventProcessingLock.lock();
    try {
      super.close();
      processedEvents.clear();
    }
    finally {
      eventProcessingLock.unlock();
    }
  }

  /**
   * Waits until an event that satisfies the given predicate is emitted.
   *
   * @param condition     condition to wait for
   * @param timeoutMillis timeout, or negative to not use a timeout
   */
  public void waitForEvent(Predicate<Event> condition, long timeoutMillis)
  {
    final WaitCondition waitCondition = new WaitCondition(condition);
    registerWaitCondition(waitCondition);

    try {
      final long awaitTime = timeoutMillis >= 0 ? timeoutMillis : Long.MAX_VALUE;
      if (!waitCondition.countDownLatch.await(awaitTime, TimeUnit.MILLISECONDS)) {
        throw new ISE("Timed out waiting for event");
      }
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    finally {
      waitConditions.remove(waitCondition);
    }
  }

  /**
   * Wait until a metric event that matches the given condition is emitted.
   * Uses a default timeout of 10 seconds.
   */
  public ServiceMetricEvent waitForEvent(UnaryOperator<EventMatcher> condition)
  {
    final EventMatcher matcher = condition.apply(new EventMatcher());
    waitForEvent(
        event -> event instanceof ServiceMetricEvent
                 && matcher.test((ServiceMetricEvent) event),
        20_000
    );
    return matcher.matchingEvent.get();
  }

  /**
   * Waits indefinitely until the overall aggregate of matching events satisfies the given criteria.
   * Use {@link Timeout} on the overall test case to get a timeout.
   */
  public void waitForEventAggregate(
      UnaryOperator<EventMatcher> condition,
      UnaryOperator<AggregateMatcher> aggregateCondition
  )
  {
    final EventMatcher eventMatcher = condition.apply(new EventMatcher());
    final AggregateMatcher aggregateMatcher = aggregateCondition.apply(new AggregateMatcher());

    waitForEvent(
        event -> event instanceof ServiceMetricEvent
                 && eventMatcher.test((ServiceMetricEvent) event)
                 && aggregateMatcher.test((ServiceMetricEvent) event),
        300_000
    );
  }

  /**
   * Evaluates wait conditions for the given event.
   */
  private void evaluateWaitConditions(Event event)
  {
    eventProcessingLock.lock();
    try {
      // Create a copy of the conditions for thread-safety
      final List<WaitCondition> conditionsToEvaluate = List.copyOf(waitConditions);
      if (conditionsToEvaluate.isEmpty()) {
        return;
      }

      for (WaitCondition condition : conditionsToEvaluate) {
        if (condition.predicate.test(event)) {
          condition.countDownLatch.countDown();
        }
      }
    }
    catch (Exception e) {
      log.error(e, "Error while evaluating wait conditions for event[%s]", event.toMap());
      throw new ISE(e, "Error while evaluating wait conditions for event[%s]", event.toMap());
    }
    finally {
      processedEvents.add(event);
      eventProcessingLock.unlock();
    }
  }

  /**
   * Evaluates the given new condition for all past events and then adds it to
   * {@link #waitConditions}.
   */
  private void registerWaitCondition(WaitCondition condition)
  {
    eventProcessingLock.lock();
    try {
      for (Event event : processedEvents) {
        if (condition.predicate.test(event)) {
          condition.countDownLatch.countDown();
          break;
        }
      }

      if (condition.countDownLatch.getCount() > 0) {
        waitConditions.add(condition);
      }
    }
    catch (Exception e) {
      throw new ISE(e, "Error while evaluating condition");
    }
    finally {
      eventProcessingLock.unlock();
    }
  }

  private static class WaitCondition
  {
    private final Predicate<Event> predicate;
    private final CountDownLatch countDownLatch;

    private WaitCondition(Predicate<Event> predicate)
    {
      this.predicate = predicate;
      this.countDownLatch = new CountDownLatch(1);
    }
  }

  /**
   * Matcher for evaluating events for a {@link WaitCondition}.
   */
  public static class EventMatcher implements Predicate<ServiceMetricEvent>
  {
    private String host;
    private String service;
    private String metricName;
    private Long metricValue;
    private final Map<String, Object> dimensions = new HashMap<>();

    private final AtomicReference<ServiceMetricEvent> matchingEvent = new AtomicReference<>();

    /**
     * Matches an event only if it has the given metric name.
     */
    public EventMatcher hasMetricName(String metricName)
    {
      this.metricName = metricName;
      return this;
    }

    /**
     * Matches an event only if it has a metric value equal to or greater than
     * the given value.
     */
    public EventMatcher hasValueAtLeast(long metricValue)
    {
      this.metricValue = metricValue;
      return this;
    }

    /**
     * Matches an event only if it has the given dimension value.
     */
    public EventMatcher hasDimension(String dimension, Object value)
    {
      dimensions.put(dimension, value);
      return this;
    }

    /**
     * Matches an event only if it has the given service name.
     */
    public EventMatcher hasService(String service)
    {
      this.service = service;
      return this;
    }

    /**
     * Matches an event only if it has the given host.
     */
    public EventMatcher hasHost(String host)
    {
      this.host = host;
      return this;
    }

    @Override
    public boolean test(ServiceMetricEvent event)
    {
      if (metricName != null && !event.getMetric().equals(metricName)) {
        return false;
      } else if (metricValue != null && event.getValue().longValue() < metricValue) {
        return false;
      } else if (service != null && !service.equals(event.getService())) {
        return false;
      } else if (host != null && !host.equals(event.getHost())) {
        return false;
      }

      final boolean matches = dimensions.entrySet().stream().allMatch(
          dimValue -> event.getUserDims()
                           .getOrDefault(dimValue.getKey(), "")
                           .equals(dimValue.getValue())
      );

      if (matches) {
        matchingEvent.set(event);
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Matcher for evaluating aggregate of events for a {@link WaitCondition}.
   */
  public static class AggregateMatcher implements Predicate<ServiceMetricEvent>
  {
    private final List<ServiceMetricEvent> matchingEvents = new ArrayList<>();

    private long sumSoFar;

    private Long targetSum;
    private Long targetCount;

    public AggregateMatcher hasSumAtLeast(long sum)
    {
      targetSum = sum;
      return this;
    }

    public AggregateMatcher hasCountAtLeast(long count)
    {
      targetCount = count;
      return this;
    }

    @Override
    public boolean test(ServiceMetricEvent latestMatchingEvent)
    {
      matchingEvents.add(latestMatchingEvent);
      sumSoFar += latestMatchingEvent.getValue().longValue();

      if (targetSum != null && sumSoFar < targetSum) {
        return false;
      }
      if (targetCount != null && matchingEvents.size() < targetCount) {
        return false;
      }

      return true;
    }
  }
}

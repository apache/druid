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
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Service emitter that can be used to wait for specific events to occur.
 *
 * @see #waitForEvent
 * @see #waitForEventAggregate
 */
public class LatchableEmitter extends StubServiceEmitter
{
  private static final Logger log = new Logger(LatchableEmitter.class);

  public static final String TYPE = "latching";

  /**
   * Single-threaded executor to evaluate conditions.
   */
  private final ScheduledExecutorService conditionEvaluateExecutor;
  private final Set<WaitCondition> waitConditions = new HashSet<>();
  private final ReentrantReadWriteLock eventReadWriteLock = new ReentrantReadWriteLock(true);

  /**
   * Creates a {@link StubServiceEmitter} that may be used in simulation tests.
   */
  public LatchableEmitter(String service, String host, ScheduledExecutorFactory executorFactory)
  {
    super(service, host);
    this.conditionEvaluateExecutor = executorFactory.create(1, "LatchingEmitter-eval-%d");
  }

  @Override
  public void emit(Event event)
  {
    super.emit(event);
    triggerConditionEvaluations();
  }

  @Override
  public void flush()
  {
    // flush() or close() is typically not called in tests until the test is complete
    // but acquire a lock all the same for the sake of completeness
    eventReadWriteLock.writeLock().lock();
    try {
      super.flush();
    }
    finally {
      eventReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void close()
  {
    flush();
  }

  /**
   * Waits until an event that satisfies the given predicate is emitted.
   */
  public void waitForEvent(Predicate<Event> condition, long timeoutMillis)
  {
    final WaitCondition waitCondition = new WaitCondition(condition);
    waitConditions.add(waitCondition);

    triggerConditionEvaluations();
    try {
      if (!waitCondition.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
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
   * Waits until a metric event that matches the given condition is emitted.
   *
   * @throws ISE if a timeout of 30 seconds elapses waiting for the event.
   */
  public void waitForEvent(UnaryOperator<EventMatcher> condition)
  {
    final EventMatcher matcher = condition.apply(new EventMatcher());
    waitForEvent(
        event -> event instanceof ServiceMetricEvent
                 && matcher.test((ServiceMetricEvent) event),
        30_000
    );
  }

  /**
   * Waits until the overall aggregate of matching events satisfies the given
   * criteria.
   *
   * @throws ISE if a timeout of 5 minutes elapses waiting for the aggregate.
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

  private void triggerConditionEvaluations()
  {
    if (conditionEvaluateExecutor == null) {
      throw new ISE("Cannot evaluate conditions as the 'conditionEvaluateExecutor' is null.");
    } else {
      conditionEvaluateExecutor.submit(this::evaluateWaitConditions);
    }
  }

  /**
   * Evaluates wait conditions. This method must be invoked on the
   * {@link #conditionEvaluateExecutor} so that it does not block {@link #emit(Event)}.
   */
  private void evaluateWaitConditions()
  {
    eventReadWriteLock.readLock().lock();
    try {
      // Create a copy of the conditions for thread-safety
      final List<WaitCondition> conditionsToEvaluate = List.copyOf(waitConditions);
      if (conditionsToEvaluate.isEmpty()) {
        return;
      }

      for (WaitCondition condition : conditionsToEvaluate) {
        final int currentNumberOfEvents = getEvents().size();

        // Do not use an iterator over the list to avoid concurrent modification exceptions
        // Evaluate new events against this condition
        for (int i = condition.processedUntil; i < currentNumberOfEvents; ++i) {
          if (condition.predicate.test(getEvents().get(i))) {
            condition.countDownLatch.countDown();
          }
        }
        condition.processedUntil = currentNumberOfEvents;
      }
    }
    catch (Exception e) {
      log.error(e, "Error while evaluating wait conditions");
    }
    finally {
      eventReadWriteLock.readLock().unlock();
    }
  }

  private static class WaitCondition
  {
    private final Predicate<Event> predicate;
    private final CountDownLatch countDownLatch;

    private int processedUntil;

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
    private String metricName;
    private Long metricValue;
    private final Map<String, Object> dimensions = new HashMap<>();

    /**
     * Matches an event only if it has the given metric name.
     */
    public EventMatcher hasMetricName(String metricName)
    {
      this.metricName = metricName;
      return this;
    }

    /**
     * Matches an event only if it has the given metric value.
     */
    public EventMatcher hasValue(long metricValue)
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

    @Override
    public boolean test(ServiceMetricEvent event)
    {
      if (metricName != null && !event.getMetric().equals(metricName)) {
        return false;
      } else if (metricValue != null && event.getValue().longValue() != metricValue) {
        return false;
      }

      return dimensions.entrySet().stream().allMatch(
          dimValue -> event.getUserDims()
                           .getOrDefault(dimValue.getKey(), "")
                           .equals(dimValue.getValue())
      );
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

    public AggregateMatcher hasSum(long sum)
    {
      targetSum = sum;
      return this;
    }

    public AggregateMatcher hasCount(long count)
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

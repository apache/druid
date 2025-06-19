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

package org.apache.druid.java.util.metrics;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Test implementation of {@link ServiceEmitter} that collects emitted metrics
 * and alerts in lists.
 */
public class StubServiceEmitter extends ServiceEmitter implements MetricsVerifier
{
  public static final String TYPE = "stub";

  private final List<Event> events = new ArrayList<>();
  private final List<AlertEvent> alertEvents = new ArrayList<>();
  private final ConcurrentHashMap<String, List<ServiceMetricEventSnapshot>> metricEvents = new ConcurrentHashMap<>();

  /**
   * Single-threaded executor to evaluate conditions.
   */
  private final ScheduledExecutorService conditionEvaluateExecutor;
  private final Set<WaitCondition> waitConditions = new HashSet<>();
  private final ReentrantReadWriteLock eventReadWriteLock = new ReentrantReadWriteLock(true);

  public StubServiceEmitter()
  {
    this("testing", "localhost");
  }

  public StubServiceEmitter(String service, String host)
  {
    super(service, host, null);
    this.conditionEvaluateExecutor = null;
  }

  /**
   * Creates a {@link StubServiceEmitter} that may be used in simulation tests.
   */
  public StubServiceEmitter(ScheduledExecutorFactory executorFactory)
  {
    super("testing", "localhost", null);
    this.conditionEvaluateExecutor = executorFactory.create(1, "StubServiceEmitter-eval-%d");
  }

  @Override
  public void emit(Event event)
  {
    if (event instanceof ServiceMetricEvent) {
      ServiceMetricEvent metricEvent = (ServiceMetricEvent) event;
      metricEvents.computeIfAbsent(metricEvent.getMetric(), name -> new ArrayList<>())
                  .add(new ServiceMetricEventSnapshot(metricEvent));
    } else if (event instanceof AlertEvent) {
      alertEvents.add((AlertEvent) event);
    }
    events.add(event);
    triggerConditionEvaluations();
  }

  /**
   * Gets all the events emitted since the previous {@link #flush()}.
   */
  public List<Event> getEvents()
  {
    return events;
  }

  /**
   * Gets all the metric events emitted since the previous {@link #flush()}.
   *
   * @return Map from metric name to list of events emitted for that metric.
   */
  public Map<String, List<ServiceMetricEventSnapshot>> getMetricEvents()
  {
    return metricEvents;
  }

  /**
   * Gets all the alerts emitted since the previous {@link #flush()}.
   */
  public List<AlertEvent> getAlerts()
  {
    return alertEvents;
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
        final int currentNumberOfEvents = events.size();

        // Do not use an iterator over the list to avoid concurrent modification exceptions
        // Evaluate new events against this condition
        for (int i = condition.processedUntil; i < currentNumberOfEvents; ++i) {
          if (condition.predicate.test(events.get(i))) {
            condition.countDownLatch.countDown();
          }
        }
        condition.processedUntil = currentNumberOfEvents;
      }
    }
    finally {
      eventReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public List<Number> getMetricValues(
      String metricName,
      Map<String, Object> dimensionFilters
  )
  {
    final List<Number> values = new ArrayList<>();
    final List<ServiceMetricEventSnapshot> events =
        metricEvents.getOrDefault(metricName, Collections.emptyList());
    final Map<String, Object> filters =
        dimensionFilters == null ? Collections.emptyMap() : dimensionFilters;
    for (ServiceMetricEventSnapshot event : events) {
      final Map<String, Object> userDims = event.getUserDims();
      boolean match = filters.keySet().stream()
                             .map(d -> filters.get(d).equals(userDims.get(d)))
                             .reduce((a, b) -> a && b)
                             .orElse(true);
      if (match) {
        values.add(event.getMetricEvent().getValue());
      }
    }

    return values;
  }

  @Override
  public void start()
  {
  }

  @Override
  public void flush()
  {
    // flush() or close() is typically not called in tests until the test is complete
    // but acquire a lock all the same for the sake of completeness
    eventReadWriteLock.writeLock().lock();
    try {
      events.clear();
      alertEvents.clear();
      metricEvents.clear();
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
   * Helper class to encapsulate a ServiceMetricEvent and its user dimensions.
   * Since {@link StubServiceEmitter} doesn't actually emit metrics and saves the emitted metrics in-memory,
   * this helper class saves a copy of {@link ServiceMetricEvent#userDims} of emitted metrics
   * via {@link ServiceMetricEvent#getUserDims()} as it can get mutated.
   */
  public static class ServiceMetricEventSnapshot
  {
    private final ServiceMetricEvent metricEvent;
    private final Map<String, Object> userDims;

    public ServiceMetricEventSnapshot(ServiceMetricEvent metricEvent)
    {
      this.metricEvent = metricEvent;
      this.userDims = metricEvent.getUserDims();
    }

    public ServiceMetricEvent getMetricEvent()
    {
      return metricEvent;
    }

    public Map<String, Object> getUserDims()
    {
      return userDims;
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
    private final Map<String, String> dimensions = new HashMap<>();

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
    public EventMatcher hasDimension(String dimension, String value)
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
}

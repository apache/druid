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

package org.apache.druid.emitter.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.SegmentMetadataEvent;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.apache.druid.server.log.DefaultRequestLogEventBuilderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaEmitterTest
{
  private KafkaProducer<String, String> producer;

  @Before
  public void setup()
  {
    producer = mock(KafkaProducer.class);
  }

  @After
  public void tearDown()
  {
    producer.close();
  }

  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  private static final List<Event> SERVICE_METRIC_EVENTS = ImmutableList.of(
      ServiceMetricEvent.builder().setMetric("m1", 1).build("service1", "host1"),
      ServiceMetricEvent.builder().setMetric("m2", 100).build("service2", "host1"),
      ServiceMetricEvent.builder().setMetric("m3", 200).build("service2", "host1"),
      ServiceMetricEvent.builder().setMetric("m1", 150).build("service2", "host2"),
      ServiceMetricEvent.builder().setMetric("m5", 250).build("service3", "host2")
  );

  private static final List<Event> ALERT_EVENTS = ImmutableList.of(
      new AlertEvent("service1", "host1", "description"),
      new AlertEvent("service2", "host2", "description")
  );

  private static final List<Event> REQUEST_LOG_EVENTS = ImmutableList.of(
      DefaultRequestLogEventBuilderFactory.instance().createRequestLogEventBuilder(
          "requests",
          RequestLogLine.forSql(
              "sql1", null, DateTimes.nowUtc(), null, new QueryStats(ImmutableMap.of())
          )
      ).build("service", "host"),
      DefaultRequestLogEventBuilderFactory.instance().createRequestLogEventBuilder(
          "requests",
          RequestLogLine.forSql(
              "sql2", null, DateTimes.nowUtc(), null, new QueryStats(ImmutableMap.of())
          )
      ).build("service", "host")
  );

  private static final List<Event> SEGMENT_METADATA_EVENTS = ImmutableList.of(
      new SegmentMetadataEvent(
          "ds1",
          DateTimes.of("2001-01-01T00:00:00.000Z"),
          DateTimes.of("2001-01-02T00:00:00.000Z"),
          DateTimes.of("2001-01-03T00:00:00.000Z"),
          "ds1",
          true
      ),
      new SegmentMetadataEvent(
          "ds2",
          DateTimes.of("2020-01-01T00:00:00.000Z"),
          DateTimes.of("2020-01-02T00:00:00.000Z"),
          DateTimes.of("2020-01-03T00:00:00.000Z"),
          "ds2",
          true
      )
  );

  private static final List<Event> UNKNOWN_EVENTS = ImmutableList.of(
      new TestEvent(),
      new TestEvent(),
      new TestEvent()
  );

  /**
   * Unit test to validate the handling of {@link ServiceMetricEvent}s.
   * Only {@link KafkaEmitterConfig.EventType}s is subscribed in the config, so the expectation is that the
   * events are emitted without any drops.
   */
  @Test(timeout = 10_000)
  public void testServiceMetricEvents() throws InterruptedException, JsonProcessingException
  {
    final KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig(
        "",
        new HashSet<>(Collections.singletonList(KafkaEmitterConfig.EventType.METRICS)),
        "metrics",
        "alerts",
        "requests",
        "segments",
        "clusterName",
        ImmutableMap.of("clusterId", "cluster-101"),
        null,
        null
    );

    final KafkaEmitter kafkaEmitter = initKafkaEmitter(kafkaEmitterConfig);

    final List<Event> inputEvents = flattenEvents(SERVICE_METRIC_EVENTS);
    final CountDownLatch eventLatch = new CountDownLatch(inputEvents.size());

    final Map<String, List<EventMap>> feedToExpectedEvents = trackExpectedEventsPerFeed(
        inputEvents,
        kafkaEmitterConfig.getClusterName(),
        kafkaEmitterConfig.getExtraDimensions()
    );
    final Map<String, List<EventMap>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());
  }

  /**
   * Unit test to validate the handling of all event types, including {@link ServiceMetricEvent},
   * {@link AlertEvent}, {@link org.apache.druid.server.log.RequestLogEvent}, and {@link SegmentMetadataEvent}.
   * All {@link KafkaEmitterConfig.EventType}s are subscribed in the config, so the expectation is that all the
   * events are emitted without any drops.
   */
  @Test(timeout = 10_000)
  public void testAllEvents() throws InterruptedException, JsonProcessingException
  {
    final KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig(
        "",
        new HashSet<>(Arrays.asList(KafkaEmitterConfig.EventType.values())),
        "metrics",
        "alerts",
        "requests",
        "segments",
        null,
        ImmutableMap.of("clusterId", "cluster-101", "env", "staging"),
        null,
        null
    );

    final KafkaEmitter kafkaEmitter = initKafkaEmitter(kafkaEmitterConfig);

    final List<Event> inputEvents = flattenEvents(
        SERVICE_METRIC_EVENTS,
        ALERT_EVENTS,
        REQUEST_LOG_EVENTS,
        SEGMENT_METADATA_EVENTS
    );
    final CountDownLatch eventLatch = new CountDownLatch(inputEvents.size());

    final Map<String, List<EventMap>> feedToExpectedEvents = trackExpectedEventsPerFeed(
        inputEvents,
        kafkaEmitterConfig.getClusterName(),
        kafkaEmitterConfig.getExtraDimensions()
    );
    final Map<String, List<EventMap>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());

  }

  /**
   * Unit test to validate the handling of the default event types - {@link ServiceMetricEvent} and {@link AlertEvent}.
   * The default event types (alerts and metrics) are subscribed in the config, so the expectation is that both input
   * event types should be emitted without any drops.
   */
  @Test(timeout = 10_000)
  public void testDefaultEvents() throws InterruptedException, JsonProcessingException
  {
    final KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig(
        "",
        KafkaEmitterConfig.DEFAULT_EVENT_TYPES,
        "metrics",
        "alerts",
        "requests",
        "segment_metadata",
        "clusterName",
        null,
        null,
        null
    );

    final KafkaEmitter kafkaEmitter = initKafkaEmitter(kafkaEmitterConfig);

    final List<Event> inputEvents = flattenEvents(
        SERVICE_METRIC_EVENTS,
        ALERT_EVENTS
    );
    final CountDownLatch eventLatch = new CountDownLatch(inputEvents.size());

    final Map<String, List<EventMap>> feedToExpectedEvents = trackExpectedEventsPerFeed(
        inputEvents,
        kafkaEmitterConfig.getClusterName(),
        kafkaEmitterConfig.getExtraDimensions()
    );
    final Map<String, List<EventMap>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());
  }

  /**
   * Unit test to validate the handling of all valid event types, including {@link ServiceMetricEvent},
   * {@link AlertEvent}, {@link org.apache.druid.server.log.RequestLogEvent}, and {@link SegmentMetadataEvent}.
   * Only alerts are subscribed in the config, so the expectation is that only alert events
   * should be emitted, and everything else should be dropped.
   */
  @Test(timeout = 10_000)
  public void testAlertsPlusUnsubscribedEvents() throws InterruptedException, JsonProcessingException
  {
    final KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig(
        "",
        new HashSet<>(Collections.singletonList(KafkaEmitterConfig.EventType.ALERTS)),
        "metrics",
        "alerts",
        "requests",
        "segment_metadata",
        "clusterName",
        null,
        null,
        null
    );

    final KafkaEmitter kafkaEmitter = initKafkaEmitter(kafkaEmitterConfig);

    // Emit everything. Since we only subscribe to alert feeds, everything else should be dropped.
    final List<Event> inputEvents = flattenEvents(
        SERVICE_METRIC_EVENTS,
        ALERT_EVENTS,
        SEGMENT_METADATA_EVENTS,
        REQUEST_LOG_EVENTS
    );

    final CountDownLatch eventLatch = new CountDownLatch(ALERT_EVENTS.size());

    final Map<String, List<EventMap>> feedToExpectedEvents = trackExpectedEventsPerFeed(
        ALERT_EVENTS,
        kafkaEmitterConfig.getClusterName(),
        kafkaEmitterConfig.getExtraDimensions()
    );
    final Map<String, List<EventMap>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());

    // Others would be dropped as we've only subscribed to alert events.
    Assert.assertEquals(SERVICE_METRIC_EVENTS.size(), kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(SEGMENT_METADATA_EVENTS.size(), kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(REQUEST_LOG_EVENTS.size(), kafkaEmitter.getRequestLostCount());
  }

  /**
   * Similar to {@link #testAllEvents()}, this test configures all event feeds to emit to the same topic.
   * <p>
   * Unit test to validate the handling of all valid event types, including {@link ServiceMetricEvent},
   * {@link AlertEvent}, {@link org.apache.druid.server.log.RequestLogEvent}, and {@link SegmentMetadataEvent}.
   * All {@link KafkaEmitterConfig.EventType}s are subscribed to the same topic in the config, so the expectation
   * is that all input events are emitted without any drops.
   * </p>
   */
  @Test(timeout = 10_000)
  public void testAllEventsWithCommonTopic() throws InterruptedException, JsonProcessingException
  {
    final KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig(
        "",
        new HashSet<>(Arrays.asList(KafkaEmitterConfig.EventType.values())),
        "topic",
        "topic",
        "topic",
        "topic",
        null,
        null,
        null,
        null
    );

    final KafkaEmitter kafkaEmitter = initKafkaEmitter(kafkaEmitterConfig);

    final List<Event> inputEvents = flattenEvents(
        SERVICE_METRIC_EVENTS,
        ALERT_EVENTS,
        SEGMENT_METADATA_EVENTS,
        REQUEST_LOG_EVENTS
    );

    final CountDownLatch eventLatch = new CountDownLatch(inputEvents.size());

    final Map<String, List<EventMap>> feedToExpectedEvents = trackExpectedEventsPerFeed(
        inputEvents,
        kafkaEmitterConfig.getClusterName(),
        kafkaEmitterConfig.getExtraDimensions()
    );
    final Map<String, List<EventMap>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());
  }

  /**
   * Unit test to validate the handling of {@link ServiceMetricEvent}s and {@link TestEvent}s.
   * The default event types (alerts and metrics) are subscribed in the config, so the expectation is that only
   * {@link ServiceMetricEvent} is expected to be emitted, while dropping all unknown {@link TestEvent}s.
   * </p>
   */
  @Test(timeout = 10_000)
  public void testUnknownEvents() throws InterruptedException, JsonProcessingException
  {
    final KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig(
        "",
        KafkaEmitterConfig.DEFAULT_EVENT_TYPES,
        "topic",
        "topic",
        null,
        null,
        "cluster-102",
        ImmutableMap.of("clusterName", "cluster-101", "env", "staging"), // clusterName again, extraDimensions should take precedence
        null,
        null
    );

    final KafkaEmitter kafkaEmitter = initKafkaEmitter(kafkaEmitterConfig);

    final List<Event> inputEvents = flattenEvents(
        UNKNOWN_EVENTS,
        SERVICE_METRIC_EVENTS
    );

    final CountDownLatch eventLatch = new CountDownLatch(SERVICE_METRIC_EVENTS.size());

    final Map<String, List<EventMap>> feedToExpectedEvents = trackExpectedEventsPerFeed(
        SERVICE_METRIC_EVENTS,
        kafkaEmitterConfig.getClusterName(),
        kafkaEmitterConfig.getExtraDimensions()
    );
    final Map<String, List<EventMap>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(UNKNOWN_EVENTS.size(), kafkaEmitter.getInvalidLostCount());
  }

  /**
   * Unit test to validate the handling of {@link ServiceMetricEvent}s when the Kafka emitter queue, which buffers up events
   * becomes full. The queue size in the config is set via {@code buffer.memory} and is computed from
   * the input events using {@code bufferEventsDrop}. The default event types (alerts and metrics) are subscribed in
   * the config, so the expectation is that all {@link ServiceMetricEvent}s up to {@code n - bufferEventsDrop} will be
   * emitted, {@code n} being the total number of input events, while dropping the last {@code bufferEventsDrop} events.
   */
  @Test(timeout = 10_000)
  public void testDropEventsWhenQueueFull() throws JsonProcessingException, InterruptedException
  {
    final List<Event> inputEvents = flattenEvents(
        SERVICE_METRIC_EVENTS
    );

    final ImmutableMap<String, String> extraDimensions = ImmutableMap.of("clusterId", "cluster-101");
    final Map<String, List<EventMap>> feedToAllEventsBeforeDrop = trackExpectedEventsPerFeed(
        inputEvents,
        null,
        extraDimensions
    );

    final int bufferEventsDrop = 3;
    Assert.assertTrue(
        StringUtils.format(
            "Total events to emit: %d. There must at least be %d events to drop.",
            inputEvents.size(),
            bufferEventsDrop
        ),
        inputEvents.size() - bufferEventsDrop > 0
    );

    Assert.assertEquals(
        "Currently the test only supports having 1 feed",
        1,
        feedToAllEventsBeforeDrop.size()
    );

    // Note: this only accounts for one feed currently. If we want to test the queuing behavior across all feeds,
    // we should track the minimum buffer size per feed, compute the global maximum across all the feeds and prune the
    // expected set of events accordingly. For the sake of testing simplicity, we skip that for now.
    int totalBufferSize = 0;
    for (final List<EventMap> feedEvents : feedToAllEventsBeforeDrop.values()) {
      for (int idx = 0; idx < feedEvents.size() - bufferEventsDrop; idx++) {
        totalBufferSize += MAPPER.writeValueAsString(feedEvents.get(idx)).getBytes(StandardCharsets.UTF_8).length;
      }
    }

    final Map<String, List<EventMap>> feedToExpectedEvents = new HashMap<>();
    for (final Map.Entry<String, List<EventMap>> expectedEvent : feedToAllEventsBeforeDrop.entrySet()) {
      List<EventMap> expectedEvents = expectedEvent.getValue();
      feedToExpectedEvents.put(expectedEvent.getKey(), expectedEvents.subList(0, expectedEvents.size() - bufferEventsDrop));
    }

    final KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig(
        "",
        KafkaEmitterConfig.DEFAULT_EVENT_TYPES,
        "metrics",
        "alerts",
        "requests",
        "segments",
        null,
        extraDimensions,
        ImmutableMap.of(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(totalBufferSize)),
        null
    );

    final KafkaEmitter kafkaEmitter = initKafkaEmitter(kafkaEmitterConfig);

    final CountDownLatch eventLatch = new CountDownLatch(inputEvents.size() - bufferEventsDrop);
    final Map<String, List<EventMap>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);

    Assert.assertEquals(bufferEventsDrop, kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());
  }

  private KafkaEmitter initKafkaEmitter(
      final KafkaEmitterConfig kafkaEmitterConfig
  )
  {
    return new KafkaEmitter(
        kafkaEmitterConfig,
        new DefaultObjectMapper()
    )
    {
      @Override
      protected Producer<String, String> setKafkaProducer()
      {
        // override send interval to 1 second
        sendInterval = 1;
        return producer;
      }
    };
  }

  private void emitEvents(
      final KafkaEmitter kafkaEmitter,
      final List<Event> emitEvents,
      final CountDownLatch eventLatch
  ) throws InterruptedException
  {
    kafkaEmitter.start();

    for (final Event event : emitEvents) {
      kafkaEmitter.emit(event);
    }

    eventLatch.await();
    kafkaEmitter.close();
  }

  private List<Event> flattenEvents(List<Event>... eventLists)
  {
    final List<Event> flattenedList = new ArrayList<>();
    for (List<Event> events : eventLists) {
      flattenedList.addAll(events);
    }
    return flattenedList;
  }

  private Map<String, List<EventMap>> trackActualEventsPerFeed(
      final CountDownLatch eventLatch
  )
  {

    // A concurrent hashmap because the producer callback can trigger concurrently and can override the map initialization
    final ConcurrentHashMap<String, List<EventMap>> feedToActualEvents = new ConcurrentHashMap<>();
    when(producer.send(any(), any())).then((invocation) -> {
      final ProducerRecord<?, ?> producerRecord = invocation.getArgument(0);
      final String value = String.valueOf(producerRecord.value());
      final EventMap eventMap = MAPPER.readValue(value, EventMap.class);
      feedToActualEvents.computeIfAbsent(
          (String) eventMap.get("feed"), k -> new ArrayList<>()
      ).add(eventMap);

      eventLatch.countDown();
      return null;
    });
    return feedToActualEvents;
  }

  private Map<String, List<EventMap>> trackExpectedEventsPerFeed(
      final List<Event> events,
      final String clusterName,
      final Map<String, String> extraDimensions
  ) throws JsonProcessingException
  {
    final Map<String, List<EventMap>> feedToExpectedEvents = new HashMap<>();
    for (final Event event : events) {
      final EventMap eventMap = MAPPER.readValue(MAPPER.writeValueAsString(event.toMap()), EventMap.class);
      eventMap.computeIfAbsent("clusterName", k -> clusterName);
      if (extraDimensions != null) {
        eventMap.putAll(extraDimensions);
      }
      feedToExpectedEvents.computeIfAbsent(
          event.getFeed(), k -> new ArrayList<>()).add(eventMap);
    }
    return feedToExpectedEvents;
  }

  private void validateEvents(
      final Map<String, List<EventMap>> feedToExpectedEvents,
      final Map<String, List<EventMap>> feedToActualEvents
  )
  {
    Assert.assertEquals(feedToExpectedEvents.size(), feedToActualEvents.size());

    for (final Map.Entry<String, List<EventMap>> actualEntry : feedToActualEvents.entrySet()) {
      final String feed = actualEntry.getKey();
      final List<EventMap> actualEvents = actualEntry.getValue();
      final List<EventMap> expectedEvents = feedToExpectedEvents.get(feed);
      assertThat(actualEvents, containsInAnyOrder(expectedEvents.toArray(new Map[0])));
    }
  }

  private static class TestEvent implements Event
  {
    TestEvent()
    {
    }

    @Override
    public EventMap toMap()
    {
      return new EventMap();
    }

    @Override
    public String getFeed()
    {
      return "testFeed";
    }
  }
}

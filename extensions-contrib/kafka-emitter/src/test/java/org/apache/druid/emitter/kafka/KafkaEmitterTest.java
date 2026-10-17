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
import com.google.common.collect.ImmutableSet;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaEmitterTest
{
  private KafkaProducer<String, String> producer;

  @BeforeEach
  public void setup()
  {
    producer = mock(KafkaProducer.class);
  }

  @AfterEach
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
  @Test
  @Timeout(value = 10_000, unit = TimeUnit.MILLISECONDS)
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
        null,
        null,
        null,
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

    Assertions.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getInvalidLostCount());

    kafkaEmitter.logLostMessagesStatus();

    Assertions.assertEquals(0, kafkaEmitter.getPreviousMetricLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getPreviousAlertLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getPreviousRequestLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getPreviousSegmentMetadataLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getPreviousInvalidLostCount());
  }

  /**
   * Unit test to validate the handling of all event types, including {@link ServiceMetricEvent},
   * {@link AlertEvent}, {@link org.apache.druid.server.log.RequestLogEvent}, and {@link SegmentMetadataEvent}.
   * All {@link KafkaEmitterConfig.EventType}s are subscribed in the config, so the expectation is that all the
   * events are emitted without any drops.
   */
  @Test
  @Timeout(value = 10_000, unit = TimeUnit.MILLISECONDS)
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
        null,
        null,
        null,
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

    Assertions.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getInvalidLostCount());
  }

  /**
   * Unit test to validate the handling of the default event types - {@link ServiceMetricEvent} and {@link AlertEvent}.
   * The default event types (alerts and metrics) are subscribed in the config, so the expectation is that both input
   * event types should be emitted without any drops.
   */
  @Test
  @Timeout(value = 10_000, unit = TimeUnit.MILLISECONDS)
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
        null,
        null,
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

    Assertions.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getInvalidLostCount());
  }

  /**
   * Unit test to validate the handling of all valid event types, including {@link ServiceMetricEvent},
   * {@link AlertEvent}, {@link org.apache.druid.server.log.RequestLogEvent}, and {@link SegmentMetadataEvent}.
   * Only alerts are subscribed in the config, so the expectation is that only alert events
   * should be emitted, and everything else should be dropped.
   */
  @Test
  @Timeout(value = 10_000, unit = TimeUnit.MILLISECONDS)
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
        null,
        null,
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

    Assertions.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getInvalidLostCount());

    Assertions.assertEquals(0, kafkaEmitter.getPreviousAlertLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getPreviousInvalidLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getPreviousMetricLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getPreviousSegmentMetadataLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getPreviousRequestLostCount());

    // Others would be dropped as we've only subscribed to alert events.
    Assertions.assertEquals(SERVICE_METRIC_EVENTS.size(), kafkaEmitter.getMetricLostCount());
    Assertions.assertNotEquals(kafkaEmitter.getMetricLostCount(), kafkaEmitter.getPreviousMetricLostCount());
    Assertions.assertEquals(SEGMENT_METADATA_EVENTS.size(), kafkaEmitter.getSegmentMetadataLostCount());
    Assertions.assertNotEquals(kafkaEmitter.getSegmentMetadataLostCount(), kafkaEmitter.getPreviousSegmentMetadataLostCount());
    Assertions.assertEquals(REQUEST_LOG_EVENTS.size(), kafkaEmitter.getRequestLostCount());
    Assertions.assertNotEquals(kafkaEmitter.getRequestLostCount(), kafkaEmitter.getPreviousRequestLostCount());

    kafkaEmitter.logLostMessagesStatus();

    Assertions.assertEquals(kafkaEmitter.getMetricLostCount(), kafkaEmitter.getPreviousMetricLostCount());
    Assertions.assertEquals(kafkaEmitter.getAlertLostCount(), kafkaEmitter.getPreviousAlertLostCount());
    Assertions.assertEquals(kafkaEmitter.getRequestLostCount(), kafkaEmitter.getPreviousRequestLostCount());
    Assertions.assertEquals(kafkaEmitter.getSegmentMetadataLostCount(), kafkaEmitter.getPreviousSegmentMetadataLostCount());
    Assertions.assertEquals(kafkaEmitter.getInvalidLostCount(), kafkaEmitter.getPreviousInvalidLostCount());
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
  @Test
  @Timeout(value = 10_000, unit = TimeUnit.MILLISECONDS)
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
        null,
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

    Assertions.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getInvalidLostCount());
  }

  /**
   * Unit test to validate the handling of {@link ServiceMetricEvent}s and {@link TestEvent}s.
   * The default event types (alerts and metrics) are subscribed in the config, so the expectation is that only
   * {@link ServiceMetricEvent} is expected to be emitted, while dropping all unknown {@link TestEvent}s.
   */
  @Test
  @Timeout(value = 10_000, unit = TimeUnit.MILLISECONDS)
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
        ImmutableMap.of("clusterName", "cluster-101", "env", "staging"),
        null,
        null,
        null,
        null,
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

    Assertions.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assertions.assertEquals(UNKNOWN_EVENTS.size(), kafkaEmitter.getInvalidLostCount());
  }

  /**
   * Unit test to validate the handling of {@link ServiceMetricEvent}s when the Kafka emitter queue, which buffers up events
   * becomes full. The queue size in the config is set via {@code buffer.memory} and is computed from
   * the input events using {@code bufferEventsDrop}. The default event types (alerts and metrics) are subscribed in
   * the config, so the expectation is that all {@link ServiceMetricEvent}s up to {@code n - bufferEventsDrop} will be
   * emitted, {@code n} being the total number of input events, while dropping the last {@code bufferEventsDrop} events.
   */
  @Test
  @Timeout(value = 10_000, unit = TimeUnit.MILLISECONDS)
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
    Assertions.assertTrue(
        inputEvents.size() - bufferEventsDrop > 0,
        StringUtils.format(
            "Total events to emit: %d. There must at least be %d events to drop.",
            inputEvents.size(),
            bufferEventsDrop
        )
    );

    Assertions.assertEquals(
        1,
        feedToAllEventsBeforeDrop.size(),
        "Currently the test only supports having 1 feed"
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
        null,
        null,
        null,
        null,
        null
    );

    final KafkaEmitter kafkaEmitter = initKafkaEmitter(kafkaEmitterConfig);

    final CountDownLatch eventLatch = new CountDownLatch(inputEvents.size() - bufferEventsDrop);
    final Map<String, List<EventMap>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);

    Assertions.assertEquals(bufferEventsDrop, kafkaEmitter.getMetricLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getInvalidLostCount());
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
    Assertions.assertEquals(feedToExpectedEvents.keySet(), feedToActualEvents.keySet());

    for (final Map.Entry<String, List<EventMap>> actualEntry : feedToActualEvents.entrySet()) {
      final String feed = actualEntry.getKey();
      final List<EventMap> actualEvents = actualEntry.getValue();
      final List<EventMap> expectedEvents = feedToExpectedEvents.get(feed);
      Assertions.assertEquals(expectedEvents.size(), actualEvents.size());
      Assertions.assertTrue(
          expectedEvents.stream().allMatch(
              expectedEvent -> Collections.frequency(expectedEvents, expectedEvent)
                               == Collections.frequency(actualEvents, expectedEvent)
          )
      );
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

  // ==================== Sampling Tests ====================

  @Test
  public void testShouldSampleOut_noSamplingWhenRateIs100()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(100, ImmutableSet.of("query/time"), null));
    ServiceMetricEvent event = ServiceMetricEvent.builder().setMetric("query/time", 100).build("svc", "host");
    Assertions.assertFalse(emitter.shouldSampleOut(event));
  }

  @Test
  public void testShouldSampleOut_noSamplingWhenSampledMetricsEmpty()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(100, null, null));
    ServiceMetricEvent event = ServiceMetricEvent.builder().setMetric("query/time", 100).build("svc", "host");
    Assertions.assertFalse(emitter.shouldSampleOut(event));
  }

  /**
   * samplingRate=10, both sampledMetrics and sampledNodeTypes empty → samples ALL metrics at 10%.
   * With rate=0 we can deterministically assert true.
   */
  @Test
  public void testShouldSampleOut_samplesAllWhenRateSetButFiltersEmpty()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(0, null, null));
    ServiceMetricEvent event = ServiceMetricEvent.builder().setMetric("query/time", 100).build("svc", "host");
    // With rate=0 and no filters, ALL metrics should be sampled out
    Assertions.assertTrue(emitter.shouldSampleOut(event));
  }

  @Test
  public void testShouldSampleOut_noSamplingForNonMatchingMetric()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(
        10,
        ImmutableSet.of("query/time", "query/bytes"),
        null
    ));
    // "jvm/mem/used" is not in sampledMetrics, so should never be sampled out
    ServiceMetricEvent event = ServiceMetricEvent.builder().setMetric("jvm/mem/used", 100).build("svc", "host");
    Assertions.assertFalse(emitter.shouldSampleOut(event));
  }

  @Test
  public void testShouldSampleOut_samplesMatchingMetricWithZeroRate()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(0, ImmutableSet.of("query/time"), null));
    ServiceMetricEvent event = ServiceMetricEvent.builder().setMetric("query/time", 100).build("svc", "host");
    // With rate=0, all matching metrics should be sampled out
    Assertions.assertTrue(emitter.shouldSampleOut(event));
  }

  @Test
  public void testShouldSampleOut_consistentHashOnId()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(50, ImmutableSet.of("query/time"), null));

    // Build event with an "id" dimension
    ServiceMetricEvent event = ServiceMetricEvent.builder()
        .setDimension("id", "query-abc-123")
        .setMetric("query/time", 100)
        .build("svc", "host");

    // Consistent hashing: same id should always produce the same result
    boolean firstResult = emitter.shouldSampleOut(event);
    for (int i = 0; i < 100; i++) {
      Assertions.assertEquals(
          firstResult,
          emitter.shouldSampleOut(event),
          "Consistent hashing should give same result for same id"
      );
    }
  }

  // ==================== Node Type Sampling Tests ====================

  /**
   * samplingRate=0, sampledMetrics=["query/time"], sampledNodeTypes=["druid/broker"]
   * Event from druid/broker with query/time → should be sampled out
   */
  @Test
  public void testShouldSampleOut_matchingMetricAndNodeType()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(
        0,
        ImmutableSet.of("query/time"),
        ImmutableSet.of("druid/broker")
    ));
    ServiceMetricEvent event = ServiceMetricEvent.builder()
        .setMetric("query/time", 100)
        .build("druid/broker", "host");
    Assertions.assertTrue(emitter.shouldSampleOut(event));
  }

  /**
   * samplingRate=0, sampledMetrics=["query/time"], sampledNodeTypes=["druid/broker"]
   * Event from druid/historical with query/time → should NOT be sampled (wrong node type)
   */
  @Test
  public void testShouldSampleOut_matchingMetricButWrongNodeType()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(
        0,
        ImmutableSet.of("query/time"),
        ImmutableSet.of("druid/broker")
    ));
    ServiceMetricEvent event = ServiceMetricEvent.builder()
        .setMetric("query/time", 100)
        .build("druid/historical", "host");
    Assertions.assertFalse(emitter.shouldSampleOut(event));
  }

  /**
   * samplingRate=0, sampledMetrics=empty, sampledNodeTypes=["druid/broker"]
   * Event from druid/broker with any metric → should be sampled out (node type filter only)
   */
  @Test
  public void testShouldSampleOut_nodeTypeOnlyFilter()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(0, null, ImmutableSet.of("druid/broker")));
    ServiceMetricEvent event = ServiceMetricEvent.builder()
        .setMetric("jvm/mem/used", 100)
        .build("druid/broker", "host");
    Assertions.assertTrue(emitter.shouldSampleOut(event));
  }

  /**
   * samplingRate=0, sampledMetrics=empty, sampledNodeTypes=["druid/broker"]
   * Event from druid/historical → should NOT be sampled (wrong node type)
   */
  @Test
  public void testShouldSampleOut_nodeTypeOnlyFilterWrongNodeType()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(0, null, ImmutableSet.of("druid/broker")));
    ServiceMetricEvent event = ServiceMetricEvent.builder()
        .setMetric("jvm/mem/used", 100)
        .build("druid/historical", "host");
    Assertions.assertFalse(emitter.shouldSampleOut(event));
  }

  /**
   * samplingRate=0, sampledMetrics=["query/time"], sampledNodeTypes=empty
   * Event with query/time from any node → should be sampled out (metric filter only)
   */
  @Test
  public void testShouldSampleOut_metricOnlyFilter()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(0, ImmutableSet.of("query/time"), null));
    ServiceMetricEvent event = ServiceMetricEvent.builder()
        .setMetric("query/time", 100)
        .build("druid/historical", "host");
    Assertions.assertTrue(emitter.shouldSampleOut(event));
  }

  /**
   * samplingRate=0, sampledMetrics=["query/time"], sampledNodeTypes=empty
   * Event with jvm/mem/used → should NOT be sampled (metric doesn't match)
   */
  @Test
  public void testShouldSampleOut_metricOnlyFilterNonMatchingMetric()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(0, ImmutableSet.of("query/time"), null));
    ServiceMetricEvent event = ServiceMetricEvent.builder()
        .setMetric("jvm/mem/used", 100)
        .build("druid/historical", "host");
    Assertions.assertFalse(emitter.shouldSampleOut(event));
  }

  /**
   * samplingRate=100 always means no sampling, even with filters configured
   */
  @Test
  public void testShouldSampleOut_rate100OverridesAllFilters()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(
        100,
        ImmutableSet.of("query/time"),
        ImmutableSet.of("druid/broker")
    ));
    ServiceMetricEvent event = ServiceMetricEvent.builder()
        .setMetric("query/time", 100)
        .build("druid/broker", "host");
    Assertions.assertFalse(emitter.shouldSampleOut(event));
  }

  /**
   * samplingRate=0, both filters empty → sample ALL metrics from all nodes (drop everything)
   */
  @Test
  public void testShouldSampleOut_rate0WithNoFiltersDropsEverything()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(0, null, null));
    ServiceMetricEvent event = ServiceMetricEvent.builder()
        .setMetric("anything/at/all", 100)
        .build("druid/coordinator", "host");
    Assertions.assertTrue(emitter.shouldSampleOut(event));
  }

  /**
   * samplingRate=0, multiple node types configured
   */
  @Test
  public void testShouldSampleOut_multipleNodeTypes()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(
        0,
        null,
        ImmutableSet.of("druid/broker", "druid/historical")
    ));

    // broker → sampled
    ServiceMetricEvent brokerEvent = ServiceMetricEvent.builder()
        .setMetric("query/time", 100)
        .build("druid/broker", "host");
    Assertions.assertTrue(emitter.shouldSampleOut(brokerEvent));

    // historical → sampled
    ServiceMetricEvent historicalEvent = ServiceMetricEvent.builder()
        .setMetric("query/time", 100)
        .build("druid/historical", "host");
    Assertions.assertTrue(emitter.shouldSampleOut(historicalEvent));

    // coordinator → NOT sampled (not in set)
    ServiceMetricEvent coordinatorEvent = ServiceMetricEvent.builder()
        .setMetric("query/time", 100)
        .build("druid/coordinator", "host");
    Assertions.assertFalse(emitter.shouldSampleOut(coordinatorEvent));
  }

  /**
   * With 10% sampling and no "id" dimension, roughly 10% of matching events should be kept.
   * The bounds are wide enough (about 13 standard deviations) for this check never to flake.
   */
  @Test
  public void testShouldSampleOut_approximateRetentionRate()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(10, ImmutableSet.of("query/time"), null));
    final ServiceMetricEvent event = ServiceMetricEvent.builder()
                                                       .setMetric("query/time", 100)
                                                       .build("druid/broker", "host");
    final int total = 100_000;
    int kept = 0;
    for (int i = 0; i < total; i++) {
      if (!emitter.shouldSampleOut(event)) {
        kept++;
      }
    }
    Assertions.assertTrue(kept > 8_800 && kept < 11_200, "Expected about 10% kept, but kept " + kept + " of " + total);
  }

  /**
   * With 50% sampling, events with different "id" values should be split between kept and dropped,
   * while each individual id keeps getting the same decision.
   */
  @Test
  public void testShouldSampleOut_hashOnIdSplitsAcrossIds()
  {
    final KafkaEmitter emitter = initKafkaEmitter(newSamplingConfig(50, ImmutableSet.of("query/time"), null));
    final int total = 10_000;
    int kept = 0;
    for (int i = 0; i < total; i++) {
      final ServiceMetricEvent event = ServiceMetricEvent.builder()
                                                         .setDimension("id", "query-" + i)
                                                         .setMetric("query/time", 100)
                                                         .build("druid/broker", "host");
      final boolean dropped = emitter.shouldSampleOut(event);
      Assertions.assertEquals(dropped, emitter.shouldSampleOut(event));
      if (!dropped) {
        kept++;
      }
    }
    Assertions.assertTrue(kept > 4_000 && kept < 6_000, "Expected about 50% kept, but kept " + kept + " of " + total);
  }

  /**
   * End-to-end: with samplingRate=0, metric events never reach the Kafka producer, while alerts, requests and
   * segment metadata are still emitted. Sampled-out events are counted as sampled, not as lost.
   */
  @Test
  @Timeout(value = 10_000, unit = TimeUnit.MILLISECONDS)
  public void testSampledMetricsAreDroppedBeforeProducerAndOtherEventsAreKept()
      throws InterruptedException, JsonProcessingException
  {
    final KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig(
        "",
        new HashSet<>(Arrays.asList(KafkaEmitterConfig.EventType.values())),
        "metrics",
        "alerts",
        "requests",
        "segments",
        "clusterName",
        null,
        null,
        null,
        null,
        0,
        null,
        null
    );
    final KafkaEmitter kafkaEmitter = initKafkaEmitter(kafkaEmitterConfig);

    final List<Event> metricEvents = flattenEvents(SERVICE_METRIC_EVENTS);
    final List<Event> keptEvents = flattenEvents(ALERT_EVENTS, REQUEST_LOG_EVENTS, SEGMENT_METADATA_EVENTS);
    final CountDownLatch eventLatch = new CountDownLatch(keptEvents.size());

    final Map<String, List<EventMap>> feedToExpectedEvents = trackExpectedEventsPerFeed(
        keptEvents,
        kafkaEmitterConfig.getClusterName(),
        kafkaEmitterConfig.getExtraDimensions()
    );
    final Map<String, List<EventMap>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, flattenEvents(metricEvents, keptEvents), eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);
    Assertions.assertEquals(metricEvents.size(), kafkaEmitter.getMetricSampledCount());
    Assertions.assertEquals(0, kafkaEmitter.getMetricEmittedCount());
    Assertions.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assertions.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
  }

  /**
   * End-to-end: only the configured metric is sampled; other metrics are still emitted.
   */
  @Test
  @Timeout(value = 10_000, unit = TimeUnit.MILLISECONDS)
  public void testOnlyConfiguredMetricsAreSampledEndToEnd() throws InterruptedException, JsonProcessingException
  {
    final KafkaEmitterConfig kafkaEmitterConfig = newSamplingConfig(0, ImmutableSet.of("query/time"), null);
    final KafkaEmitter kafkaEmitter = initKafkaEmitter(kafkaEmitterConfig);

    final List<Event> sampledEvents = ImmutableList.of(
        ServiceMetricEvent.builder().setMetric("query/time", 10).build("druid/broker", "host"),
        ServiceMetricEvent.builder().setMetric("query/time", 20).build("druid/broker", "host")
    );
    final List<Event> keptEvents = ImmutableList.of(
        ServiceMetricEvent.builder().setMetric("jvm/mem/used", 30).build("druid/broker", "host"),
        ServiceMetricEvent.builder().setMetric("query/bytes", 40).build("druid/broker", "host")
    );
    final CountDownLatch eventLatch = new CountDownLatch(keptEvents.size());

    final Map<String, List<EventMap>> feedToExpectedEvents = trackExpectedEventsPerFeed(
        keptEvents,
        kafkaEmitterConfig.getClusterName(),
        kafkaEmitterConfig.getExtraDimensions()
    );
    final Map<String, List<EventMap>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, flattenEvents(sampledEvents, keptEvents), eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);
    Assertions.assertEquals(sampledEvents.size(), kafkaEmitter.getMetricSampledCount());
    Assertions.assertEquals(keptEvents.size(), kafkaEmitter.getMetricEmittedCount());
    Assertions.assertEquals(0, kafkaEmitter.getMetricLostCount());

    // Exercises the periodic sampling summary log path.
    kafkaEmitter.logLostMessagesStatus();
  }

  /**
   * The sampling summary must only cover the latest interval. An interval that emits metrics but samples none
   * skips the log line, but must still move the emitted-count baseline forward, so its emissions are not
   * reported again in a later interval's summary.
   */
  @Test
  public void testSamplingSummaryWindowAdvancesWhenNothingSampled()
  {
    final KafkaEmitter kafkaEmitter = initKafkaEmitter(newSamplingConfig(0, ImmutableSet.of("query/time"), null));
    final ServiceMetricEvent keptEvent = ServiceMetricEvent.builder()
                                                           .setMetric("jvm/mem/used", 1)
                                                           .build("druid/broker", "host");
    final ServiceMetricEvent sampledEvent = ServiceMetricEvent.builder()
                                                              .setMetric("query/time", 1)
                                                              .build("druid/broker", "host");

    // Interval 1: two metrics emitted, none sampled out.
    kafkaEmitter.emit(keptEvent);
    kafkaEmitter.emit(keptEvent);
    kafkaEmitter.logLostMessagesStatus();
    Assertions.assertEquals(0, kafkaEmitter.getMetricSampledCount());
    Assertions.assertEquals(2, kafkaEmitter.getPreviousMetricEmittedCount());

    // Interval 2: one emitted and one sampled out. The window must start from interval 1's end.
    kafkaEmitter.emit(keptEvent);
    kafkaEmitter.emit(sampledEvent);
    kafkaEmitter.logLostMessagesStatus();
    Assertions.assertEquals(3, kafkaEmitter.getPreviousMetricEmittedCount());
    Assertions.assertEquals(1, kafkaEmitter.getPreviousMetricSampledCount());
  }

  /**
   * Sampling is decided before the event is serialized, so a sampled-out metric never pays for serialization.
   * A dimension that fails to serialize proves it: the sampled-out event is counted as sampled, not invalid,
   * while an event that is kept still goes through serialization and is counted as invalid as before.
   */
  @Test
  public void testSamplingHappensBeforeSerialization()
  {
    final KafkaEmitter kafkaEmitter = initKafkaEmitter(newSamplingConfig(0, ImmutableSet.of("query/time"), null));

    kafkaEmitter.emit(
        ServiceMetricEvent.builder()
                          .setDimension("bad", new UnserializableValue())
                          .setMetric("query/time", 1)
                          .build("druid/broker", "host")
    );
    Assertions.assertEquals(1, kafkaEmitter.getMetricSampledCount());
    Assertions.assertEquals(0, kafkaEmitter.getInvalidLostCount());

    kafkaEmitter.emit(
        ServiceMetricEvent.builder()
                          .setDimension("bad", new UnserializableValue())
                          .setMetric("jvm/mem/used", 1)
                          .build("druid/broker", "host")
    );
    Assertions.assertEquals(1, kafkaEmitter.getMetricSampledCount());
    Assertions.assertEquals(1, kafkaEmitter.getInvalidLostCount());
  }

  /**
   * A dimension value that Jackson cannot serialize, because its only property getter throws.
   */
  private static class UnserializableValue
  {
    @SuppressWarnings("unused")
    public String getValue()
    {
      throw new IllegalStateException("not serializable");
    }
  }

  private static KafkaEmitterConfig newSamplingConfig(
      final Integer samplingRate,
      final Set<String> sampledMetrics,
      final Set<String> sampledNodeTypes
  )
  {
    return new KafkaEmitterConfig(
        "localhost:9092",
        null,
        "metrics",
        "alerts",
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        samplingRate,
        sampledMetrics,
        sampledNodeTypes
    );
  }
}

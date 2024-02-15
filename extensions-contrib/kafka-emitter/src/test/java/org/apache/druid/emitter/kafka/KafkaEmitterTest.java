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
import java.util.concurrent.CountDownLatch;

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

  @Test(timeout = 10_000)
  public void testServiceMetricEvents() throws JsonProcessingException, InterruptedException
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

    final Map<String, List<String>> feedToExpectedEvents = trackExpectedEventsPerFeed(
        inputEvents,
        kafkaEmitterConfig.getClusterName(),
        kafkaEmitterConfig.getExtraDimensions()
    );
    final Map<String, List<String>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());
  }

  @Test(timeout = 10_000)
  public void testAllEvents() throws JsonProcessingException, InterruptedException
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

    final Map<String, List<String>> feedToExpectedEvents = trackExpectedEventsPerFeed(
        inputEvents,
        kafkaEmitterConfig.getClusterName(),
        kafkaEmitterConfig.getExtraDimensions()
    );
    final Map<String, List<String>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());

  }

  @Test(timeout = 10_000)
  public void testDefaultEvents() throws JsonProcessingException, InterruptedException
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

    final Map<String, List<String>> feedToExpectedEvents = trackExpectedEventsPerFeed(
        inputEvents,
        kafkaEmitterConfig.getClusterName(),
        kafkaEmitterConfig.getExtraDimensions()
    );
    final Map<String, List<String>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());
  }

  @Test(timeout = 10_000)
  public void testAlertsPlusUnsubscribedEvents() throws JsonProcessingException, InterruptedException
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

    final Map<String, List<String>> feedToExpectedEvents = trackExpectedEventsPerFeed(
        ALERT_EVENTS,
        kafkaEmitterConfig.getClusterName(),
        kafkaEmitterConfig.getExtraDimensions()
    );
    final Map<String, List<String>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());

    // Others would be dropped as we've only subscribed to alert events.
    Assert.assertEquals(SERVICE_METRIC_EVENTS.size(), kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(SEGMENT_METADATA_EVENTS.size(), kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(REQUEST_LOG_EVENTS.size(), kafkaEmitter.getRequestLostCount());
  }

  @Test(timeout = 10_000)
  public void testAllEventsWithCommonTopic() throws JsonProcessingException, InterruptedException
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

    final Map<String, List<String>> feedToExpectedEvents = trackExpectedEventsPerFeed(
        inputEvents,
        kafkaEmitterConfig.getClusterName(),
        kafkaEmitterConfig.getExtraDimensions()
    );
    final Map<String, List<String>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());
  }

  @Test(timeout = 10_000)
  public void testUnknownEvents() throws JsonProcessingException, InterruptedException
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

    final Map<String, List<String>> feedToExpectedEvents = trackExpectedEventsPerFeed(
        SERVICE_METRIC_EVENTS,
        kafkaEmitterConfig.getClusterName(),
        kafkaEmitterConfig.getExtraDimensions()
    );
    final Map<String, List<String>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(feedToExpectedEvents, feedToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getRequestLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(UNKNOWN_EVENTS.size(), kafkaEmitter.getInvalidLostCount());
  }

  @Test(timeout = 10_000)
  public void testDropEventsWhenQueueFull() throws JsonProcessingException, InterruptedException
  {
    final List<Event> inputEvents = flattenEvents(
        SERVICE_METRIC_EVENTS
    );

    final ImmutableMap<String, String> extraDimensions = ImmutableMap.of("clusterId", "cluster-101");
    final Map<String, List<String>> feedToAllEventsBeforeDrop = trackExpectedEventsPerFeed(
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
    for (final List<String> feedEvents : feedToAllEventsBeforeDrop.values()) {
      for (int idx = 0; idx < feedEvents.size() - bufferEventsDrop; idx++) {
        totalBufferSize += feedEvents.get(idx).getBytes(StandardCharsets.UTF_8).length;
      }
    }

    final Map<String, List<String>> feedToExpectedEvents = new HashMap<>();
    for (final Map.Entry<String, List<String>> expectedEvent : feedToAllEventsBeforeDrop.entrySet()) {
      List<String> expectedEvents = expectedEvent.getValue();
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
    final Map<String, List<String>> feedToActualEvents = trackActualEventsPerFeed(eventLatch);

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

  private Map<String, List<String>> trackActualEventsPerFeed(
      final CountDownLatch eventLatch
  )
  {
    final Map<String, List<String>> feedToActualEvents = new HashMap<>();
    when(producer.send(any(), any())).then((invocation) -> {
      final ProducerRecord<?, ?> producerRecord = invocation.getArgument(0);
      final String value = String.valueOf(producerRecord.value());
      final EventMap eventMap = MAPPER.readValue(value, EventMap.class);
      feedToActualEvents.computeIfAbsent(
          (String) eventMap.get("feed"), k -> new ArrayList<>()
      ).add(value);

      eventLatch.countDown();
      return null;
    });
    return feedToActualEvents;
  }

  private Map<String, List<String>> trackExpectedEventsPerFeed(
      final List<Event> events,
      final String clusterName,
      final Map<String, String> extraDimensions
  ) throws JsonProcessingException
  {
    final Map<String, List<String>> feedToExpectedEvents = new HashMap<>();
    for (final Event event : events) {
      final EventMap eventMap = event.toMap();
      eventMap.computeIfAbsent("clusterName", k -> clusterName);
      if (extraDimensions != null) {
        eventMap.putAll(extraDimensions);
      }
      feedToExpectedEvents.computeIfAbsent(
          event.getFeed(), k -> new ArrayList<>()).add(MAPPER.writeValueAsString(eventMap)
      );
    }
    return feedToExpectedEvents;
  }

  private void validateEvents(
      final Map<String, List<String>> feedToExpectedEvents,
      final Map<String, List<String>> feedToActualEvents
  )
  {
    Assert.assertEquals(feedToExpectedEvents.size(), feedToActualEvents.size());

    for (final Map.Entry<String, List<String>> actualEntry : feedToActualEvents.entrySet()) {
      final String feed = actualEntry.getKey();
      final List<String> actualEvents = actualEntry.getValue();
      final List<String> expectedEvents = feedToExpectedEvents.get(feed);
      Assert.assertEquals(expectedEvents, actualEvents);
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

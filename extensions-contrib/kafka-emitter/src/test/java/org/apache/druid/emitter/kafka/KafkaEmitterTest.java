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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
      ServiceMetricEvent.builder().setMetric("m1", 1).build("service", "host")
  );

  private static final List<Event> ALERT_EVENTS = ImmutableList.of(
      new AlertEvent("service", "host", "description")
  );

  private static final List<Event> REQUEST_LOG_EVENTS = ImmutableList.of(
      DefaultRequestLogEventBuilderFactory.instance().createRequestLogEventBuilder(
          "requests",
          RequestLogLine.forSql(
              "", null, DateTimes.nowUtc(), null, new QueryStats(ImmutableMap.of())
          )
      ).build("service", "host")
  );

  private static final List<Event> SEGMENT_METADATA_EVENTS = ImmutableList.of(
      new SegmentMetadataEvent(
          "dummy_datasource",
          DateTimes.of("2001-01-01T00:00:00.000Z"),
          DateTimes.of("2001-01-02T00:00:00.000Z"),
          DateTimes.of("2001-01-03T00:00:00.000Z"),
          "dummy_version",
          true
      )
  );

  @Test(timeout = 10_000)
  public void testKafkaEmitterServiceMetricEvents() throws JsonProcessingException, InterruptedException
  {
    final KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig(
        "",
        new HashSet<>(Collections.singletonList(KafkaEmitterConfig.EventType.METRICS)),
        "metrics",
        "alerts",
        "requests",
        "segment_metadata",
        "clusterName",
        ImmutableMap.of("clusterId", "cluster-101"),
        null,
        null
    );

    final KafkaEmitter kafkaEmitter = initKafkaEmitter(kafkaEmitterConfig);

    final List<Event> inputEvents = flattenEvents(SERVICE_METRIC_EVENTS);
    final CountDownLatch eventLatch = new CountDownLatch(inputEvents.size());

    final Map<String, List<String>> topicToExpectedEvents = trackExpectedEvents(SERVICE_METRIC_EVENTS, kafkaEmitterConfig);
    final Map<String, List<String>> topicToActualEvents = trackActualEventsInCallback(eventLatch);

    emitEvents(kafkaEmitter, SERVICE_METRIC_EVENTS, eventLatch);

    validateEvents(topicToExpectedEvents, topicToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());

    validateEvents(topicToExpectedEvents, topicToActualEvents);
  }

  @Test(timeout = 10_000)
  public void testKafkaEmitterAllEvents() throws JsonProcessingException, InterruptedException
  {
    final KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig(
        "",
        new HashSet<>(Arrays.asList(KafkaEmitterConfig.EventType.values())),
        "metrics",
        "alerts",
        "requests",
        "segment_metadata",
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

    final Map<String, List<String>> topicToExpectedEvents = trackExpectedEvents(inputEvents, kafkaEmitterConfig);
    final Map<String, List<String>> topicToActualEvents = trackActualEventsInCallback(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(topicToExpectedEvents, topicToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());

  }

  @Test(timeout = 10_000)
  public void testKafkaEmitterDefaultEvents() throws JsonProcessingException, InterruptedException
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

    final Map<String, List<String>> topicToExpectedEvents = trackExpectedEvents(inputEvents, kafkaEmitterConfig);
    final Map<String, List<String>> topicToActualEvents = trackActualEventsInCallback(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(topicToExpectedEvents, topicToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());
  }

  @Test(timeout = 10_000)
  public void testKafkaEmitterDefaultEventsMissing() throws JsonProcessingException, InterruptedException
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

    // Includes everything, but we've only subscribed to alerts type.
    final List<Event> inputEvents = flattenEvents(
        SERVICE_METRIC_EVENTS,
        ALERT_EVENTS,
        SEGMENT_METADATA_EVENTS,
        REQUEST_LOG_EVENTS
    );

    final CountDownLatch eventLatch = new CountDownLatch(ALERT_EVENTS.size());

    final Map<String, List<String>> topicToExpectedEvents = trackExpectedEvents(ALERT_EVENTS, kafkaEmitterConfig);
    final Map<String, List<String>> topicToActualEvents = trackActualEventsInCallback(eventLatch);

    emitEvents(kafkaEmitter, inputEvents, eventLatch);

    validateEvents(topicToExpectedEvents, topicToActualEvents);

    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());

    // Others would be dropped
    Assert.assertEquals(SERVICE_METRIC_EVENTS.size(), kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(SEGMENT_METADATA_EVENTS.size(), kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(REQUEST_LOG_EVENTS.size(), kafkaEmitter.getRequestLostCount());
  }

  private KafkaEmitter initKafkaEmitter(
      final KafkaEmitterConfig kafkaEmitterConfig
  )
  {
    final KafkaEmitter kafkaEmitter = new KafkaEmitter(
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
    return kafkaEmitter;
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

  private Map<String, List<String>> trackActualEventsInCallback(
      final CountDownLatch eventLatch
  )
  {
    final Map<String, List<String>> topicToActualEvents = new HashMap<>();
    when(producer.send(any(), any())).then((invocation) -> {
      final ProducerRecord<?, ?> producerRecord = invocation.getArgument(0);
      final Object value = producerRecord.value();

      topicToActualEvents.computeIfAbsent(
          producerRecord.topic(), k -> new ArrayList<>()
      ).add(String.valueOf(value));

      eventLatch.countDown();
      return null;
    });
    return topicToActualEvents;
  }

  private Map<String, List<String>> trackExpectedEvents(
      final List<Event> events,
      final KafkaEmitterConfig kafkaEmitterConfig
  ) throws JsonProcessingException
  {
    final Map<String, List<String>> topicToExpectedEvents = new HashMap<>();
    for (final Event event : events) {
      final EventMap eventMap = event.toMap();
      if (kafkaEmitterConfig.getExtraDimensions() != null) {
        eventMap.putAll(kafkaEmitterConfig.getExtraDimensions());
      }
      eventMap.computeIfAbsent("clusterName", k -> kafkaEmitterConfig.getClusterName());
      topicToExpectedEvents.computeIfAbsent(
          event.getFeed(), k -> new ArrayList<>()).add(MAPPER.writeValueAsString(eventMap)
      );
    }
    return topicToExpectedEvents;
  }

  private void validateEvents(
      final Map<String, List<String>> topicToExpectedEvents,
      final Map<String, List<String>> topicToActualEvents
  )
  {
    Assert.assertEquals(topicToExpectedEvents.size(), topicToActualEvents.size());

    for (final Map.Entry<String, List<String>> actualEntry : topicToActualEvents.entrySet()) {
      final String topic = actualEntry.getKey();
      final List<String> actualEvents = actualEntry.getValue();
      final List<String> expectedEvents = topicToExpectedEvents.get(topic);
      Assert.assertEquals(expectedEvents, actualEvents);
    }
  }
}

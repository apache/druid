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
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.metadata.DynamicConfigProvider;
import org.apache.druid.metadata.MapStringDynamicConfigProvider;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class KafkaEmitterConfigTest
{
  private static final DynamicConfigProvider<String> DEFAULT_PRODUCER_SECRETS = new MapStringDynamicConfigProvider(
      ImmutableMap.of("testSecretKey", "testSecretValue"));
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerDeserKafkaEmitterConfig() throws IOException
  {
    KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig(
        "hostname",
        null,
        "metricTest",
        "alertTest",
        "requestTest",
        "metadataTest",
        "clusterNameTest",
        ImmutableMap.of("env", "preProd"),
        ImmutableMap.<String, String>builder()
                    .put("testKey", "testValue").build(),
        DEFAULT_PRODUCER_SECRETS
    );
    String kafkaEmitterConfigString = MAPPER.writeValueAsString(kafkaEmitterConfig);
    KafkaEmitterConfig kafkaEmitterConfigExpected = MAPPER.readerFor(KafkaEmitterConfig.class)
                                                          .readValue(kafkaEmitterConfigString);
    Assert.assertEquals(kafkaEmitterConfigExpected, kafkaEmitterConfig);
  }

  @Test
  public void testSerDeserKafkaEmitterConfigNullRequestTopic() throws IOException
  {
    KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig(
        "hostname",
        null,
        "metricTest",
        "alertTest",
        null,
        "metadataTest",
        "clusterNameTest",
        null,
        ImmutableMap.<String, String>builder()
                    .put("testKey", "testValue").build(),
        DEFAULT_PRODUCER_SECRETS
    );
    String kafkaEmitterConfigString = MAPPER.writeValueAsString(kafkaEmitterConfig);
    KafkaEmitterConfig kafkaEmitterConfigExpected = MAPPER.readerFor(KafkaEmitterConfig.class)
                                                          .readValue(kafkaEmitterConfigString);
    Assert.assertEquals(kafkaEmitterConfigExpected, kafkaEmitterConfig);
  }

  @Test
  public void testSerDeserKafkaEmitterConfigNullMetricsTopic() throws IOException
  {
    Set<KafkaEmitterConfig.EventType> eventTypeSet = new HashSet<KafkaEmitterConfig.EventType>();
    eventTypeSet.add(KafkaEmitterConfig.EventType.SEGMENT_METADATA);
    KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig(
        "hostname",
        eventTypeSet,
        null,
        null,
        null,
        "metadataTest",
        "clusterNameTest",
        null,
        ImmutableMap.<String, String>builder()
                    .put("testKey", "testValue").build(),
        DEFAULT_PRODUCER_SECRETS
    );
    String kafkaEmitterConfigString = MAPPER.writeValueAsString(kafkaEmitterConfig);
    KafkaEmitterConfig kafkaEmitterConfigExpected = MAPPER.readerFor(KafkaEmitterConfig.class)
                                                          .readValue(kafkaEmitterConfigString);
    Assert.assertEquals(kafkaEmitterConfigExpected, kafkaEmitterConfig);
  }

  @Test
  public void testSerDeNotRequiredKafkaProducerConfigOrKafkaSecretProducer() throws JsonProcessingException
  {
    KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig(
        "localhost:9092",
        null,
        "metricTest",
        "alertTest",
        null,
        "metadataTest",
        null,
        ImmutableMap.of("env", "preProd"),
        null,
        null
    );
    String kafkaEmitterConfigString = MAPPER.writeValueAsString(kafkaEmitterConfig);
    KafkaEmitterConfig kafkaEmitterConfigExpected = MAPPER.readerFor(KafkaEmitterConfig.class)
                                                          .readValue(kafkaEmitterConfigString);
    Assert.assertEquals(kafkaEmitterConfigExpected, kafkaEmitterConfig);
    try {
      @SuppressWarnings("unused")
      KafkaEmitter emitter = new KafkaEmitter(kafkaEmitterConfig, MAPPER);
    }
    catch (NullPointerException e) {
      Assert.fail();
    }
  }

  @Test
  public void testDeserializeEventTypesWithDifferentCase() throws JsonProcessingException
  {
    Assert.assertEquals(
        KafkaEmitterConfig.EventType.SEGMENT_METADATA,
        MAPPER.readValue("\"segment_metadata\"", KafkaEmitterConfig.EventType.class)
    );
    Assert.assertEquals(
        KafkaEmitterConfig.EventType.ALERTS,
        MAPPER.readValue("\"alerts\"", KafkaEmitterConfig.EventType.class)
    );
    Assert.assertThrows(
        ValueInstantiationException.class,
        () -> MAPPER.readValue("\"segmentMetadata\"", KafkaEmitterConfig.EventType.class)
    );
  }

  @Test
  public void testJacksonModules()
  {
    Assert.assertTrue(new KafkaEmitterModule().getJacksonModules().isEmpty());
  }

  @Test
  public void testNullBootstrapServers()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new KafkaEmitterConfig(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        ),
        operatorExceptionMatcher()
            .expectMessageIs("druid.emitter.kafka.bootstrap.servers must be specified.")
    );
  }

  @Test
  public void testNullMetricTopic()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new KafkaEmitterConfig(
                "foo",
                new HashSet<>(Collections.singletonList(KafkaEmitterConfig.EventType.METRICS)),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        ),
        operatorExceptionMatcher()
            .expectMessageIs("druid.emitter.kafka.metric.topic must be specified if druid.emitter.kafka.event.types contains metrics.")
    );
  }

  @Test
  public void testNullAlertTopic()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new KafkaEmitterConfig(
                "foo",
                null,
                "foo",
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        ),
        operatorExceptionMatcher()
            .expectMessageIs(
                "druid.emitter.kafka.alert.topic must be specified if druid.emitter.kafka.event.types contains alerts."
            )
    );
  }

  @Test
  public void testNullRequestTopic()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new KafkaEmitterConfig(
                "foo",
                new HashSet<>(Arrays.asList(KafkaEmitterConfig.EventType.values())),
                "foo",
                "foo",
                null,
                null,
                null,
                null,
                null,
                null
            )
        ),
        operatorExceptionMatcher()
            .expectMessageIs(
                "druid.emitter.kafka.request.topic must be specified if druid.emitter.kafka.event.types contains requests."
            )
    );
  }

  @Test
  public void testNullSegmentMetadataTopic()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new KafkaEmitterConfig(
                "foo",
                new HashSet<>(Arrays.asList(KafkaEmitterConfig.EventType.values())),
                "foo",
                "bar",
                "baz",
                null,
                null,
                null,
                null,
                null
            )
        ),
        operatorExceptionMatcher()
            .expectMessageIs(
                "druid.emitter.kafka.segmentMetadata.topic must be specified if druid.emitter.kafka.event.types contains segment_metadata."
            )
    );
  }

  private DruidExceptionMatcher operatorExceptionMatcher()
  {
    return new DruidExceptionMatcher(
        DruidException.Persona.OPERATOR,
        DruidException.Category.NOT_FOUND,
        "general"
    );
  }
}

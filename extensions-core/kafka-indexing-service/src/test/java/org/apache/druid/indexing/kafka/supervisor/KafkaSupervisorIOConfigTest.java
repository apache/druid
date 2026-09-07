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

package org.apache.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.kafka.KafkaConsumerConfigs;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.KafkaRecordSupplier;
import org.apache.druid.indexing.seekablestream.extension.KafkaConfigOverrides;
import org.apache.druid.indexing.seekablestream.supervisor.BoundedStreamConfig;
import org.apache.druid.indexing.seekablestream.supervisor.IdleConfig;
import org.apache.druid.indexing.seekablestream.supervisor.LagAggregator;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.LagBasedAutoScalerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.easymock.EasyMock.createMock;

public class KafkaSupervisorIOConfigTest
{
  private final ObjectMapper mapper;

  public KafkaSupervisorIOConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules(new KafkaIndexTaskModule().getJacksonModules());
  }

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"topic\": \"my-topic\",\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"}\n"
                     + "}";

    KafkaSupervisorIOConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                KafkaSupervisorIOConfig.class
            )
        ), KafkaSupervisorIOConfig.class
    );

    Assertions.assertEquals("my-topic", config.getTopic());
    Assertions.assertNull(config.getTopicPattern());
    Assertions.assertEquals(1, (int) config.getReplicas());
    Assertions.assertEquals(1, (int) config.getTaskCount());
    Assertions.assertNull(config.getStopTaskCount());
    Assertions.assertEquals((int) config.getTaskCount(), config.getMaxAllowedStops());
    Assertions.assertEquals(Duration.standardMinutes(60), config.getTaskDuration());
    Assertions.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assertions.assertEquals(100, config.getPollTimeout());
    Assertions.assertEquals(Duration.standardSeconds(5), config.getStartDelay());
    Assertions.assertEquals(Duration.standardSeconds(30), config.getPeriod());
    Assertions.assertFalse(config.isUseEarliestOffset());
    Assertions.assertEquals(Duration.standardMinutes(30), config.getCompletionTimeout());
    Assertions.assertFalse(config.getLateMessageRejectionPeriod().isPresent(), "lateMessageRejectionPeriod");
    Assertions.assertFalse(config.getEarlyMessageRejectionPeriod().isPresent(), "earlyMessageRejectionPeriod");
    Assertions.assertFalse(config.getLateMessageRejectionStartDateTime().isPresent(), "lateMessageRejectionStartDateTime");
  }

  @Test
  public void testSerdeWithTopicPattern() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"topicPattern\": \"my-topic.*\",\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"}\n"
                     + "}";

    KafkaSupervisorIOConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                KafkaSupervisorIOConfig.class
            )
        ), KafkaSupervisorIOConfig.class
    );

    Assertions.assertEquals("my-topic.*", config.getTopicPattern());
    Assertions.assertNull(config.getTopic());
  }

  @Test
  public void testSerdeWithNonDefaultsWithLateMessagePeriod() throws Exception
  {
    String jsonStr = "{\n"
        + "  \"type\": \"kafka\",\n"
        + "  \"topic\": \"my-topic\",\n"
        + "  \"replicas\": 3,\n"
        + "  \"taskCount\": 9,\n"
        + "  \"taskDuration\": \"PT30M\",\n"
        + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
        + "  \"pollTimeout\": 1000,\n"
        + "  \"startDelay\": \"PT1M\",\n"
        + "  \"period\": \"PT10S\",\n"
        + "  \"useEarliestOffset\": true,\n"
        + "  \"completionTimeout\": \"PT45M\",\n"
        + "  \"lateMessageRejectionPeriod\": \"PT1H\",\n"
        + "  \"earlyMessageRejectionPeriod\": \"PT1H\"\n"
        + "}";

    KafkaSupervisorIOConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                KafkaSupervisorIOConfig.class
                )
            ), KafkaSupervisorIOConfig.class
        );

    Assertions.assertEquals("my-topic", config.getTopic());
    Assertions.assertNull(config.getTopicPattern());
    Assertions.assertEquals(3, (int) config.getReplicas());
    Assertions.assertEquals(9, (int) config.getTaskCount());
    Assertions.assertEquals(Duration.standardMinutes(30), config.getTaskDuration());
    Assertions.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assertions.assertEquals(1000, config.getPollTimeout());
    Assertions.assertEquals(Duration.standardMinutes(1), config.getStartDelay());
    Assertions.assertEquals(Duration.standardSeconds(10), config.getPeriod());
    Assertions.assertTrue(config.isUseEarliestOffset());
    Assertions.assertEquals(Duration.standardMinutes(45), config.getCompletionTimeout());
    Assertions.assertEquals(Duration.standardHours(1), config.getLateMessageRejectionPeriod().orNull());
    Assertions.assertEquals(Duration.standardHours(1), config.getEarlyMessageRejectionPeriod().orNull());
  }

  @Test
  public void testSerdeWithNonDefaultsWithLateMessageStartDateTime() throws Exception
  {
    String jsonStr = "{\n"
        + "  \"type\": \"kafka\",\n"
        + "  \"topic\": \"my-topic\",\n"
        + "  \"replicas\": 3,\n"
        + "  \"taskCount\": 9,\n"
        + "  \"taskDuration\": \"PT30M\",\n"
        + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
        + "  \"pollTimeout\": 1000,\n"
        + "  \"startDelay\": \"PT1M\",\n"
        + "  \"period\": \"PT10S\",\n"
        + "  \"useEarliestOffset\": true,\n"
        + "  \"completionTimeout\": \"PT45M\",\n"
        + "  \"earlyMessageRejectionPeriod\": \"PT1H\",\n"
        + "  \"lateMessageRejectionStartDateTime\": \"2016-05-31T12:00Z\"\n"
        + "}";

    KafkaSupervisorIOConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                KafkaSupervisorIOConfig.class
                )
            ), KafkaSupervisorIOConfig.class
        );

    Assertions.assertEquals("my-topic", config.getTopic());
    Assertions.assertNull(config.getTopicPattern());
    Assertions.assertEquals(3, (int) config.getReplicas());
    Assertions.assertEquals(9, (int) config.getTaskCount());
    Assertions.assertEquals(Duration.standardMinutes(30), config.getTaskDuration());
    Assertions.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assertions.assertEquals(1000, config.getPollTimeout());
    Assertions.assertEquals(Duration.standardMinutes(1), config.getStartDelay());
    Assertions.assertEquals(Duration.standardSeconds(10), config.getPeriod());
    Assertions.assertTrue(config.isUseEarliestOffset());
    Assertions.assertEquals(Duration.standardMinutes(45), config.getCompletionTimeout());
    Assertions.assertEquals(DateTimes.of("2016-05-31T12:00Z"), config.getLateMessageRejectionStartDateTime().orNull());
  }

  @Test
  public void testSerdeForConsumerPropertiesWithPasswords() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"topic\": \"my-topic\",\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\",\n"
                     + "   \"ssl.truststore.password\":{\"type\": \"default\", \"password\": \"mytruststorepassword\"},\n"
                     + "   \"ssl.keystore.password\":{\"type\": \"default\", \"password\": \"mykeystorepassword\"},\n"
                     + "   \"ssl.key.password\":\"mykeypassword\"}\n"
                     + "}";

    KafkaSupervisorIOConfig config = mapper.readValue(jsonStr, KafkaSupervisorIOConfig.class);
    Properties props = new Properties();
    KafkaRecordSupplier.addConsumerPropertiesFromConfig(props, mapper, config.getConsumerProperties());

    Assertions.assertEquals("my-topic", config.getTopic());
    Assertions.assertNull(config.getTopicPattern());
    Assertions.assertEquals("localhost:9092", props.getProperty("bootstrap.servers"));
    Assertions.assertEquals("mytruststorepassword", props.getProperty("ssl.truststore.password"));
    Assertions.assertEquals("mykeystorepassword", props.getProperty("ssl.keystore.password"));
    Assertions.assertEquals("mykeypassword", props.getProperty("ssl.key.password"));
  }

  @Test
  public void testTopicRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"}\n"
                     + "}";

    assertJsonMappingException(
        DruidException.class,
        "Either topic or topicPattern must be specified",
        () -> mapper.readValue(jsonStr, KafkaSupervisorIOConfig.class)
    );
  }

  @Test
  public void testConsumerPropertiesRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"topic\": \"my-topic\"\n"
                     + "}";

    assertJsonMappingException(
        NullPointerException.class,
        "consumerProperties",
        () -> mapper.readValue(jsonStr, KafkaSupervisorIOConfig.class)
    );
  }

  @Test
  public void testBootstrapServersRequired() throws Exception
  {
    String jsonStr = "{\n"
        + "  \"type\": \"kafka\",\n"
        + "  \"topic\": \"my-topic\",\n"
        + "  \"consumerProperties\": {}\n"
        + "}";

    assertJsonMappingException(
        NullPointerException.class,
        "bootstrap.servers",
        () -> mapper.readValue(jsonStr, KafkaSupervisorIOConfig.class)
    );
  }

  @Test
  public void testSerdeWithBothExclusiveProperties()
  {
    Assertions.assertThrows(JsonMappingException.class, () -> {
      String jsonStr = "{\n"
          + "  \"type\": \"kafka\",\n"
          + "  \"topic\": \"my-topic\",\n"
          + "  \"replicas\": 3,\n"
          + "  \"taskCount\": 9,\n"
          + "  \"taskDuration\": \"PT30M\",\n"
          + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
          + "  \"pollTimeout\": 1000,\n"
          + "  \"startDelay\": \"PT1M\",\n"
          + "  \"period\": \"PT10S\",\n"
          + "  \"useEarliestOffset\": true,\n"
          + "  \"completionTimeout\": \"PT45M\",\n"
          + "  \"lateMessageRejectionPeriod\": \"PT1H\",\n"
          + "  \"earlyMessageRejectionPeriod\": \"PT1H\",\n"
          + "  \"lateMessageRejectionStartDateTime\": \"2016-05-31T12:00Z\"\n"
          + "}";
      mapper.readValue(
          mapper.writeValueAsString(
              mapper.readValue(
                  jsonStr,
                  KafkaSupervisorIOConfig.class
              )
          ), KafkaSupervisorIOConfig.class
      );
    });
  }

  @Test
  public void testAutoScalingConfigSerde() throws JsonProcessingException
  {
    HashMap<String, Object> autoScalerConfig = new HashMap<>();
    autoScalerConfig.put("enableTaskAutoScaler", true);
    autoScalerConfig.put("lagCollectionIntervalMillis", 500);
    autoScalerConfig.put("lagCollectionRangeMillis", 500);
    autoScalerConfig.put("scaleOutThreshold", 0);
    autoScalerConfig.put("triggerScaleOutFractionThreshold", 0.0);
    autoScalerConfig.put("scaleInThreshold", 1000000);
    autoScalerConfig.put("triggerScaleInFractionThreshold", 0.8);
    autoScalerConfig.put("scaleActionStartDelayMillis", 0);
    autoScalerConfig.put("scaleActionPeriodMillis", 100);
    autoScalerConfig.put("taskCountMax", 10);
    autoScalerConfig.put("taskCountMin", 1);
    autoScalerConfig.put("scaleInStep", 1);
    autoScalerConfig.put("scaleOutStep", 2);
    autoScalerConfig.put("minTriggerScaleActionFrequencyMillis", 1200000);
    autoScalerConfig.put("minScaleUpDelay", "PT20M");
    autoScalerConfig.put("minScaleDownDelay", "PT20M");

    final Map<String, Object> consumerProperties = KafkaConsumerConfigs.getConsumerProperties();
    consumerProperties.put("bootstrap.servers", "localhost:8082");

    KafkaSupervisorIOConfig kafkaSupervisorIOConfig = new KafkaSupervisorIOConfig(
        "test",
        null,
        null,
        1,
        1,
        new Period("PT1H"),
        consumerProperties,
        mapper.convertValue(autoScalerConfig, LagBasedAutoScalerConfig.class),
        null,
        KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
        new Period("P1D"),
        new Period("PT30S"),
        true,
        new Period("PT30M"),
        null,
        null,
        null,
        null,
        null,
        null,
        false,
        null,
        null
    );
    String ioConfig = mapper.writeValueAsString(kafkaSupervisorIOConfig);
    KafkaSupervisorIOConfig kafkaSupervisorIOConfig1 = mapper.readValue(ioConfig, KafkaSupervisorIOConfig.class);
    Assertions.assertNotNull(kafkaSupervisorIOConfig1.getAutoScalerConfig());
    Assertions.assertTrue(kafkaSupervisorIOConfig1.getAutoScalerConfig().getEnableTaskAutoScaler());
    Assertions.assertEquals(1, kafkaSupervisorIOConfig1.getAutoScalerConfig().getTaskCountMin());
    Assertions.assertEquals(10, kafkaSupervisorIOConfig1.getAutoScalerConfig().getTaskCountMax());
    Assertions.assertEquals(
        1200000,
        kafkaSupervisorIOConfig1.getAutoScalerConfig().getMinTriggerScaleActionFrequencyMillis()
    );

    autoScalerConfig.put("taskCountStart", 5);
    kafkaSupervisorIOConfig = new KafkaSupervisorIOConfig(
        "test",
        null,
        null,
        1,
        1,
        new Period("PT1H"),
        consumerProperties,
        mapper.convertValue(autoScalerConfig, LagBasedAutoScalerConfig.class),
        LagAggregator.DEFAULT,
        KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
        new Period("P1D"),
        new Period("PT30S"),
        true,
        new Period("PT30M"),
        null,
        null,
        null,
        null,
        null,
        null,
        false,
        null,
        null
    );
    Assertions.assertEquals(1, kafkaSupervisorIOConfig.getTaskCount());

    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          autoScalerConfig.put("taskCountStart", 11); // > max task count
          mapper.convertValue(autoScalerConfig, LagBasedAutoScalerConfig.class);
        },
        "taskCountMin <= taskCountStart <= taskCountMax"
    );

    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          autoScalerConfig.put("taskCountStart", 0); // < min task count
          mapper.convertValue(autoScalerConfig, LagBasedAutoScalerConfig.class);
        },
        "taskCountMin <= taskCountStart <= taskCountMax"
    );
  }

  @Test
  public void testTaskCountStartFallbackAndExplicitFlag()
  {
    final Map<String, Object> autoScalerConfig = ImmutableMap.of(
        "enableTaskAutoScaler", true,
        "taskCountMin", 1,
        "taskCountMax", 10,
        "taskCountStart", 5
    );

    Assertions.assertEquals(7, makeIOConfig(7, autoScalerConfig).getTaskCount());
    Assertions.assertTrue(makeIOConfig(7, autoScalerConfig).isTaskCountExplicit());

    Assertions.assertEquals(5, makeIOConfig(null, autoScalerConfig).getTaskCount());
    Assertions.assertFalse(makeIOConfig(null, autoScalerConfig).isTaskCountExplicit());
  }

  private KafkaSupervisorIOConfig makeIOConfig(Integer taskCount, Map<String, Object> autoScalerConfig)
  {
    return new KafkaSupervisorIOConfig(
        "test",
        null,
        null,
        1,
        taskCount,
        new Period("PT1H"),
        ImmutableMap.of("bootstrap.servers", "localhost:8082"),
        mapper.convertValue(autoScalerConfig, LagBasedAutoScalerConfig.class),
        LagAggregator.DEFAULT,
        KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
        new Period("P1D"),
        new Period("PT30S"),
        true,
        new Period("PT30M"),
        null,
        null,
        null,
        null,
        null,
        null,
        false,
        null,
        null
    );
  }

  @Test
  public void testIdleConfigSerde() throws JsonProcessingException
  {
    HashMap<String, Object> idleConfig = new HashMap<>();
    idleConfig.put("enabled", true);
    idleConfig.put("inactiveAfterMillis", 600000L);

    final Map<String, Object> consumerProperties = KafkaConsumerConfigs.getConsumerProperties();
    consumerProperties.put("bootstrap.servers", "localhost:8082");

    KafkaSupervisorIOConfig kafkaSupervisorIOConfig = new KafkaSupervisorIOConfig(
        "test",
        null,
        null,
        1,
        1,
        new Period("PT1H"),
        consumerProperties,
        null,
        null,
        KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
        new Period("P1D"),
        new Period("PT30S"),
        true,
        new Period("PT30M"),
        null,
        null,
        null,
        null,
        mapper.convertValue(idleConfig, IdleConfig.class),
        null,
        false,
        null,
        null
    );
    String ioConfig = mapper.writeValueAsString(kafkaSupervisorIOConfig);
    KafkaSupervisorIOConfig kafkaSupervisorIOConfig1 = mapper.readValue(ioConfig, KafkaSupervisorIOConfig.class);

    Assertions.assertNotNull(kafkaSupervisorIOConfig1.getIdleConfig());
    Assertions.assertTrue(kafkaSupervisorIOConfig1.getIdleConfig().isEnabled());
    Assertions.assertEquals(Long.valueOf(600000), kafkaSupervisorIOConfig1.getIdleConfig().getInactiveAfterMillis());
  }

  @Test
  public void testBoundedModeSerdeWithIntegerOffsets() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"topic\": \"my-topic\",\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"boundedStreamConfig\": {\n"
                     + "    \"startSequenceNumbers\": {\"0\": 100, \"1\": 200},\n"
                     + "    \"endSequenceNumbers\": {\"0\": 500, \"1\": 600}\n"
                     + "  }\n"
                     + "}";

    KafkaSupervisorIOConfig config = mapper.readValue(jsonStr, KafkaSupervisorIOConfig.class);

    Assertions.assertTrue(config.isBounded());
    Assertions.assertNotNull(config.getBoundedStreamConfig());
    Assertions.assertEquals(2, config.getBoundedStreamConfig().getStartSequenceNumbers().size());
    Assertions.assertEquals(2, config.getBoundedStreamConfig().getEndSequenceNumbers().size());
  }

  @Test
  public void testBoundedModeSerdeWithStringOffsets() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"topic\": \"my-topic\",\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"boundedStreamConfig\": {\n"
                     + "    \"startSequenceNumbers\": {\"0\": \"100\", \"1\": \"200\"},\n"
                     + "    \"endSequenceNumbers\": {\"0\": \"500\", \"1\": \"600\"}\n"
                     + "  }\n"
                     + "}";

    KafkaSupervisorIOConfig config = mapper.readValue(jsonStr, KafkaSupervisorIOConfig.class);

    Assertions.assertTrue(config.isBounded());
    Assertions.assertNotNull(config.getBoundedStreamConfig());
    Assertions.assertEquals(2, config.getBoundedStreamConfig().getStartSequenceNumbers().size());
    Assertions.assertEquals(2, config.getBoundedStreamConfig().getEndSequenceNumbers().size());
  }

  @Test
  public void testBoundedModeSerdeWithMixedOffsets() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"topic\": \"my-topic\",\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"boundedStreamConfig\": {\n"
                     + "    \"startSequenceNumbers\": {\"0\": 100, \"1\": \"200\"},\n"
                     + "    \"endSequenceNumbers\": {\"0\": 500, \"1\": \"600\"}\n"
                     + "  }\n"
                     + "}";

    KafkaSupervisorIOConfig config = mapper.readValue(jsonStr, KafkaSupervisorIOConfig.class);

    Assertions.assertTrue(config.isBounded());
    Assertions.assertNotNull(config.getBoundedStreamConfig());
  }

  @Test
  public void testUnboundedModeByDefault() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"topic\": \"my-topic\",\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"}\n"
                     + "}";

    KafkaSupervisorIOConfig config = mapper.readValue(jsonStr, KafkaSupervisorIOConfig.class);

    Assertions.assertFalse(config.isBounded());
    Assertions.assertNull(config.getBoundedStreamConfig());
  }

  @Test
  public void testBoundedModeRoundTrip() throws Exception
  {
    final Map<String, Object> consumerProperties = KafkaConsumerConfigs.getConsumerProperties();
    consumerProperties.put("bootstrap.servers", "localhost:8082");

    Map<String, Integer> startOffsets = new HashMap<>();
    startOffsets.put("0", 100);
    startOffsets.put("1", 200);

    Map<String, Integer> endOffsets = new HashMap<>();
    endOffsets.put("0", 500);
    endOffsets.put("1", 600);

    BoundedStreamConfig boundedConfig = new BoundedStreamConfig(startOffsets, endOffsets);

    KafkaSupervisorIOConfig original = new KafkaSupervisorIOConfig(
        "test-topic",
        null,
        null,
        1,
        1,
        new Period("PT1H"),
        consumerProperties,
        null,
        LagAggregator.DEFAULT,
        KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
        new Period("P1D"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        null,
        null,
        null,
        null,
        false,
        null,
        boundedConfig
    );

    String json = mapper.writeValueAsString(original);
    KafkaSupervisorIOConfig deserialized = mapper.readValue(json, KafkaSupervisorIOConfig.class);

    Assertions.assertTrue(deserialized.isBounded());
    Assertions.assertNotNull(deserialized.getBoundedStreamConfig());
    Assertions.assertEquals(2, deserialized.getBoundedStreamConfig().getStartSequenceNumbers().size());
    Assertions.assertEquals(2, deserialized.getBoundedStreamConfig().getEndSequenceNumbers().size());
  }

  private static KafkaIOConfigBuilder ioConfigBuilder()
  {
    return new KafkaIOConfigBuilder()
        .withTopic("topic")
        .withConsumerProperties(Map.of("bootstrap.servers", "localhost:9092"))
        .withReplicas(1)
        .withTaskCount(2)
        .withTaskDuration(new Period("PT1H"));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    final KafkaSupervisorIOConfig config = ioConfigBuilder().build();
    Assertions.assertEquals(config, ioConfigBuilder().build());
    Assertions.assertEquals(config.hashCode(), ioConfigBuilder().build().hashCode());
    Assertions.assertNotEquals(config, null);
    Assertions.assertNotEquals(config, "not an io config");
    Assertions.assertNotEquals(config, ioConfigBuilder().withTopic("other").build());
    Assertions.assertNotEquals(config, ioConfigBuilder().withReplicas(9).build());
    Assertions.assertNotEquals(config, ioConfigBuilder().withTaskCount(9).build());
    Assertions.assertNotEquals(
        config,
        ioConfigBuilder().withConsumerProperties(Map.of("bootstrap.servers", "other:9092")).build()
    );
    Assertions.assertNotEquals(config, ioConfigBuilder().withEmitTimeLagMetrics(true).build());
  }

  @Test
  public void testTuningConfigEqualsAndHashCode()
  {
    final KafkaSupervisorTuningConfig config = new KafkaTuningConfigBuilder().build();
    Assertions.assertEquals(config, new KafkaTuningConfigBuilder().build());
    Assertions.assertEquals(config.hashCode(), new KafkaTuningConfigBuilder().build().hashCode());
    Assertions.assertNotEquals(config, null);
    Assertions.assertNotEquals(config, "not a tuning config");
    Assertions.assertNotEquals(config, new KafkaTuningConfigBuilder().withWorkerThreads(99).build());
    Assertions.assertNotEquals(config, new KafkaTuningConfigBuilder().withShutdownTimeout(new Period("PT99M")).build());
    Assertions.assertNotEquals(config, new KafkaTuningConfigBuilder().withOffsetFetchPeriod(new Period("PT99S")).build());
  }

  /**
   * Drift guard for this class's own fields (base fields are covered by SeekableStreamSupervisorIOConfigTest):
   * a field omitted from {@code equals} would let a changed spec persist without restarting the supervisor.
   */
  @Test
  public void testEqualsContractCoversAllFields()
  {
    EqualsVerifier.forClass(KafkaSupervisorIOConfig.class)
                  .usingGetClass()
                  .withRedefinedSuperclass()
                  .withIgnoredFields("taskCountExplicit", "autoScalerEnabled")
                  .suppress(Warning.NONFINAL_FIELDS)
                  .withPrefabValues(Optional.class, Optional.of("a"), Optional.of("b"))
                  .withPrefabValues(InputFormat.class, createMock(InputFormat.class), createMock(InputFormat.class))
                  .withPrefabValues(AutoScalerConfig.class, createMock(AutoScalerConfig.class), createMock(AutoScalerConfig.class))
                  .withPrefabValues(LagAggregator.class, createMock(LagAggregator.class), createMock(LagAggregator.class))
                  .withPrefabValues(
                      KafkaConfigOverrides.class,
                      createMock(KafkaConfigOverrides.class),
                      createMock(KafkaConfigOverrides.class)
                  )
                  .verify();
  }

  @Test
  public void testTuningConfigEqualsContractCoversAllFields()
  {
    EqualsVerifier.forClass(KafkaSupervisorTuningConfig.class)
                  .usingGetClass()
                  .withRedefinedSuperclass()
                  .suppress(Warning.NONFINAL_FIELDS)
                  .verify();
  }

  private static void assertJsonMappingException(
      Class<? extends Throwable> causeType,
      String expectedMessage,
      Executable executable
  )
  {
    final JsonMappingException exception = Assertions.assertThrows(JsonMappingException.class, executable);
    Assertions.assertInstanceOf(causeType, exception.getCause());
    Assertions.assertTrue(exception.getMessage().contains(expectedMessage));
  }
}

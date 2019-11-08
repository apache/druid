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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.KafkaRecordSupplier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.hamcrest.CoreMatchers;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Properties;

public class KafkaSupervisorIOConfigTest
{
  private final ObjectMapper mapper;

  public KafkaSupervisorIOConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules((Iterable<Module>) new KafkaIndexTaskModule().getJacksonModules());
  }

  @Rule
  public final ExpectedException exception = ExpectedException.none();

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

    Assert.assertEquals("my-topic", config.getTopic());
    Assert.assertEquals(1, (int) config.getReplicas());
    Assert.assertEquals(1, (int) config.getTaskCount());
    Assert.assertEquals(Duration.standardMinutes(60), config.getTaskDuration());
    Assert.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assert.assertEquals(100, config.getPollTimeout());
    Assert.assertEquals(Duration.standardSeconds(5), config.getStartDelay());
    Assert.assertEquals(Duration.standardSeconds(30), config.getPeriod());
    Assert.assertEquals(false, config.isUseEarliestOffset());
    Assert.assertEquals(Duration.standardMinutes(30), config.getCompletionTimeout());
    Assert.assertFalse("lateMessageRejectionPeriod", config.getLateMessageRejectionPeriod().isPresent());
    Assert.assertFalse("earlyMessageRejectionPeriod", config.getEarlyMessageRejectionPeriod().isPresent());
    Assert.assertFalse("lateMessageRejectionStartDateTime", config.getLateMessageRejectionStartDateTime().isPresent());
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

    Assert.assertEquals("my-topic", config.getTopic());
    Assert.assertEquals(3, (int) config.getReplicas());
    Assert.assertEquals(9, (int) config.getTaskCount());
    Assert.assertEquals(Duration.standardMinutes(30), config.getTaskDuration());
    Assert.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assert.assertEquals(1000, config.getPollTimeout());
    Assert.assertEquals(Duration.standardMinutes(1), config.getStartDelay());
    Assert.assertEquals(Duration.standardSeconds(10), config.getPeriod());
    Assert.assertEquals(true, config.isUseEarliestOffset());
    Assert.assertEquals(Duration.standardMinutes(45), config.getCompletionTimeout());
    Assert.assertEquals(Duration.standardHours(1), config.getLateMessageRejectionPeriod().get());
    Assert.assertEquals(Duration.standardHours(1), config.getEarlyMessageRejectionPeriod().get());
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

    Assert.assertEquals("my-topic", config.getTopic());
    Assert.assertEquals(3, (int) config.getReplicas());
    Assert.assertEquals(9, (int) config.getTaskCount());
    Assert.assertEquals(Duration.standardMinutes(30), config.getTaskDuration());
    Assert.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assert.assertEquals(1000, config.getPollTimeout());
    Assert.assertEquals(Duration.standardMinutes(1), config.getStartDelay());
    Assert.assertEquals(Duration.standardSeconds(10), config.getPeriod());
    Assert.assertEquals(true, config.isUseEarliestOffset());
    Assert.assertEquals(Duration.standardMinutes(45), config.getCompletionTimeout());
    Assert.assertEquals(DateTimes.of("2016-05-31T12:00Z"), config.getLateMessageRejectionStartDateTime().get());
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

    Assert.assertEquals("my-topic", config.getTopic());
    Assert.assertEquals("localhost:9092", props.getProperty("bootstrap.servers"));
    Assert.assertEquals("mytruststorepassword", props.getProperty("ssl.truststore.password"));
    Assert.assertEquals("mykeystorepassword", props.getProperty("ssl.keystore.password"));
    Assert.assertEquals("mykeypassword", props.getProperty("ssl.key.password"));
  }

  @Test
  public void testTopicRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"}\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("topic"));
    mapper.readValue(jsonStr, KafkaSupervisorIOConfig.class);
  }

  @Test
  public void testConsumerPropertiesRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"topic\": \"my-topic\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("consumerProperties"));
    mapper.readValue(jsonStr, KafkaSupervisorIOConfig.class);
  }

  @Test
  public void testBootstrapServersRequired() throws Exception
  {
    String jsonStr = "{\n"
        + "  \"type\": \"kafka\",\n"
        + "  \"topic\": \"my-topic\",\n"
        + "  \"consumerProperties\": {}\n"
        + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("bootstrap.servers"));
    mapper.readValue(jsonStr, KafkaSupervisorIOConfig.class);
  }

  @Test
  public void testSerdeWithBothExclusiveProperties() throws Exception
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
        + "  \"earlyMessageRejectionPeriod\": \"PT1H\",\n"
        + "  \"lateMessageRejectionStartDateTime\": \"2016-05-31T12:00Z\"\n"
        + "}";
    exception.expect(JsonMappingException.class);
    KafkaSupervisorIOConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                KafkaSupervisorIOConfig.class
                )
            ), KafkaSupervisorIOConfig.class
        );
  }
}

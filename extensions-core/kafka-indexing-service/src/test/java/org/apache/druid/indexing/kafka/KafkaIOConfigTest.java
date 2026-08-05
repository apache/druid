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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.indexing.IOConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Collections;

public class KafkaIOConfigTest
{
  private final ObjectMapper mapper;

  public KafkaIOConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules(new KafkaIndexTaskModule().getJacksonModules());
  }

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"}\n"
                     + "}";

    KafkaIndexTaskIOConfig config = (KafkaIndexTaskIOConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                IOConfig.class
            )
        ), IOConfig.class
    );

    Assertions.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assertions.assertEquals("mytopic", config.getStartSequenceNumbers().getStream());
    Assertions.assertEquals(ImmutableMap.of(new KafkaTopicPartition(false, null, 0), 1L, new KafkaTopicPartition(false, null, 1), 10L),
                        config.getStartSequenceNumbers().getPartitionSequenceNumberMap());
    Assertions.assertEquals("mytopic", config.getEndSequenceNumbers().getStream());
    Assertions.assertEquals(ImmutableMap.of(new KafkaTopicPartition(false, null, 0), 15L, new KafkaTopicPartition(false, null, 1),
                                        200L), config.getEndSequenceNumbers().getPartitionSequenceNumberMap());
    Assertions.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assertions.assertTrue(config.isUseTransaction());
    Assertions.assertNull(config.getMinimumMessageTime(), "minimumMessageTime");
    Assertions.assertNull(config.getMaximumMessageTime(), "maximumMessageTime");
    Assertions.assertEquals(Collections.emptySet(), config.getStartSequenceNumbers().getExclusivePartitions());
  }

  @Test
  public void testSerdeWithDefaultsAndSequenceNumbers() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mytopic\", \"partitionSequenceNumberMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"mytopic\", \"partitionSequenceNumberMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"}\n"
                     + "}";

    KafkaIndexTaskIOConfig config = (KafkaIndexTaskIOConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                IOConfig.class
            )
        ), IOConfig.class
    );

    Assertions.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assertions.assertEquals("mytopic", config.getStartSequenceNumbers().getStream());
    Assertions.assertEquals(ImmutableMap.of(new KafkaTopicPartition(false, null, 0), 1L, new KafkaTopicPartition(false, null, 1), 10L),
                        config.getStartSequenceNumbers().getPartitionSequenceNumberMap());
    Assertions.assertEquals("mytopic", config.getEndSequenceNumbers().getStream());
    Assertions.assertEquals(ImmutableMap.of(new KafkaTopicPartition(false, null, 0), 15L, new KafkaTopicPartition(false, null, 1),
                                        200L), config.getEndSequenceNumbers().getPartitionSequenceNumberMap());
    Assertions.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assertions.assertTrue(config.isUseTransaction());
    Assertions.assertNull(config.getMinimumMessageTime(), "minimumMessageTime");
    Assertions.assertNull(config.getMaximumMessageTime(), "maximumMessageTime");
    Assertions.assertEquals(Collections.emptySet(), config.getStartSequenceNumbers().getExclusivePartitions());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    KafkaIndexTaskIOConfig config = (KafkaIndexTaskIOConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                IOConfig.class
            )
        ), IOConfig.class
    );

    Assertions.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assertions.assertEquals("mytopic", config.getStartSequenceNumbers().getStream());
    Assertions.assertEquals(ImmutableMap.of(new KafkaTopicPartition(false, null, 0), 1L, new KafkaTopicPartition(false, null, 1), 10L),
                        config.getStartSequenceNumbers().getPartitionSequenceNumberMap());
    Assertions.assertEquals("mytopic", config.getEndSequenceNumbers().getStream());
    Assertions.assertEquals(ImmutableMap.of(new KafkaTopicPartition(false, null, 0), 15L, new KafkaTopicPartition(false, null, 1),
                                        200L), config.getEndSequenceNumbers().getPartitionSequenceNumberMap());
    Assertions.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assertions.assertFalse(config.isUseTransaction());
    Assertions.assertEquals(DateTimes.of("2016-05-31T12:00Z"), config.getMinimumMessageTime());
    Assertions.assertEquals(DateTimes.of("2016-05-31T14:00Z"), config.getMaximumMessageTime());
    Assertions.assertEquals(Collections.emptySet(), config.getStartSequenceNumbers().getExclusivePartitions());
  }

  @Test
  public void testBaseSequenceNameRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    assertJsonMappingException(
        NullPointerException.class,
        "baseSequenceName",
        () -> mapper.readValue(jsonStr, IOConfig.class)
    );
  }

  @Test
  public void testStartPartitionsRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    assertJsonMappingException(
        NullPointerException.class,
        "startPartitions",
        () -> mapper.readValue(jsonStr, IOConfig.class)
    );
  }

  @Test
  public void testEndPartitionsRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    assertJsonMappingException(
        NullPointerException.class,
        "endSequenceNumbers",
        () -> mapper.readValue(jsonStr, IOConfig.class)
    );
  }

  @Test
  public void testConsumerPropertiesRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    assertJsonMappingException(
        NullPointerException.class,
        "consumerProperties",
        () -> mapper.readValue(jsonStr, IOConfig.class)
    );
  }

  @Test
  public void testStartAndEndTopicMatch() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"other\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    assertJsonMappingException(
        IllegalArgumentException.class,
        "start topic/stream and end topic/stream must match",
        () -> mapper.readValue(jsonStr, IOConfig.class)
    );
  }

  @Test
  public void testStartAndEndPartitionSetMatch() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    assertJsonMappingException(
        IllegalArgumentException.class,
        "start partition set and end partition set must match",
        () -> mapper.readValue(jsonStr, IOConfig.class)
    );
  }

  @Test
  public void testEndOffsetGreaterThanStart() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":2}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    assertJsonMappingException(
        IllegalArgumentException.class,
        "end offset must be >= start offset",
        () -> mapper.readValue(jsonStr, IOConfig.class)
    );
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

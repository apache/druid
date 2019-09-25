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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.indexing.IOConfig;
import org.hamcrest.CoreMatchers;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class KafkaIOConfigTest
{
  private final ObjectMapper mapper;

  public KafkaIOConfigTest()
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

    Assert.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assert.assertEquals("mytopic", config.getStartSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of(0, 1L, 1, 10L), config.getStartSequenceNumbers().getPartitionSequenceNumberMap());
    Assert.assertEquals("mytopic", config.getEndSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of(0, 15L, 1, 200L), config.getEndSequenceNumbers().getPartitionSequenceNumberMap());
    Assert.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assert.assertTrue(config.isUseTransaction());
    Assert.assertFalse("minimumMessageTime", config.getMinimumMessageTime().isPresent());
    Assert.assertFalse("maximumMessageTime", config.getMaximumMessageTime().isPresent());
    Assert.assertEquals(Collections.emptySet(), config.getStartSequenceNumbers().getExclusivePartitions());
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

    Assert.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assert.assertEquals("mytopic", config.getStartSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of(0, 1L, 1, 10L), config.getStartSequenceNumbers().getPartitionSequenceNumberMap());
    Assert.assertEquals("mytopic", config.getEndSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of(0, 15L, 1, 200L), config.getEndSequenceNumbers().getPartitionSequenceNumberMap());
    Assert.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assert.assertTrue(config.isUseTransaction());
    Assert.assertFalse("minimumMessageTime", config.getMinimumMessageTime().isPresent());
    Assert.assertFalse("maximumMessageTime", config.getMaximumMessageTime().isPresent());
    Assert.assertEquals(Collections.emptySet(), config.getStartSequenceNumbers().getExclusivePartitions());
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

    Assert.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assert.assertEquals("mytopic", config.getStartSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of(0, 1L, 1, 10L), config.getStartSequenceNumbers().getPartitionSequenceNumberMap());
    Assert.assertEquals("mytopic", config.getEndSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of(0, 15L, 1, 200L), config.getEndSequenceNumbers().getPartitionSequenceNumberMap());
    Assert.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assert.assertFalse(config.isUseTransaction());
    Assert.assertEquals(DateTimes.of("2016-05-31T12:00Z"), config.getMinimumMessageTime().get());
    Assert.assertEquals(DateTimes.of("2016-05-31T14:00Z"), config.getMaximumMessageTime().get());
    Assert.assertEquals(Collections.emptySet(), config.getStartSequenceNumbers().getExclusivePartitions());
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

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("baseSequenceName"));
    mapper.readValue(jsonStr, IOConfig.class);
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

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("startPartitions"));
    mapper.readValue(jsonStr, IOConfig.class);
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

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("endSequenceNumbers"));
    mapper.readValue(jsonStr, IOConfig.class);
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

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("consumerProperties"));
    mapper.readValue(jsonStr, IOConfig.class);
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

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));
    exception.expectMessage(CoreMatchers.containsString("start topic/stream and end topic/stream must match"));
    mapper.readValue(jsonStr, IOConfig.class);
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

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));
    exception.expectMessage(CoreMatchers.containsString("start partition set and end partition set must match"));
    mapper.readValue(jsonStr, IOConfig.class);
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

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));
    exception.expectMessage(CoreMatchers.containsString("end offset must be >= start offset"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testDeserializeToOldIoConfig() throws IOException
  {
    final KafkaIndexTaskIOConfig currentConfig = new KafkaIndexTaskIOConfig(
        0,
        "baseSequenceNamee",
        null,
        null,
        new SeekableStreamStartSequenceNumbers<>("stream", ImmutableMap.of(1, 10L, 2, 5L), null),
        new SeekableStreamEndSequenceNumbers<>("stream", ImmutableMap.of(1, 20L, 2, 30L)),
        ImmutableMap.of("consumer", "properties"),
        100L,
        true,
        DateTimes.nowUtc(),
        DateTimes.nowUtc()
    );
    final byte[] json = mapper.writeValueAsBytes(currentConfig);

    final ObjectMapper oldMapper = new DefaultObjectMapper();
    oldMapper.registerSubtypes(new NamedType(OldKafkaIndexTaskIoConfig.class, "kafka"));
    final OldKafkaIndexTaskIoConfig oldConfig = (OldKafkaIndexTaskIoConfig) oldMapper.readValue(json, IOConfig.class);

    Assert.assertEquals(currentConfig.getTaskGroupId().intValue(), oldConfig.taskGroupId);
    Assert.assertEquals(currentConfig.getBaseSequenceName(), oldConfig.baseSequenceName);
    Assert.assertEquals(currentConfig.getStartSequenceNumbers(), oldConfig.startPartitions.asStartPartitions(true));
    Assert.assertEquals(currentConfig.getEndSequenceNumbers(), oldConfig.getEndPartitions());
    Assert.assertEquals(currentConfig.getConsumerProperties(), oldConfig.getConsumerProperties());
    Assert.assertEquals(currentConfig.getPollTimeout(), oldConfig.getPollTimeout());
    Assert.assertEquals(currentConfig.isUseTransaction(), oldConfig.isUseTransaction());
    Assert.assertEquals(currentConfig.getMinimumMessageTime(), oldConfig.getMinimumMessageTime());
    Assert.assertEquals(currentConfig.getMaximumMessageTime(), oldConfig.getMaximumMessageTime());
  }

  @Test
  public void testDeserializeFromOldIoConfig() throws IOException
  {
    final ObjectMapper oldMapper = new DefaultObjectMapper();
    oldMapper.registerSubtypes(new NamedType(OldKafkaIndexTaskIoConfig.class, "kafka"));

    final OldKafkaIndexTaskIoConfig oldConfig = new OldKafkaIndexTaskIoConfig(
        0,
        "baseSequenceNamee",
        new SeekableStreamEndSequenceNumbers<>("stream", ImmutableMap.of(1, 10L, 2, 5L)),
        new SeekableStreamEndSequenceNumbers<>("stream", ImmutableMap.of(1, 20L, 2, 30L)),
        ImmutableMap.of("consumer", "properties"),
        100L,
        true,
        DateTimes.nowUtc(),
        DateTimes.nowUtc()
    );
    final byte[] json = oldMapper.writeValueAsBytes(oldConfig);

    final KafkaIndexTaskIOConfig currentConfig = (KafkaIndexTaskIOConfig) mapper.readValue(json, IOConfig.class);
    Assert.assertEquals(oldConfig.getTaskGroupId(), currentConfig.getTaskGroupId().intValue());
    Assert.assertEquals(oldConfig.getBaseSequenceName(), currentConfig.getBaseSequenceName());
    Assert.assertEquals(oldConfig.getStartPartitions().asStartPartitions(true), currentConfig.getStartSequenceNumbers());
    Assert.assertEquals(oldConfig.getEndPartitions(), currentConfig.getEndSequenceNumbers());
    Assert.assertEquals(oldConfig.getConsumerProperties(), currentConfig.getConsumerProperties());
    Assert.assertEquals(oldConfig.getPollTimeout(), currentConfig.getPollTimeout());
    Assert.assertEquals(oldConfig.isUseTransaction(), currentConfig.isUseTransaction());
    Assert.assertEquals(oldConfig.getMinimumMessageTime(), currentConfig.getMinimumMessageTime());
    Assert.assertEquals(oldConfig.getMaximumMessageTime(), currentConfig.getMaximumMessageTime());
  }

  private static class OldKafkaIndexTaskIoConfig implements IOConfig
  {
    private final int taskGroupId;
    private final String baseSequenceName;
    private final SeekableStreamEndSequenceNumbers<Integer, Long> startPartitions;
    private final SeekableStreamEndSequenceNumbers<Integer, Long> endPartitions;
    private final Map<String, Object> consumerProperties;
    private final long pollTimeout;
    private final boolean useTransaction;
    private final Optional<DateTime> minimumMessageTime;
    private final Optional<DateTime> maximumMessageTime;

    @JsonCreator
    private OldKafkaIndexTaskIoConfig(
        @JsonProperty("taskGroupId") int taskGroupId,
        @JsonProperty("baseSequenceName") String baseSequenceName,
        @JsonProperty("startPartitions") @Nullable SeekableStreamEndSequenceNumbers<Integer, Long> startPartitions,
        @JsonProperty("endPartitions") @Nullable SeekableStreamEndSequenceNumbers<Integer, Long> endPartitions,
        @JsonProperty("consumerProperties") Map<String, Object> consumerProperties,
        @JsonProperty("pollTimeout") Long pollTimeout,
        @JsonProperty("useTransaction") Boolean useTransaction,
        @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
        @JsonProperty("maximumMessageTime") DateTime maximumMessageTime
    )
    {
      this.taskGroupId = taskGroupId;
      this.baseSequenceName = baseSequenceName;
      this.startPartitions = startPartitions;
      this.endPartitions = endPartitions;
      this.consumerProperties = consumerProperties;
      this.pollTimeout = pollTimeout;
      this.useTransaction = useTransaction;
      this.minimumMessageTime = Optional.fromNullable(minimumMessageTime);
      this.maximumMessageTime = Optional.fromNullable(maximumMessageTime);
    }

    @JsonProperty
    public int getTaskGroupId()
    {
      return taskGroupId;
    }

    @JsonProperty
    public String getBaseSequenceName()
    {
      return baseSequenceName;
    }

    @JsonProperty
    public SeekableStreamEndSequenceNumbers<Integer, Long> getStartPartitions()
    {
      return startPartitions;
    }

    @JsonProperty
    public SeekableStreamEndSequenceNumbers<Integer, Long> getEndPartitions()
    {
      return endPartitions;
    }

    @JsonProperty
    public Map<String, Object> getConsumerProperties()
    {
      return consumerProperties;
    }

    @JsonProperty
    public long getPollTimeout()
    {
      return pollTimeout;
    }

    @JsonProperty
    public boolean isUseTransaction()
    {
      return useTransaction;
    }

    @JsonProperty
    public Optional<DateTime> getMinimumMessageTime()
    {
      return minimumMessageTime;
    }

    @JsonProperty
    public Optional<DateTime> getMaximumMessageTime()
    {
      return maximumMessageTime;
    }
  }
}

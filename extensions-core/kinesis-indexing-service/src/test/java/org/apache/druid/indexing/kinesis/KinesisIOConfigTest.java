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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.indexing.IOConfig;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class KinesisIOConfigTest
{
  private final ObjectMapper mapper;

  public KinesisIOConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules(new KinesisIndexingServiceModule().getJacksonModules());
  }

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"endpoint\": \"kinesis.us-east-1.amazonaws.com\",\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"1\", \"1\":\"10\"}},\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"15\", \"1\":\"200\"}}\n"
                     + "}";

    KinesisIndexTaskIOConfig config = (KinesisIndexTaskIOConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                IOConfig.class
            )
        ), IOConfig.class
    );

    Assertions.assertNull(config.getTaskGroupId());
    Assertions.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assertions.assertEquals("mystream", config.getStartSequenceNumbers().getStream());
    Assertions.assertEquals(
        ImmutableMap.of("0", "1", "1", "10"),
        config.getStartSequenceNumbers().getPartitionSequenceNumberMap()
    );
    Assertions.assertEquals("mystream", config.getEndSequenceNumbers().getStream());
    Assertions.assertEquals(
        ImmutableMap.of("0", "15", "1", "200"),
        config.getEndSequenceNumbers().getPartitionSequenceNumberMap()
    );
    Assertions.assertTrue(config.isUseTransaction());
    Assertions.assertNull(config.getMinimumMessageTime(), "minimumMessageTime");
    Assertions.assertEquals(config.getEndpoint(), "kinesis.us-east-1.amazonaws.com");
    Assertions.assertEquals(config.getFetchDelayMillis(), 0);
    Assertions.assertEquals(Collections.emptySet(), config.getStartSequenceNumbers().getExclusivePartitions());
    Assertions.assertNull(config.getAwsAssumedRoleArn());
    Assertions.assertNull(config.getAwsExternalId());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"1\", \"1\":\"10\"}, \"exclusivePartitions\" : [\"0\"] },\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"15\", \"1\":\"200\"}},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\",\n"
                     + "  \"endpoint\": \"kinesis.us-east-2.amazonaws.com\",\n"
                     + "  \"fetchDelayMillis\": 1000,\n"
                     + "  \"awsAssumedRoleArn\": \"role\",\n"
                     + "  \"awsExternalId\": \"awsexternalid\"\n"
                     + "}";

    KinesisIndexTaskIOConfig config = (KinesisIndexTaskIOConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                IOConfig.class
            )
        ), IOConfig.class
    );

    Assertions.assertEquals((Integer) 0, config.getTaskGroupId());
    Assertions.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assertions.assertEquals("mystream", config.getStartSequenceNumbers().getStream());
    Assertions.assertEquals(
        ImmutableMap.of("0", "1", "1", "10"),
        config.getStartSequenceNumbers().getPartitionSequenceNumberMap()
    );
    Assertions.assertEquals("mystream", config.getEndSequenceNumbers().getStream());
    Assertions.assertEquals(
        ImmutableMap.of("0", "15", "1", "200"),
        config.getEndSequenceNumbers().getPartitionSequenceNumberMap()
    );
    Assertions.assertFalse(config.isUseTransaction());
    Assertions.assertEquals(DateTimes.of("2016-05-31T12:00Z"), config.getMinimumMessageTime());
    Assertions.assertEquals(DateTimes.of("2016-05-31T14:00Z"), config.getMaximumMessageTime());
    Assertions.assertEquals(config.getEndpoint(), "kinesis.us-east-2.amazonaws.com");
    Assertions.assertEquals(config.getStartSequenceNumbers().getExclusivePartitions(), ImmutableSet.of("0"));
    Assertions.assertEquals(1000, config.getFetchDelayMillis());
    Assertions.assertEquals("role", config.getAwsAssumedRoleArn());
    Assertions.assertEquals("awsexternalid", config.getAwsExternalId());
  }

  @Test
  public void testBaseSequenceNameRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"endpoint\": \"kinesis.us-east-1.amazonaws.com\",\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"1\", \"1\":\"10\"}},\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"15\", \"1\":\"200\"}}\n"
                     + "}";

    JsonMappingException exception = Assertions.assertThrows(
        JsonMappingException.class,
        () -> mapper.readValue(jsonStr, IOConfig.class)
    );
    Assertions.assertInstanceOf(NullPointerException.class, exception.getCause());
    Assertions.assertTrue(exception.getMessage().contains("baseSequenceName"));
  }

  @Test
  public void teststartSequenceNumbersRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"endpoint\": \"kinesis.us-east-1.amazonaws.com\",\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"15\", \"1\":\"200\"}}\n"
                     + "}";

    JsonMappingException exception = Assertions.assertThrows(
        JsonMappingException.class,
        () -> mapper.readValue(jsonStr, IOConfig.class)
    );
    Assertions.assertInstanceOf(NullPointerException.class, exception.getCause());
    Assertions.assertTrue(exception.getMessage().contains("startSequenceNumbers"));
  }

  @Test
  public void testendSequenceNumbersRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"endpoint\": \"kinesis.us-east-1.amazonaws.com\",\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"1\", \"1\":\"10\"}}\n"
                     + "}";

    JsonMappingException exception = Assertions.assertThrows(
        JsonMappingException.class,
        () -> mapper.readValue(jsonStr, IOConfig.class)
    );
    Assertions.assertInstanceOf(NullPointerException.class, exception.getCause());
    Assertions.assertTrue(exception.getMessage().contains("endSequenceNumbers"));
  }

  @Test
  public void testStartAndEndstreamMatch() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"endpoint\": \"kinesis.us-east-1.amazonaws.com\",\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"1\", \"1\":\"10\"}},\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"notmystream\", \"partitionSequenceNumberMap\" : {\"0\":\"15\", \"1\":\"200\"}}\n"
                     + "}";

    JsonMappingException exception = Assertions.assertThrows(
        JsonMappingException.class,
        () -> mapper.readValue(jsonStr, IOConfig.class)
    );
    Assertions.assertInstanceOf(IllegalArgumentException.class, exception.getCause());
    Assertions.assertTrue(exception.getMessage().contains("must match"));
  }

  @Test
  public void testStartAndendSequenceNumbersetMatch() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"endpoint\": \"kinesis.us-east-1.amazonaws.com\",\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"1\", \"1\":\"10\"}},\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"15\", \"2\":\"200\"}}\n"
                     + "}";

    JsonMappingException exception = Assertions.assertThrows(
        JsonMappingException.class,
        () -> mapper.readValue(jsonStr, IOConfig.class)
    );
    Assertions.assertInstanceOf(IllegalArgumentException.class, exception.getCause());
    Assertions.assertTrue(
        exception.getMessage().contains("start partition set and end partition set must match")
    );
  }

  @Test
  public void testEndPointRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"1\", \"1\":\"10\"}},\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"15\", \"1\":\"200\"}}\n"
                     + "}";

    JsonMappingException exception = Assertions.assertThrows(
        JsonMappingException.class,
        () -> mapper.readValue(jsonStr, IOConfig.class)
    );
    Assertions.assertInstanceOf(NullPointerException.class, exception.getCause());
    Assertions.assertTrue(exception.getMessage().contains("endpoint"));
  }

  @Test
  public void testDeserializeToOldIoConfig() throws IOException
  {
    final KinesisIndexTaskIOConfig currentConfig = new KinesisIndexTaskIOConfig(
        0,
        "baseSequenceName",
        new SeekableStreamStartSequenceNumbers<>(
            "stream",
            ImmutableMap.of("1", "10L", "2", "5L"),
            ImmutableSet.of("1")
        ),
        new SeekableStreamEndSequenceNumbers<>("stream", ImmutableMap.of("1", "20L", "2", "30L")),
        true,
        DateTimes.nowUtc(),
        DateTimes.nowUtc(),
        null,
        "endpoint",
        2000,
        "awsAssumedRoleArn",
        "awsExternalId",
        Duration.standardHours(2).getStandardMinutes()
    );

    final byte[] json = mapper.writeValueAsBytes(currentConfig);
    final ObjectMapper oldMapper = new DefaultObjectMapper();
    oldMapper.registerSubtypes(new NamedType(OldKinesisIndexTaskIoConfig.class, "kinesis"));

    final OldKinesisIndexTaskIoConfig oldConfig = (OldKinesisIndexTaskIoConfig) oldMapper.readValue(
        json,
        IOConfig.class
    );

    Assertions.assertEquals(currentConfig.getBaseSequenceName(), oldConfig.getBaseSequenceName());
    Assertions.assertEquals(
        currentConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap(),
        oldConfig.getStartPartitions().getPartitionSequenceNumberMap()
    );
    Assertions.assertEquals(
        currentConfig.getStartSequenceNumbers().getExclusivePartitions(),
        oldConfig.getExclusiveStartSequenceNumberPartitions()
    );
    Assertions.assertEquals(currentConfig.getEndSequenceNumbers(), oldConfig.getEndPartitions());
    Assertions.assertEquals(currentConfig.isUseTransaction(), oldConfig.isUseTransaction());
    Assertions.assertEquals(currentConfig.getMinimumMessageTime(), oldConfig.getMinimumMessageTime());
    Assertions.assertEquals(currentConfig.getMaximumMessageTime(), oldConfig.getMaximumMessageTime());
    Assertions.assertEquals(currentConfig.getEndpoint(), oldConfig.getEndpoint());
    Assertions.assertEquals(currentConfig.getFetchDelayMillis(), oldConfig.getFetchDelayMillis());
    Assertions.assertEquals(currentConfig.getAwsAssumedRoleArn(), oldConfig.getAwsAssumedRoleArn());
    Assertions.assertEquals(currentConfig.getAwsExternalId(), oldConfig.getAwsExternalId());
  }

  @Test
  public void testDeserializeFromOldIoConfig() throws IOException
  {
    final ObjectMapper oldMapper = new DefaultObjectMapper();
    oldMapper.registerSubtypes(new NamedType(OldKinesisIndexTaskIoConfig.class, "kinesis"));

    final OldKinesisIndexTaskIoConfig oldConfig = new OldKinesisIndexTaskIoConfig(
        "baseSequenceName",
        new SeekableStreamEndSequenceNumbers<>("stream", ImmutableMap.of("1", "10L", "2", "5L")),
        new SeekableStreamEndSequenceNumbers<>("stream", ImmutableMap.of("1", "20L", "2", "30L")),
        ImmutableSet.of("1"),
        true,
        DateTimes.nowUtc(),
        DateTimes.nowUtc(),
        "endpoint",
        2000,
        "awsAssumedRoleArn",
        "awsExternalId"
    );

    final byte[] json = oldMapper.writeValueAsBytes(oldConfig);
    final KinesisIndexTaskIOConfig currentConfig = (KinesisIndexTaskIOConfig) mapper.readValue(json, IOConfig.class);

    Assertions.assertNull(currentConfig.getTaskGroupId());
    Assertions.assertEquals(oldConfig.getBaseSequenceName(), currentConfig.getBaseSequenceName());
    Assertions.assertEquals(
        oldConfig.getStartPartitions().getPartitionSequenceNumberMap(),
        currentConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap()
    );
    Assertions.assertEquals(
        oldConfig.getExclusiveStartSequenceNumberPartitions(),
        currentConfig.getStartSequenceNumbers().getExclusivePartitions()
    );
    Assertions.assertEquals(oldConfig.getEndPartitions(), currentConfig.getEndSequenceNumbers());
    Assertions.assertEquals(oldConfig.isUseTransaction(), currentConfig.isUseTransaction());
    Assertions.assertEquals(oldConfig.getMinimumMessageTime(), currentConfig.getMinimumMessageTime());
    Assertions.assertEquals(oldConfig.getMaximumMessageTime(), currentConfig.getMaximumMessageTime());
    Assertions.assertEquals(oldConfig.getEndpoint(), currentConfig.getEndpoint());
    Assertions.assertEquals(oldConfig.getFetchDelayMillis(), currentConfig.getFetchDelayMillis());
    Assertions.assertEquals(oldConfig.getAwsAssumedRoleArn(), currentConfig.getAwsAssumedRoleArn());
    Assertions.assertEquals(oldConfig.getAwsExternalId(), currentConfig.getAwsExternalId());
  }

  private static class OldKinesisIndexTaskIoConfig implements IOConfig
  {
    private final String baseSequenceName;
    private final SeekableStreamEndSequenceNumbers<String, String> startPartitions;
    private final SeekableStreamEndSequenceNumbers<String, String> endPartitions;
    private final Set<String> exclusiveStartSequenceNumberPartitions;
    private final boolean useTransaction;
    private final DateTime minimumMessageTime;
    private final DateTime maximumMessageTime;
    private final String endpoint;
    private final Integer fetchDelayMillis;
    private final String awsAssumedRoleArn;
    private final String awsExternalId;

    @JsonCreator
    private OldKinesisIndexTaskIoConfig(
        @JsonProperty("baseSequenceName") String baseSequenceName,
        @JsonProperty("startPartitions") @Nullable SeekableStreamEndSequenceNumbers<String, String> startPartitions,
        @JsonProperty("endPartitions") @Nullable SeekableStreamEndSequenceNumbers<String, String> endPartitions,
        @JsonProperty("exclusiveStartSequenceNumberPartitions") Set<String> exclusiveStartSequenceNumberPartitions,
        @JsonProperty("useTransaction") Boolean useTransaction,
        @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
        @JsonProperty("maximumMessageTime") DateTime maximumMessageTime,
        @JsonProperty("endpoint") String endpoint,
        @JsonProperty("fetchDelayMillis") Integer fetchDelayMillis,
        @JsonProperty("awsAssumedRoleArn") String awsAssumedRoleArn,
        @JsonProperty("awsExternalId") String awsExternalId
    )
    {
      this.baseSequenceName = baseSequenceName;
      this.startPartitions = startPartitions;
      this.endPartitions = endPartitions;
      this.exclusiveStartSequenceNumberPartitions = exclusiveStartSequenceNumberPartitions;
      this.useTransaction = useTransaction;
      this.minimumMessageTime = minimumMessageTime;
      this.maximumMessageTime = maximumMessageTime;
      this.endpoint = endpoint;
      this.fetchDelayMillis = fetchDelayMillis;
      this.awsAssumedRoleArn = awsAssumedRoleArn;
      this.awsExternalId = awsExternalId;
    }

    @JsonProperty
    public String getBaseSequenceName()
    {
      return baseSequenceName;
    }

    @JsonProperty
    public SeekableStreamEndSequenceNumbers<String, String> getStartPartitions()
    {
      return startPartitions;
    }

    @JsonProperty
    public SeekableStreamEndSequenceNumbers<String, String> getEndPartitions()
    {
      return endPartitions;
    }

    @JsonProperty
    public Set<String> getExclusiveStartSequenceNumberPartitions()
    {
      return exclusiveStartSequenceNumberPartitions;
    }

    @JsonProperty
    public boolean isUseTransaction()
    {
      return useTransaction;
    }

    @JsonProperty
    public DateTime getMinimumMessageTime()
    {
      return minimumMessageTime;
    }

    @JsonProperty
    public DateTime getMaximumMessageTime()
    {
      return maximumMessageTime;
    }

    @JsonProperty
    public String getEndpoint()
    {
      return endpoint;
    }

    @JsonProperty
    public int getFetchDelayMillis()
    {
      return fetchDelayMillis;
    }

    @JsonProperty
    public String getAwsAssumedRoleArn()
    {
      return awsAssumedRoleArn;
    }

    @JsonProperty
    public String getAwsExternalId()
    {
      return awsExternalId;
    }
  }
}

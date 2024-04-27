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
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import java.util.Set;

public class KinesisIOConfigTest
{
  private final ObjectMapper mapper;

  public KinesisIOConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules((Iterable<Module>) new KinesisIndexingServiceModule().getJacksonModules());
  }

  @Rule
  public final ExpectedException exception = ExpectedException.none();

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

    Assert.assertNull(config.getTaskGroupId());
    Assert.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assert.assertEquals("mystream", config.getStartSequenceNumbers().getStream());
    Assert.assertEquals(
        ImmutableMap.of("0", "1", "1", "10"),
        config.getStartSequenceNumbers().getPartitionSequenceNumberMap()
    );
    Assert.assertEquals("mystream", config.getEndSequenceNumbers().getStream());
    Assert.assertEquals(
        ImmutableMap.of("0", "15", "1", "200"),
        config.getEndSequenceNumbers().getPartitionSequenceNumberMap()
    );
    Assert.assertTrue(config.isUseTransaction());
    Assert.assertFalse("minimumMessageTime", config.getMinimumMessageTime().isPresent());
    Assert.assertEquals(config.getEndpoint(), "kinesis.us-east-1.amazonaws.com");
    Assert.assertEquals(config.getFetchDelayMillis(), 0);
    Assert.assertEquals(Collections.emptySet(), config.getStartSequenceNumbers().getExclusivePartitions());
    Assert.assertNull(config.getAwsAssumedRoleArn());
    Assert.assertNull(config.getAwsExternalId());
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

    Assert.assertEquals((Integer) 0, config.getTaskGroupId());
    Assert.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assert.assertEquals("mystream", config.getStartSequenceNumbers().getStream());
    Assert.assertEquals(
        ImmutableMap.of("0", "1", "1", "10"),
        config.getStartSequenceNumbers().getPartitionSequenceNumberMap()
    );
    Assert.assertEquals("mystream", config.getEndSequenceNumbers().getStream());
    Assert.assertEquals(
        ImmutableMap.of("0", "15", "1", "200"),
        config.getEndSequenceNumbers().getPartitionSequenceNumberMap()
    );
    Assert.assertFalse(config.isUseTransaction());
    Assert.assertTrue("maximumMessageTime", config.getMaximumMessageTime().isPresent());
    Assert.assertTrue("minimumMessageTime", config.getMinimumMessageTime().isPresent());
    Assert.assertEquals(DateTimes.of("2016-05-31T12:00Z"), config.getMinimumMessageTime().get());
    Assert.assertEquals(DateTimes.of("2016-05-31T14:00Z"), config.getMaximumMessageTime().get());
    Assert.assertEquals(config.getEndpoint(), "kinesis.us-east-2.amazonaws.com");
    Assert.assertEquals(config.getStartSequenceNumbers().getExclusivePartitions(), ImmutableSet.of("0"));
    Assert.assertEquals(1000, config.getFetchDelayMillis());
    Assert.assertEquals("role", config.getAwsAssumedRoleArn());
    Assert.assertEquals("awsexternalid", config.getAwsExternalId());
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

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("baseSequenceName"));
    mapper.readValue(jsonStr, IOConfig.class);
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

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("startSequenceNumbers"));
    mapper.readValue(jsonStr, IOConfig.class);
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

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("endSequenceNumbers"));
    mapper.readValue(jsonStr, IOConfig.class);
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

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));
    exception.expectMessage(CoreMatchers.containsString("must match"));
    mapper.readValue(jsonStr, IOConfig.class);
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

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));
    exception.expectMessage(CoreMatchers.containsString("start partition set and end partition set must match"));
    mapper.readValue(jsonStr, IOConfig.class);
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

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("endpoint"));
    mapper.readValue(jsonStr, IOConfig.class);
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
        "awsExternalId"
    );

    final byte[] json = mapper.writeValueAsBytes(currentConfig);
    final ObjectMapper oldMapper = new DefaultObjectMapper();
    oldMapper.registerSubtypes(new NamedType(OldKinesisIndexTaskIoConfig.class, "kinesis"));

    final OldKinesisIndexTaskIoConfig oldConfig = (OldKinesisIndexTaskIoConfig) oldMapper.readValue(
        json,
        IOConfig.class
    );

    Assert.assertEquals(currentConfig.getBaseSequenceName(), oldConfig.getBaseSequenceName());
    Assert.assertEquals(
        currentConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap(),
        oldConfig.getStartPartitions().getPartitionSequenceNumberMap()
    );
    Assert.assertEquals(
        currentConfig.getStartSequenceNumbers().getExclusivePartitions(),
        oldConfig.getExclusiveStartSequenceNumberPartitions()
    );
    Assert.assertEquals(currentConfig.getEndSequenceNumbers(), oldConfig.getEndPartitions());
    Assert.assertEquals(currentConfig.isUseTransaction(), oldConfig.isUseTransaction());
    Assert.assertEquals(currentConfig.getMinimumMessageTime(), oldConfig.getMinimumMessageTime());
    Assert.assertEquals(currentConfig.getMaximumMessageTime(), oldConfig.getMaximumMessageTime());
    Assert.assertEquals(currentConfig.getEndpoint(), oldConfig.getEndpoint());
    Assert.assertEquals(currentConfig.getFetchDelayMillis(), oldConfig.getFetchDelayMillis());
    Assert.assertEquals(currentConfig.getAwsAssumedRoleArn(), oldConfig.getAwsAssumedRoleArn());
    Assert.assertEquals(currentConfig.getAwsExternalId(), oldConfig.getAwsExternalId());
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

    Assert.assertNull(currentConfig.getTaskGroupId());
    Assert.assertEquals(oldConfig.getBaseSequenceName(), currentConfig.getBaseSequenceName());
    Assert.assertEquals(
        oldConfig.getStartPartitions().getPartitionSequenceNumberMap(),
        currentConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap()
    );
    Assert.assertEquals(
        oldConfig.getExclusiveStartSequenceNumberPartitions(),
        currentConfig.getStartSequenceNumbers().getExclusivePartitions()
    );
    Assert.assertEquals(oldConfig.getEndPartitions(), currentConfig.getEndSequenceNumbers());
    Assert.assertEquals(oldConfig.isUseTransaction(), currentConfig.isUseTransaction());
    Assert.assertEquals(oldConfig.getMinimumMessageTime(), currentConfig.getMinimumMessageTime());
    Assert.assertEquals(oldConfig.getMaximumMessageTime(), currentConfig.getMaximumMessageTime());
    Assert.assertEquals(oldConfig.getEndpoint(), currentConfig.getEndpoint());
    Assert.assertEquals(oldConfig.getFetchDelayMillis(), currentConfig.getFetchDelayMillis());
    Assert.assertEquals(oldConfig.getAwsAssumedRoleArn(), currentConfig.getAwsAssumedRoleArn());
    Assert.assertEquals(oldConfig.getAwsExternalId(), currentConfig.getAwsExternalId());
  }

  private static class OldKinesisIndexTaskIoConfig implements IOConfig
  {
    private final String baseSequenceName;
    private final SeekableStreamEndSequenceNumbers<String, String> startPartitions;
    private final SeekableStreamEndSequenceNumbers<String, String> endPartitions;
    private final Set<String> exclusiveStartSequenceNumberPartitions;
    private final boolean useTransaction;
    private final Optional<DateTime> minimumMessageTime;
    private final Optional<DateTime> maximumMessageTime;
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
      this.minimumMessageTime = Optional.fromNullable(minimumMessageTime);
      this.maximumMessageTime = Optional.fromNullable(maximumMessageTime);
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
    public Optional<DateTime> getMinimumMessageTime()
    {
      return minimumMessageTime;
    }

    @JsonProperty
    public Optional<DateTime> getMaximumMessageTime()
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

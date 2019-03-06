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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.indexing.IOConfig;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

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
                     + "  \"startPartitions\": {\"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"1\", \"1\":\"10\"}},\n"
                     + "  \"endPartitions\": {\"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"15\", \"1\":\"200\"}}\n"
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
    Assert.assertEquals("mystream", config.getStartPartitions().getStream());
    Assert.assertEquals(
        ImmutableMap.of("0", "1", "1", "10"),
        config.getStartPartitions().getPartitionSequenceNumberMap()
    );
    Assert.assertEquals("mystream", config.getEndPartitions().getStream());
    Assert.assertEquals(
        ImmutableMap.of("0", "15", "1", "200"),
        config.getEndPartitions().getPartitionSequenceNumberMap()
    );
    Assert.assertTrue(config.isUseTransaction());
    Assert.assertFalse("minimumMessageTime", config.getMinimumMessageTime().isPresent());
    Assert.assertEquals(config.getEndpoint(), "kinesis.us-east-1.amazonaws.com");
    Assert.assertEquals(config.getRecordsPerFetch(), 4000);
    Assert.assertEquals(config.getFetchDelayMillis(), 0);
    Assert.assertEquals(Collections.emptySet(), config.getExclusiveStartSequenceNumberPartitions());
    Assert.assertNull(config.getAwsAssumedRoleArn());
    Assert.assertNull(config.getAwsExternalId());
    Assert.assertFalse(config.isDeaggregate());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"1\", \"1\":\"10\"}},\n"
                     + "  \"endPartitions\": {\"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"15\", \"1\":\"200\"}},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\",\n"
                     + "  \"endpoint\": \"kinesis.us-east-2.amazonaws.com\",\n"
                     + "  \"recordsPerFetch\": 1000,\n"
                     + "  \"fetchDelayMillis\": 1000,\n"
                     + "  \"exclusiveStartSequenceNumberPartitions\": [\"0\"],\n"
                     + "  \"awsAssumedRoleArn\": \"role\",\n"
                     + "  \"awsExternalId\": \"awsexternalid\",\n"
                     + "  \"deaggregate\": true\n"
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
    Assert.assertEquals("mystream", config.getStartPartitions().getStream());
    Assert.assertEquals(
        ImmutableMap.of("0", "1", "1", "10"),
        config.getStartPartitions().getPartitionSequenceNumberMap()
    );
    Assert.assertEquals("mystream", config.getEndPartitions().getStream());
    Assert.assertEquals(
        ImmutableMap.of("0", "15", "1", "200"),
        config.getEndPartitions().getPartitionSequenceNumberMap()
    );
    Assert.assertFalse(config.isUseTransaction());
    Assert.assertTrue("maximumMessageTime", config.getMaximumMessageTime().isPresent());
    Assert.assertTrue("minimumMessageTime", config.getMinimumMessageTime().isPresent());
    Assert.assertEquals(DateTimes.of("2016-05-31T12:00Z"), config.getMinimumMessageTime().get());
    Assert.assertEquals(DateTimes.of("2016-05-31T14:00Z"), config.getMaximumMessageTime().get());
    Assert.assertEquals(config.getEndpoint(), "kinesis.us-east-2.amazonaws.com");
    Assert.assertEquals(config.getExclusiveStartSequenceNumberPartitions(), ImmutableSet.of("0"));
    Assert.assertEquals(1000, config.getRecordsPerFetch());
    Assert.assertEquals(1000, config.getFetchDelayMillis());
    Assert.assertEquals("role", config.getAwsAssumedRoleArn());
    Assert.assertEquals("awsexternalid", config.getAwsExternalId());
    Assert.assertTrue(config.isDeaggregate());
  }

  @Test
  public void testBaseSequenceNameRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"endpoint\": \"kinesis.us-east-1.amazonaws.com\",\n"
                     + "  \"startPartitions\": {\"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"1\", \"1\":\"10\"}},\n"
                     + "  \"endPartitions\": {\"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"15\", \"1\":\"200\"}}\n"
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
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"endpoint\": \"kinesis.us-east-1.amazonaws.com\",\n"
                     + "  \"endPartitions\": {\"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"15\", \"1\":\"200\"}}\n"
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
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"endpoint\": \"kinesis.us-east-1.amazonaws.com\",\n"
                     + "  \"startPartitions\": {\"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"1\", \"1\":\"10\"}}\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("endPartitions"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testStartAndEndstreamMatch() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"endpoint\": \"kinesis.us-east-1.amazonaws.com\",\n"
                     + "  \"startPartitions\": {\"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"1\", \"1\":\"10\"}},\n"
                     + "  \"endPartitions\": {\"stream\":\"notmystream\", \"partitionSequenceNumberMap\" : {\"0\":\"15\", \"1\":\"200\"}}\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));
    exception.expectMessage(CoreMatchers.containsString("must match"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testStartAndEndPartitionSetMatch() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"endpoint\": \"kinesis.us-east-1.amazonaws.com\",\n"
                     + "  \"startPartitions\": {\"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"1\", \"1\":\"10\"}},\n"
                     + "  \"endPartitions\": {\"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"15\", \"2\":\"200\"}}\n"
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
                     + "  \"startPartitions\": {\"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"1\", \"1\":\"10\"}},\n"
                     + "  \"endPartitions\": {\"stream\":\"mystream\", \"partitionSequenceNumberMap\" : {\"0\":\"15\", \"1\":\"200\"}}\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("endpoint"));
    mapper.readValue(jsonStr, IOConfig.class);
  }
}

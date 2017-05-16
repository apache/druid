/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.kafka;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.indexing.IOConfig;
import org.hamcrest.CoreMatchers;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"}\n"
                     + "}";

    KafkaIOConfig config = (KafkaIOConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                IOConfig.class
            )
        ), IOConfig.class
    );

    Assert.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assert.assertEquals("mytopic", config.getStartPartitions().getTopic());
    Assert.assertEquals(ImmutableMap.of(0, 1L, 1, 10L), config.getStartPartitions().getPartitionOffsetMap());
    Assert.assertEquals("mytopic", config.getEndPartitions().getTopic());
    Assert.assertEquals(ImmutableMap.of(0, 15L, 1, 200L), config.getEndPartitions().getPartitionOffsetMap());
    Assert.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assert.assertEquals(true, config.isUseTransaction());
    Assert.assertEquals(false, config.isPauseAfterRead());
    Assert.assertFalse("minimumMessageTime", config.getMinimumMessageTime().isPresent());
    Assert.assertFalse("skipOffsetGaps", config.isSkipOffsetGaps());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"pauseAfterRead\": true,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"skipOffsetGaps\": true\n"
                     + "}";

    KafkaIOConfig config = (KafkaIOConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                IOConfig.class
            )
        ), IOConfig.class
    );

    Assert.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assert.assertEquals("mytopic", config.getStartPartitions().getTopic());
    Assert.assertEquals(ImmutableMap.of(0, 1L, 1, 10L), config.getStartPartitions().getPartitionOffsetMap());
    Assert.assertEquals("mytopic", config.getEndPartitions().getTopic());
    Assert.assertEquals(ImmutableMap.of(0, 15L, 1, 200L), config.getEndPartitions().getPartitionOffsetMap());
    Assert.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assert.assertEquals(false, config.isUseTransaction());
    Assert.assertEquals(true, config.isPauseAfterRead());
    Assert.assertEquals(new DateTime("2016-05-31T12:00Z"), config.getMinimumMessageTime().get());
    Assert.assertTrue("skipOffsetGaps", config.isSkipOffsetGaps());
  }

  @Test
  public void testBaseSequenceNameRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"pauseAfterRead\": true,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\"\n"
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
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"pauseAfterRead\": true,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\"\n"
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
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"pauseAfterRead\": true,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("endPartitions"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testConsumerPropertiesRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"pauseAfterRead\": true,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\"\n"
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
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"other\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"pauseAfterRead\": true,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));
    exception.expectMessage(CoreMatchers.containsString("start topic and end topic must match"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testStartAndEndPartitionSetMatch() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"pauseAfterRead\": true,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\"\n"
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
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":2}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"pauseAfterRead\": true,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));
    exception.expectMessage(CoreMatchers.containsString("end offset must be >= start offset"));
    mapper.readValue(jsonStr, IOConfig.class);
  }
}

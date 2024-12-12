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

package org.apache.druid.indexing.rabbitstream.supervisor;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.rabbitstream.RabbitStreamIndexTaskModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.hamcrest.CoreMatchers;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class RabbitStreamSupervisorIOConfigTest
{
  private final ObjectMapper mapper;

  public RabbitStreamSupervisorIOConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules((Iterable<Module>) new RabbitStreamIndexTaskModule().getJacksonModules());
  }

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\n"
        + "  \"type\": \"rabbit\",\n"
        + "  \"stream\": \"my-stream\",\n"
        + "  \"uri\": \"rabbitmq-stream://localhost:5552\"\n"
        + "}";

    RabbitStreamSupervisorIOConfig config = mapper.readValue(
        jsonStr,
        RabbitStreamSupervisorIOConfig.class);

    Assert.assertEquals("my-stream", config.getStream());
    Assert.assertEquals(config.getUri(), "rabbitmq-stream://localhost:5552");
    Assert.assertEquals(1, (int) config.getReplicas());
    Assert.assertEquals(1, (int) config.getTaskCount());
    Assert.assertEquals(Duration.standardMinutes(60), config.getTaskDuration());
    Assert.assertEquals(Duration.standardSeconds(5), config.getStartDelay());
    Assert.assertEquals(Duration.standardSeconds(30), config.getPeriod());
    Assert.assertFalse(config.isUseEarliestSequenceNumber());
    Assert.assertEquals(Duration.standardMinutes(30), config.getCompletionTimeout());
    Assert.assertFalse("lateMessageRejectionPeriod", config.getLateMessageRejectionPeriod().isPresent());
    Assert.assertFalse("earlyMessageRejectionPeriod", config.getEarlyMessageRejectionPeriod().isPresent());
    Assert.assertFalse("lateMessageRejectionStartDateTime", config.getLateMessageRejectionStartDateTime().isPresent());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
        + "  \"type\": \"rabbit\",\n"
        + "  \"stream\": \"my-stream\",\n"
        + "  \"uri\": \"rabbitmq-stream://localhost:5552\",\n"
        + "  \"replicas\": 3,\n"
        + "  \"taskCount\": 9,\n"
        + "  \"taskDuration\": \"PT30M\",\n"
        + "  \"startDelay\": \"PT1M\",\n"
        + "  \"period\": \"PT10S\",\n"
        + "  \"useEarliestOffset\": true,\n"
        + "  \"completionTimeout\": \"PT45M\",\n"
        + "  \"lateMessageRejectionPeriod\": \"PT1H\",\n"
        + "  \"earlyMessageRejectionPeriod\": \"PT1H\",\n"
        + "  \"recordsPerFetch\": 4000\n"
        + "}";

    RabbitStreamSupervisorIOConfig config = mapper.readValue(
        jsonStr,
        RabbitStreamSupervisorIOConfig.class);

    Assert.assertEquals("my-stream", config.getStream());
    Assert.assertEquals(config.getUri(), "rabbitmq-stream://localhost:5552");
    Assert.assertEquals(3, (int) config.getReplicas());
    Assert.assertEquals(9, (int) config.getTaskCount());
    Assert.assertEquals(Duration.standardMinutes(30), config.getTaskDuration());
    Assert.assertEquals(Duration.standardMinutes(1), config.getStartDelay());
    Assert.assertEquals(Duration.standardSeconds(10), config.getPeriod());
    Assert.assertTrue(config.isUseEarliestSequenceNumber());
    Assert.assertEquals(Duration.standardMinutes(45), config.getCompletionTimeout());
    Assert.assertEquals(Duration.standardHours(1), config.getLateMessageRejectionPeriod().get());
    Assert.assertEquals(Duration.standardHours(1), config.getEarlyMessageRejectionPeriod().get());
    // Assert.assertEquals((Integer) 4000, config.getRecordsPerFetch());
    // Assert.assertEquals(1000, config.getFetchDelayMillis());
  }

  @Test
  public void testStreamRequired() throws Exception
  {
    String jsonStr = "{\n"
        + "  \"type\": \"rabbit\"\n"
        + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("stream"));
    mapper.readValue(jsonStr, RabbitStreamSupervisorIOConfig.class);
  }

  @Test
  public void testURIRequired() throws Exception
  {
    String jsonStr = "{\n"
        + "  \"type\": \"rabbit\",\n"
        + "  \"stream\": \"my-stream\"\n"
        + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("uri"));
    mapper.readValue(jsonStr, RabbitStreamSupervisorIOConfig.class);
  }

}

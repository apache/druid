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

package org.apache.druid.indexing.kinesis.supervisor;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexing.kinesis.KinesisIndexingServiceModule;
import org.apache.druid.indexing.kinesis.KinesisRegion;
import org.apache.druid.indexing.seekablestream.supervisor.LagAggregator;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.easymock.EasyMock.createMock;

public class KinesisSupervisorIOConfigTest
{
  private final ObjectMapper mapper;

  public KinesisSupervisorIOConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules(new KinesisIndexingServiceModule().getJacksonModules());
  }

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"stream\": \"my-stream\"\n"
                     + "}";

    KinesisSupervisorIOConfig config = mapper.readValue(
        jsonStr,
        KinesisSupervisorIOConfig.class
    );

    Assertions.assertEquals("my-stream", config.getStream());
    Assertions.assertEquals(KinesisRegion.US_EAST_1.getEndpoint(), config.getEndpoint());
    Assertions.assertEquals(1, (int) config.getReplicas());
    Assertions.assertEquals(1, (int) config.getTaskCount());
    Assertions.assertNull(config.getStopTaskCount());
    Assertions.assertEquals((int) config.getTaskCount(), config.getMaxAllowedStops());
    Assertions.assertEquals(Duration.standardMinutes(60), config.getTaskDuration());
    Assertions.assertEquals(Duration.standardSeconds(5), config.getStartDelay());
    Assertions.assertEquals(Duration.standardSeconds(30), config.getPeriod());
    Assertions.assertFalse(config.isUseEarliestSequenceNumber());
    Assertions.assertEquals(Duration.standardMinutes(30), config.getCompletionTimeout());
    Assertions.assertFalse(config.getLateMessageRejectionPeriod().isPresent(), "lateMessageRejectionPeriod");
    Assertions.assertFalse(config.getEarlyMessageRejectionPeriod().isPresent(), "earlyMessageRejectionPeriod");
    Assertions.assertFalse(config.getLateMessageRejectionStartDateTime().isPresent(), "lateMessageRejectionStartDateTime");
    Assertions.assertEquals(0, config.getFetchDelayMillis());
    Assertions.assertNull(config.getAwsAssumedRoleArn());
    Assertions.assertNull(config.getAwsExternalId());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"stream\": \"my-stream\",\n"
                     + "  \"region\": \"us-east-2\",\n"
                     + "  \"endpoint\": \"kinesis.us-east-2.amazonaws.com\",\n"
                     + "  \"replicas\": 3,\n"
                     + "  \"taskCount\": 9,\n"
                     + "  \"taskDuration\": \"PT30M\",\n"
                     + "  \"startDelay\": \"PT1M\",\n"
                     + "  \"period\": \"PT10S\",\n"
                     + "  \"useEarliestSequenceNumber\": true,\n"
                     + "  \"completionTimeout\": \"PT45M\",\n"
                     + "  \"lateMessageRejectionPeriod\": \"PT1H\",\n"
                     + "  \"earlyMessageRejectionPeriod\": \"PT1H\",\n"
                     + "  \"fetchDelayMillis\": 1000,\n"
                     + "  \"awsAssumedRoleArn\": \"role\",\n"
                     + "  \"awsExternalId\": \"awsexternalid\"\n"
                     + "}";

    KinesisSupervisorIOConfig config = mapper.readValue(
        jsonStr,
        KinesisSupervisorIOConfig.class
    );

    Assertions.assertEquals("my-stream", config.getStream());
    Assertions.assertEquals(config.getEndpoint(), "kinesis.us-east-2.amazonaws.com");
    Assertions.assertEquals(3, (int) config.getReplicas());
    Assertions.assertEquals(9, (int) config.getTaskCount());
    Assertions.assertEquals(Duration.standardMinutes(30), config.getTaskDuration());
    Assertions.assertEquals(Duration.standardMinutes(1), config.getStartDelay());
    Assertions.assertEquals(Duration.standardSeconds(10), config.getPeriod());
    Assertions.assertTrue(config.isUseEarliestSequenceNumber());
    Assertions.assertEquals(Duration.standardMinutes(45), config.getCompletionTimeout());
    Assertions.assertEquals(Duration.standardHours(1), config.getLateMessageRejectionPeriod().get());
    Assertions.assertEquals(Duration.standardHours(1), config.getEarlyMessageRejectionPeriod().get());
    Assertions.assertEquals(1000, config.getFetchDelayMillis());
    Assertions.assertEquals("role", config.getAwsAssumedRoleArn());
    Assertions.assertEquals("awsexternalid", config.getAwsExternalId());
  }

  @Test
  public void testTopicRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\"\n"
                     + "}";

    JsonMappingException exception = Assertions.assertThrows(
        JsonMappingException.class,
        () -> mapper.readValue(jsonStr, KinesisSupervisorIOConfig.class)
    );
    Assertions.assertInstanceOf(NullPointerException.class, exception.getCause());
    Assertions.assertTrue(exception.getMessage().contains("stream"));
  }

  @Test
  public void testBoundedModeSerdeWithStringOffsets() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"stream\": \"my-stream\",\n"
                     + "  \"boundedStreamConfig\": {\n"
                     + "    \"startSequenceNumbers\": {\"shardId-000000000000\": \"49590338271490256608559692538361571095921575989136588898\", \"shardId-000000000001\": \"49590338271512257353759162668991891722121171891717232706\"},\n"
                     + "    \"endSequenceNumbers\": {\"shardId-000000000000\": \"49590338271534258098958632799622211348320767794297876514\", \"shardId-000000000001\": \"49590338271556258844158102930252531974520363696878520322\"}\n"
                     + "  }\n"
                     + "}";

    KinesisSupervisorIOConfig config = mapper.readValue(jsonStr, KinesisSupervisorIOConfig.class);

    Assertions.assertTrue(config.isBounded());
    Assertions.assertNotNull(config.getBoundedStreamConfig());
    Assertions.assertEquals(2, config.getBoundedStreamConfig().getStartSequenceNumbers().size());
    Assertions.assertEquals(2, config.getBoundedStreamConfig().getEndSequenceNumbers().size());
  }

  @Test
  public void testBoundedModeSerdeWithNumericOffsets() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"stream\": \"my-stream\",\n"
                     + "  \"boundedStreamConfig\": {\n"
                     + "    \"startSequenceNumbers\": {\"shardId-000000000000\": 100, \"shardId-000000000001\": 200},\n"
                     + "    \"endSequenceNumbers\": {\"shardId-000000000000\": 500, \"shardId-000000000001\": 600}\n"
                     + "  }\n"
                     + "}";

    KinesisSupervisorIOConfig config = mapper.readValue(jsonStr, KinesisSupervisorIOConfig.class);

    Assertions.assertTrue(config.isBounded());
    Assertions.assertNotNull(config.getBoundedStreamConfig());
    Assertions.assertEquals(2, config.getBoundedStreamConfig().getStartSequenceNumbers().size());
    Assertions.assertEquals(2, config.getBoundedStreamConfig().getEndSequenceNumbers().size());
  }

  @Test
  public void testBoundedModeSerdeWithMixedOffsets() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"stream\": \"my-stream\",\n"
                     + "  \"boundedStreamConfig\": {\n"
                     + "    \"startSequenceNumbers\": {\"shardId-000000000000\": \"49590338271490256608559692538361571095921575989136588898\", \"shardId-000000000001\": 200},\n"
                     + "    \"endSequenceNumbers\": {\"shardId-000000000000\": 500, \"shardId-000000000001\": \"49590338271556258844158102930252531974520363696878520322\"}\n"
                     + "  }\n"
                     + "}";

    KinesisSupervisorIOConfig config = mapper.readValue(jsonStr, KinesisSupervisorIOConfig.class);

    Assertions.assertTrue(config.isBounded());
    Assertions.assertNotNull(config.getBoundedStreamConfig());
  }

  @Test
  public void testUnboundedModeByDefault() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"stream\": \"my-stream\"\n"
                     + "}";

    KinesisSupervisorIOConfig config = mapper.readValue(jsonStr, KinesisSupervisorIOConfig.class);

    Assertions.assertFalse(config.isBounded());
    Assertions.assertNull(config.getBoundedStreamConfig());
  }

  private static KinesisIOConfigBuilder ioConfigBuilder()
  {
    return new KinesisIOConfigBuilder()
        .withStream("stream")
        .withEndpoint("awsEndpoint")
        .withReplicas(1)
        .withTaskCount(2)
        .withTaskDuration(new Period("PT1H"));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    final KinesisSupervisorIOConfig config = ioConfigBuilder().build();
    Assertions.assertEquals(config, ioConfigBuilder().build());
    Assertions.assertEquals(config.hashCode(), ioConfigBuilder().build().hashCode());
    Assertions.assertNotEquals(config, null);
    Assertions.assertNotEquals(config, "not an io config");
    Assertions.assertNotEquals(config, ioConfigBuilder().withEndpoint("other").build());
    Assertions.assertNotEquals(config, ioConfigBuilder().withReplicas(9).build());
    Assertions.assertNotEquals(config, ioConfigBuilder().withTaskCount(9).build());
    Assertions.assertNotEquals(config, ioConfigBuilder().withFetchDelayMillis(999).build());
    Assertions.assertNotEquals(config, ioConfigBuilder().withDeaggregate(true).build());
  }

  @Test
  public void testTuningConfigEqualsAndHashCode()
  {
    final KinesisSupervisorTuningConfig config = KinesisSupervisorTuningConfig.defaultConfig();
    Assertions.assertEquals(config, KinesisSupervisorTuningConfig.defaultConfig());
    Assertions.assertEquals(config.hashCode(), KinesisSupervisorTuningConfig.defaultConfig().hashCode());
    Assertions.assertNotEquals(config, null);
    Assertions.assertNotEquals(config, "not a tuning config");
  }

  /**
   * Drift guard for this class's own fields (base fields are covered by SeekableStreamSupervisorIOConfigTest):
   * a field omitted from {@code equals} would let a changed spec persist without restarting the supervisor.
   */
  @Test
  public void testEqualsContractCoversAllFields()
  {
    EqualsVerifier.forClass(KinesisSupervisorIOConfig.class)
                  .usingGetClass()
                  .withRedefinedSuperclass()
                  .withIgnoredFields("taskCountExplicit", "autoScalerEnabled")
                  .suppress(Warning.NONFINAL_FIELDS)
                  .withPrefabValues(Optional.class, Optional.of("a"), Optional.of("b"))
                  .withPrefabValues(InputFormat.class, createMock(InputFormat.class), createMock(InputFormat.class))
                  .withPrefabValues(AutoScalerConfig.class, createMock(AutoScalerConfig.class), createMock(AutoScalerConfig.class))
                  .withPrefabValues(LagAggregator.class, createMock(LagAggregator.class), createMock(LagAggregator.class))
                  .verify();
  }

  @Test
  public void testTuningConfigEqualsContractCoversAllFields()
  {
    EqualsVerifier.forClass(KinesisSupervisorTuningConfig.class)
                  .usingGetClass()
                  .withRedefinedSuperclass()
                  .suppress(Warning.NONFINAL_FIELDS)
                  .verify();
  }

}

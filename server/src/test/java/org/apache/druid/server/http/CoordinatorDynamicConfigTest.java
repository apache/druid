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

package org.apache.druid.server.http;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.utils.JvmUtils;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Set;

/**
 *
 */
public class CoordinatorDynamicConfigTest
{
  private static final int EXPECTED_DEFAULT_MAX_SEGMENTS_IN_NODE_LOADING_QUEUE = 500;

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
                     + "  \"mergeBytesLimit\": 1,\n"
                     + "  \"mergeSegmentsLimit\" : 1,\n"
                     + "  \"maxSegmentsToMove\": 1,\n"
                     + "  \"replicantLifetime\": 1,\n"
                     + "  \"replicationThrottleLimit\": 1,\n"
                     + "  \"balancerComputeThreads\": 2, \n"
                     + "  \"killDataSourceWhitelist\": [\"test1\",\"test2\"],\n"
                     + "  \"killTaskSlotRatio\": 0.15,\n"
                     + "  \"maxKillTaskSlots\": 2,\n"
                     + "  \"maxSegmentsInNodeLoadingQueue\": 1,\n"
                     + "  \"decommissioningNodes\": [\"host1\", \"host2\"],\n"
                     + "  \"pauseCoordination\": false,\n"
                     + "  \"replicateAfterLoadTimeout\": false\n"
                     + "}\n";

    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    ImmutableSet<String> decommissioning = ImmutableSet.of("host1", "host2");
    ImmutableSet<String> whitelist = ImmutableSet.of("test1", "test2");
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.15,
        2,
        false,
        1,
        decommissioning,
        false,
        false
    );

    actual = CoordinatorDynamicConfig.builder().withDecommissioningNodes(ImmutableSet.of("host1")).build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.15,
        2,
        false,
        1,
        ImmutableSet.of("host1"),
        false,
        false
    );

    actual = CoordinatorDynamicConfig.builder().build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.15,
        2,
        false,
        1,
        ImmutableSet.of("host1"),
        false,
        false
    );

    actual = CoordinatorDynamicConfig.builder().withPauseCoordination(true).build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.15,
        2,
        false,
        1,
        ImmutableSet.of("host1"),
        true,
        false
    );

    actual = CoordinatorDynamicConfig.builder().withReplicateAfterLoadTimeout(true).build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.15,
        2,
        false,
        1,
        ImmutableSet.of("host1"),
        true,
        true
    );

    actual = CoordinatorDynamicConfig.builder().build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.15,
        2,
        false,
        1,
        ImmutableSet.of("host1"),
        true,
        true
    );

    actual = CoordinatorDynamicConfig.builder().withKillTaskSlotRatio(1.0).build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        1.0,
        2,
        false,
        1,
        ImmutableSet.of("host1"),
        true,
        true
    );

    actual = CoordinatorDynamicConfig.builder().withMaxKillTaskSlots(5).build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        1.0,
        5,
        false,
        1,
        ImmutableSet.of("host1"),
        true,
        true
    );
  }

  @Test
  public void testDeserializationWithUnknownProperties() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"unknownProperty\": 2, \n"
                     + "  \"maxSegmentsInNodeLoadingQueue\": 15\n"
                     + "}\n";

    CoordinatorDynamicConfig dynamicConfig
        = mapper.readValue(jsonStr, CoordinatorDynamicConfig.class);
    Assert.assertEquals(15, dynamicConfig.getMaxSegmentsInNodeLoadingQueue());
  }

  @Test
  public void testConstructorWithNullsShouldKillUnusedSegmentsInAllDataSources()
  {
    CoordinatorDynamicConfig config = new CoordinatorDynamicConfig(
        1,
        1,
        1,
        1,
        1,
        2,
        10,
        null,
        null,
        null,
        null,
        null,
        ImmutableSet.of("host1"),
        true,
        true,
        false,
        false,
        null
    );
    Assert.assertTrue(config.isKillUnusedSegmentsInAllDataSources());
    Assert.assertTrue(config.getSpecificDataSourcesToKillUnusedSegmentsIn().isEmpty());
  }

  @Test
  public void testConstructorWithSpecificDataSourcesToKillShouldNotKillUnusedSegmentsInAllDatasources()
  {
    CoordinatorDynamicConfig config = new CoordinatorDynamicConfig(
        1,
        1,
        1,
        1,
        1,
        2,
        10,
        ImmutableSet.of("test1"),
        null,
        null,
        null,
        null,
        ImmutableSet.of("host1"),
        true,
        true,
        false,
        false,
        null
    );
    Assert.assertFalse(config.isKillUnusedSegmentsInAllDataSources());
    Assert.assertEquals(ImmutableSet.of("test1"), config.getSpecificDataSourcesToKillUnusedSegmentsIn());
  }

  @Test
  public void testDecommissioningParametersBackwardCompatibility() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
                     + "  \"mergeBytesLimit\": 1,\n"
                     + "  \"mergeSegmentsLimit\" : 1,\n"
                     + "  \"maxSegmentsToMove\": 1,\n"
                     + "  \"replicantLifetime\": 1,\n"
                     + "  \"replicationThrottleLimit\": 1,\n"
                     + "  \"balancerComputeThreads\": 2, \n"
                     + "  \"killDataSourceWhitelist\": [\"test1\",\"test2\"],\n"
                     + "  \"maxSegmentsInNodeLoadingQueue\": 1\n"
                     + "}\n";

    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    ImmutableSet<String> decommissioning = ImmutableSet.of();
    ImmutableSet<String> whitelist = ImmutableSet.of("test1", "test2");
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        1.0,
        Integer.MAX_VALUE,
        false,
        1,
        decommissioning,
        false,
        false
    );

    actual = CoordinatorDynamicConfig.builder().withDecommissioningNodes(ImmutableSet.of("host1")).build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        1.0,
        Integer.MAX_VALUE,
        false,
        1,
        ImmutableSet.of("host1"),
        false,
        false
    );

    actual = CoordinatorDynamicConfig.builder().build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        1.0,
        Integer.MAX_VALUE,
        false,
        1,
        ImmutableSet.of("host1"),
        false,
        false
    );
  }

  @Test
  public void testSerdeWithStringInKillDataSourceWhitelist() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
                     + "  \"mergeBytesLimit\": 1,\n"
                     + "  \"mergeSegmentsLimit\" : 1,\n"
                     + "  \"maxSegmentsToMove\": 1,\n"
                     + "  \"replicantLifetime\": 1,\n"
                     + "  \"replicationThrottleLimit\": 1,\n"
                     + "  \"balancerComputeThreads\": 2, \n"
                     + "  \"killDataSourceWhitelist\": \"test1, test2\", \n"
                     + "  \"maxSegmentsInNodeLoadingQueue\": 1\n"
                     + "}\n";

    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        ImmutableSet.of("test1", "test2"),
        1.0,
        Integer.MAX_VALUE,
        false,
        1,
        ImmutableSet.of(),
        false,
        false
    );
  }

  @Test
  public void testHandleMissingPercentOfSegmentsToConsiderPerMove() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
                     + "  \"mergeBytesLimit\": 1,\n"
                     + "  \"mergeSegmentsLimit\" : 1,\n"
                     + "  \"maxSegmentsToMove\": 1,\n"
                     + "  \"replicantLifetime\": 1,\n"
                     + "  \"replicationThrottleLimit\": 1,\n"
                     + "  \"balancerComputeThreads\": 2, \n"
                     + "  \"killDataSourceWhitelist\": [\"test1\",\"test2\"],\n"
                     + "  \"maxSegmentsInNodeLoadingQueue\": 1,\n"
                     + "  \"decommissioningNodes\": [\"host1\", \"host2\"],\n"
                     + "  \"pauseCoordination\": false\n"
                     + "}\n";
    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, CoordinatorDynamicConfig.class)
        ),
        CoordinatorDynamicConfig.class
    );
    ImmutableSet<String> decommissioning = ImmutableSet.of("host1", "host2");
    ImmutableSet<String> whitelist = ImmutableSet.of("test1", "test2");
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        1.0,
        Integer.MAX_VALUE,
        false,
        1,
        decommissioning,
        false,
        false
    );
  }

  @Test
  public void testSerdeWithKillAllDataSources() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
                     + "  \"mergeBytesLimit\": 1,\n"
                     + "  \"mergeSegmentsLimit\" : 1,\n"
                     + "  \"maxSegmentsToMove\": 1,\n"
                     + "  \"replicantLifetime\": 1,\n"
                     + "  \"replicationThrottleLimit\": 1,\n"
                     + "  \"balancerComputeThreads\": 2, \n"
                     + "  \"killAllDataSources\": true,\n"
                     + "  \"maxSegmentsInNodeLoadingQueue\": 1\n"
                     + "}\n";

    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, CoordinatorDynamicConfig.class)
        ),
        CoordinatorDynamicConfig.class
    );

    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        ImmutableSet.of(),
        1.0,
        Integer.MAX_VALUE,
        true,
        1,
        ImmutableSet.of(),
        false,
        false
    );

    // killAllDataSources is a config in versions 0.22.x and older and is no longer used.
    // This used to be an invalid config, but as of 0.23.0 the killAllDataSources flag no longer exsist,
    // so this is a valid config
    jsonStr = "{\n"
              + "  \"killDataSourceWhitelist\": [\"test1\",\"test2\"],\n"
              + "  \"killAllDataSources\": true\n"
              + "}\n";
    actual = mapper.readValue(jsonStr, CoordinatorDynamicConfig.class);

    Assert.assertFalse(actual.isKillUnusedSegmentsInAllDataSources());
    Assert.assertEquals(2, actual.getSpecificDataSourcesToKillUnusedSegmentsIn().size());
  }

  @Test
  public void testDeserializeWithoutMaxSegmentsInNodeLoadingQueue() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
                     + "  \"mergeBytesLimit\": 1,\n"
                     + "  \"mergeSegmentsLimit\" : 1,\n"
                     + "  \"maxSegmentsToMove\": 1,\n"
                     + "  \"replicantLifetime\": 1,\n"
                     + "  \"replicationThrottleLimit\": 1,\n"
                     + "  \"balancerComputeThreads\": 2, \n"
                     + "  \"killAllDataSources\": true\n"
                     + "}\n";

    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, CoordinatorDynamicConfig.class)
        ),
        CoordinatorDynamicConfig.class
    );

    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        ImmutableSet.of(),
        1.0,
        Integer.MAX_VALUE,
        true,
        EXPECTED_DEFAULT_MAX_SEGMENTS_IN_NODE_LOADING_QUEUE,
        ImmutableSet.of(),
        false,
        false
    );
  }

  @Test
  public void testBuilderDefaults()
  {
    CoordinatorDynamicConfig defaultConfig = CoordinatorDynamicConfig.builder().build();
    ImmutableSet<String> emptyList = ImmutableSet.of();
    assertConfig(
        defaultConfig,
        900000,
        524288000,
        100,
        100,
        15,
        500,
        getDefaultNumBalancerThreads(),
        emptyList,
        1.0,
        Integer.MAX_VALUE,
        true,
        EXPECTED_DEFAULT_MAX_SEGMENTS_IN_NODE_LOADING_QUEUE,
        emptyList,
        false,
        false
    );
  }

  @Test
  public void testBuilderWithDefaultSpecificDataSourcesToKillUnusedSegmentsInSpecified()
  {
    CoordinatorDynamicConfig defaultConfig =
        CoordinatorDynamicConfig.builder()
                                .withSpecificDataSourcesToKillUnusedSegmentsIn(ImmutableSet.of("DATASOURCE"))
                                .build();
    CoordinatorDynamicConfig config = CoordinatorDynamicConfig.builder().build(defaultConfig);
    assertConfig(
        config,
        900000,
        524288000,
        100,
        100,
        15,
        500,
        getDefaultNumBalancerThreads(),
        ImmutableSet.of("DATASOURCE"),
        1.0,
        Integer.MAX_VALUE,
        false,
        EXPECTED_DEFAULT_MAX_SEGMENTS_IN_NODE_LOADING_QUEUE,
        ImmutableSet.of(),
        false,
        false
    );
  }

  @Test
  public void testUpdate()
  {
    CoordinatorDynamicConfig current = CoordinatorDynamicConfig
        .builder()
        .withSpecificDataSourcesToKillUnusedSegmentsIn(ImmutableSet.of("x"))
        .build();

    Assert.assertEquals(
        current,
        new CoordinatorDynamicConfig.Builder(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ).build(current)
    );
  }

  private void assertThatDeserializationFailsWithMessage(String json, String message)
  {
    JsonMappingException e = Assert.assertThrows(
        JsonMappingException.class,
        () -> mapper.readValue(
            mapper.writeValueAsString(
                mapper.readValue(json, CoordinatorDynamicConfig.class)
            ),
            CoordinatorDynamicConfig.class
        )
    );
    Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
    IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
    Assert.assertEquals(message, cause.getMessage());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    CoordinatorDynamicConfig config1 = CoordinatorDynamicConfig.builder().build();
    CoordinatorDynamicConfig config2 = CoordinatorDynamicConfig.builder().build();
    Assert.assertEquals(config1, config2);
    Assert.assertEquals(config1.hashCode(), config2.hashCode());
  }

  private void assertConfig(
      CoordinatorDynamicConfig config,
      long expectedLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments,
      long expectedMergeBytesLimit,
      int expectedMergeSegmentsLimit,
      int expectedMaxSegmentsToMove,
      int expectedReplicantLifetime,
      int expectedReplicationThrottleLimit,
      int expectedBalancerComputeThreads,
      Set<String> expectedSpecificDataSourcesToKillUnusedSegmentsIn,
      Double expectedKillTaskSlotRatio,
      @Nullable Integer expectedMaxKillTaskSlots,
      boolean expectedKillUnusedSegmentsInAllDataSources,
      int expectedMaxSegmentsInNodeLoadingQueue,
      Set<String> decommissioningNodes,
      boolean pauseCoordination,
      boolean replicateAfterLoadTimeout
  )
  {
    Assert.assertEquals(
        expectedLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments,
        config.getMarkSegmentAsUnusedDelayMillis()
    );
    Assert.assertEquals(expectedMergeBytesLimit, config.getMergeBytesLimit());
    Assert.assertEquals(expectedMergeSegmentsLimit, config.getMergeSegmentsLimit());
    Assert.assertEquals(expectedMaxSegmentsToMove, config.getMaxSegmentsToMove());
    Assert.assertEquals(expectedReplicantLifetime, config.getReplicantLifetime());
    Assert.assertEquals(expectedReplicationThrottleLimit, config.getReplicationThrottleLimit());
    Assert.assertEquals(expectedBalancerComputeThreads, config.getBalancerComputeThreads());
    Assert.assertEquals(
        expectedSpecificDataSourcesToKillUnusedSegmentsIn,
        config.getSpecificDataSourcesToKillUnusedSegmentsIn()
    );
    Assert.assertEquals(expectedKillUnusedSegmentsInAllDataSources, config.isKillUnusedSegmentsInAllDataSources());
    Assert.assertEquals(expectedKillTaskSlotRatio, config.getKillTaskSlotRatio(), 0.001);
    Assert.assertEquals((int) expectedMaxKillTaskSlots, config.getMaxKillTaskSlots());
    Assert.assertEquals(expectedMaxSegmentsInNodeLoadingQueue, config.getMaxSegmentsInNodeLoadingQueue());
    Assert.assertEquals(decommissioningNodes, config.getDecommissioningNodes());
    Assert.assertEquals(pauseCoordination, config.getPauseCoordination());
    Assert.assertEquals(replicateAfterLoadTimeout, config.getReplicateAfterLoadTimeout());
  }

  private static int getDefaultNumBalancerThreads()
  {
    return Math.max(1, JvmUtils.getRuntimeInfo().getAvailableProcessors() / 2);
  }
}

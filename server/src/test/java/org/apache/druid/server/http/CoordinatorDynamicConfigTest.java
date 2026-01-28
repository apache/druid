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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.utils.JvmUtils;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

public class CoordinatorDynamicConfigTest
{
  private static final int EXPECTED_DEFAULT_MAX_SEGMENTS_IN_NODE_LOADING_QUEUE = 500;

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
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
                     + "  \"replicateAfterLoadTimeout\": false,\n"
                     + "  \"turboLoadingNodes\":[\"host1\", \"host3\"],\n"
                     + "  \"cloneServers\":{\"host5\": \"host6\"}\n"
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
    ImmutableSet<String> turboLoadingNodes = ImmutableSet.of("host1", "host3");
    ImmutableMap<String, String> cloneServers = ImmutableMap.of("host5", "host6");
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.15,
        2,
        1,
        decommissioning,
        false,
        false,
        turboLoadingNodes,
        cloneServers
    );

    actual = CoordinatorDynamicConfig.builder().withDecommissioningNodes(ImmutableSet.of("host1")).build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.15,
        2,
        1,
        ImmutableSet.of("host1"),
        false,
        false,
        turboLoadingNodes,
        cloneServers
    );

    actual = CoordinatorDynamicConfig.builder().build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.15,
        2,
        1,
        ImmutableSet.of("host1"),
        false,
        false,
        turboLoadingNodes,
        cloneServers
    );

    actual = CoordinatorDynamicConfig.builder().withPauseCoordination(true).build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.15,
        2,
        1,
        ImmutableSet.of("host1"),
        true,
        false,
        turboLoadingNodes,
        cloneServers
    );

    actual = CoordinatorDynamicConfig.builder().withReplicateAfterLoadTimeout(true).build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.15,
        2,
        1,
        ImmutableSet.of("host1"),
        true,
        true,
        turboLoadingNodes,
        cloneServers
    );

    actual = CoordinatorDynamicConfig.builder().build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.15,
        2,
        1,
        ImmutableSet.of("host1"),
        true,
        true,
        turboLoadingNodes,
        cloneServers
    );

    actual = CoordinatorDynamicConfig.builder().withKillTaskSlotRatio(0.1).build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.1,
        2,
        1,
        ImmutableSet.of("host1"),
        true,
        true,
        turboLoadingNodes,
        cloneServers
    );

    actual = CoordinatorDynamicConfig.builder().withMaxKillTaskSlots(5).build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.1,
        5,
        1,
        ImmutableSet.of("host1"),
        true,
        true,
        turboLoadingNodes,
        cloneServers
    );

    actual = CoordinatorDynamicConfig.builder()
                                     .withTurboLoadingNodes(ImmutableSet.of("host3"))
                                     .withCloneServers(ImmutableMap.of("host3", "host4")).build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.1,
        5,
        1,
        ImmutableSet.of("host1"),
        true,
        true,
        ImmutableSet.of("host3"),
        ImmutableMap.of("host3", "host4")
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
        null,
        ImmutableSet.of("host1"),
        null
    );
    Assert.assertTrue(config.getSpecificDataSourcesToKillUnusedSegmentsIn().isEmpty());
  }

  @Test
  public void testConstructorWithSpecificDataSourcesToKillShouldNotKillUnusedSegmentsInAllDatasources()
  {
    CoordinatorDynamicConfig config = new CoordinatorDynamicConfig(
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
        null,
        ImmutableSet.of("host1"),
        null
    );
    Assert.assertEquals(ImmutableSet.of("test1"), config.getSpecificDataSourcesToKillUnusedSegmentsIn());
  }

  @Test
  public void testDecommissioningParametersBackwardCompatibility() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
                     + "  \"maxSegmentsToMove\": 1,\n"
                     + "  \"replicantLifetime\": 1,\n"
                     + "  \"replicationThrottleLimit\": 1,\n"
                     + "  \"balancerComputeThreads\": 2, \n"
                     + "  \"killDataSourceWhitelist\": [\"test1\",\"test2\"],\n"
                     + "  \"maxSegmentsInNodeLoadingQueue\": 1,\n"
                     + "  \"turboLoadingNodes\": [\"host3\",\"host4\"],\n"
                     + "  \"cloneServers\": {\"host3\":\"host4\", \"host5\":\"host6\"}\n"
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
    ImmutableSet<String> turboLoading = ImmutableSet.of("host3", "host4");
    ImmutableMap<String, String> cloneServers = ImmutableMap.of("host3", "host4", "host5", "host6");
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.1,
        Integer.MAX_VALUE,
        1,
        decommissioning,
        false,
        false,
        turboLoading,
        cloneServers
    );

    actual = CoordinatorDynamicConfig.builder().withDecommissioningNodes(ImmutableSet.of("host1")).build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.1,
        Integer.MAX_VALUE,
        1,
        ImmutableSet.of("host1"),
        false,
        false,
        turboLoading,
        cloneServers
    );

    actual = CoordinatorDynamicConfig.builder().build(actual);
    assertConfig(
        actual,
        1,
        1,
        1,
        1,
        2,
        whitelist,
        0.1,
        Integer.MAX_VALUE,
        1,
        ImmutableSet.of("host1"),
        false,
        false,
        turboLoading,
        cloneServers
    );
  }

  @Test
  public void testSerdeWithStringInKillDataSourceWhitelist() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
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
        2,
        ImmutableSet.of("test1", "test2"),
        0.1,
        Integer.MAX_VALUE,
        1,
        ImmutableSet.of(),
        false,
        false,
        ImmutableSet.of(),
        ImmutableMap.of()
    );
  }

  @Test
  public void testHandleMissingPercentOfSegmentsToConsiderPerMove() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
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
        2,
        whitelist,
        0.1,
        Integer.MAX_VALUE,
        1,
        decommissioning,
        false,
        false,
        ImmutableSet.of(),
        ImmutableMap.of()
    );
  }

  @Test
  public void testDeserializeWithoutMaxSegmentsInNodeLoadingQueue() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
                     + "  \"maxSegmentsToMove\": 1,\n"
                     + "  \"replicantLifetime\": 1,\n"
                     + "  \"replicationThrottleLimit\": 1,\n"
                     + "  \"balancerComputeThreads\": 2\n"
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
        2,
        ImmutableSet.of(),
        0.1,
        Integer.MAX_VALUE,
        EXPECTED_DEFAULT_MAX_SEGMENTS_IN_NODE_LOADING_QUEUE,
        ImmutableSet.of(),
        false,
        false,
        ImmutableSet.of(),
        ImmutableMap.of()
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
        100,
        15,
        500,
        getDefaultNumBalancerThreads(),
        emptyList,
        0.1,
        Integer.MAX_VALUE,
        EXPECTED_DEFAULT_MAX_SEGMENTS_IN_NODE_LOADING_QUEUE,
        emptyList,
        false,
        false,
        ImmutableSet.of(),
        ImmutableMap.of()
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
        100,
        15,
        500,
        getDefaultNumBalancerThreads(),
        ImmutableSet.of("DATASOURCE"),
        0.1,
        Integer.MAX_VALUE,
        EXPECTED_DEFAULT_MAX_SEGMENTS_IN_NODE_LOADING_QUEUE,
        ImmutableSet.of(),
        false,
        false,
        ImmutableSet.of(),
        ImmutableMap.of()
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
        CoordinatorDynamicConfig.builder().build(current)
    );
  }

  @Test
  public void testTurboLoadingNodes()
  {
    CoordinatorDynamicConfig config = CoordinatorDynamicConfig
        .builder()
        .withTurboLoadingNodes(ImmutableSet.of("localhost:8083"))
        .build();

    Assert.assertEquals(SegmentLoadingMode.NORMAL, config.getLoadingModeForServer("localhost:8082"));
    Assert.assertEquals(SegmentLoadingMode.TURBO, config.getLoadingModeForServer("localhost:8083"));
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
      int expectedMaxSegmentsToMove,
      int expectedReplicantLifetime,
      int expectedReplicationThrottleLimit,
      int expectedBalancerComputeThreads,
      Set<String> expectedSpecificDataSourcesToKillUnusedSegmentsIn,
      Double expectedKillTaskSlotRatio,
      @Nullable Integer expectedMaxKillTaskSlots,
      int expectedMaxSegmentsInNodeLoadingQueue,
      Set<String> decommissioningNodes,
      boolean pauseCoordination,
      boolean replicateAfterLoadTimeout,
      Set<String> turboLoadingNodes,
      Map<String, String> cloneServers
  )
  {
    Assert.assertEquals(
        expectedLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments,
        config.getMarkSegmentAsUnusedDelayMillis()
    );
    Assert.assertEquals(expectedMaxSegmentsToMove, config.getMaxSegmentsToMove());
    Assert.assertEquals(expectedReplicantLifetime, config.getReplicantLifetime());
    Assert.assertEquals(expectedReplicationThrottleLimit, config.getReplicationThrottleLimit());
    Assert.assertEquals(expectedBalancerComputeThreads, config.getBalancerComputeThreads());
    Assert.assertEquals(
        expectedSpecificDataSourcesToKillUnusedSegmentsIn,
        config.getSpecificDataSourcesToKillUnusedSegmentsIn()
    );
    Assert.assertEquals(expectedKillTaskSlotRatio, config.getKillTaskSlotRatio(), 0.001);
    Assert.assertEquals((int) expectedMaxKillTaskSlots, config.getMaxKillTaskSlots());
    Assert.assertEquals(expectedMaxSegmentsInNodeLoadingQueue, config.getMaxSegmentsInNodeLoadingQueue());
    Assert.assertEquals(decommissioningNodes, config.getDecommissioningNodes());
    Assert.assertEquals(pauseCoordination, config.getPauseCoordination());
    Assert.assertEquals(replicateAfterLoadTimeout, config.getReplicateAfterLoadTimeout());
    Assert.assertEquals(turboLoadingNodes, config.getTurboLoadingNodes());
    Assert.assertEquals(cloneServers, config.getCloneServers());
  }

  private static int getDefaultNumBalancerThreads()
  {
    return Math.max(1, JvmUtils.getRuntimeInfo().getAvailableProcessors() / 2);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(CoordinatorDynamicConfig.class)
                  .withIgnoredFields("validDebugDimensions")
                  .usingGetClass()
                  .verify();
  }
}

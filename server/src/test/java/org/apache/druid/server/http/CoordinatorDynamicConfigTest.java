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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 */
public class CoordinatorDynamicConfigTest
{
  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
                     + "  \"mergeBytesLimit\": 1,\n"
                     + "  \"mergeSegmentsLimit\" : 1,\n"
                     + "  \"maxSegmentsToMove\": 1,\n"
                     + "  \"percentOfSegmentsToConsiderPerMove\": 1,\n"
                     + "  \"replicantLifetime\": 1,\n"
                     + "  \"replicationThrottleLimit\": 1,\n"
                     + "  \"balancerComputeThreads\": 2, \n"
                     + "  \"emitBalancingStats\": true,\n"
                     + "  \"killDataSourceWhitelist\": [\"test1\",\"test2\"],\n"
                     + "  \"maxSegmentsInNodeLoadingQueue\": 1,\n"
                     + "  \"decommissioningNodes\": [\"host1\", \"host2\"],\n"
                     + "  \"decommissioningMaxPercentOfMaxSegmentsToMove\": 9,\n"
                     + "  \"pauseCoordination\": false\n"
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
    assertConfig(actual, 1, 1, 1, 1, 1, 1, 1, 2, true, whitelist, false, 1, decommissioning, 9, false);

    actual = CoordinatorDynamicConfig.builder().withDecommissioningNodes(ImmutableSet.of("host1")).build(actual);
    assertConfig(actual, 1, 1, 1, 1, 1, 1, 1, 2, true, whitelist, false, 1, ImmutableSet.of("host1"), 9, false);

    actual = CoordinatorDynamicConfig.builder().withDecommissioningMaxPercentOfMaxSegmentsToMove(5).build(actual);
    assertConfig(actual, 1, 1, 1, 1, 1, 1, 1, 2, true, whitelist, false, 1, ImmutableSet.of("host1"), 5, false);

    actual = CoordinatorDynamicConfig.builder().withPauseCoordination(true).build(actual);
    assertConfig(actual, 1, 1, 1, 1, 1, 1, 1, 2, true, whitelist, false, 1, ImmutableSet.of("host1"), 5, true);

    actual = CoordinatorDynamicConfig.builder().withPercentOfSegmentsToConsiderPerMove(10).build(actual);
    assertConfig(actual, 1, 1, 1, 1, 10, 1, 1, 2, true, whitelist, false, 1, ImmutableSet.of("host1"), 5, true);
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
                     + "  \"emitBalancingStats\": true,\n"
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
    assertConfig(actual, 1, 1, 1, 1, 100, 1, 1, 2, true, whitelist, false, 1, decommissioning, 0, false);

    actual = CoordinatorDynamicConfig.builder().withDecommissioningNodes(ImmutableSet.of("host1")).build(actual);
    assertConfig(actual, 1, 1, 1, 1, 100, 1, 1, 2, true, whitelist, false, 1, ImmutableSet.of("host1"), 0, false);

    actual = CoordinatorDynamicConfig.builder().withDecommissioningMaxPercentOfMaxSegmentsToMove(5).build(actual);
    assertConfig(actual, 1, 1, 1, 1, 100, 1, 1, 2, true, whitelist, false, 1, ImmutableSet.of("host1"), 5, false);
  }

  @Test
  public void testSerdeWithStringinKillDataSourceWhitelist() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
                     + "  \"mergeBytesLimit\": 1,\n"
                     + "  \"mergeSegmentsLimit\" : 1,\n"
                     + "  \"maxSegmentsToMove\": 1,\n"
                     + "  \"percentOfSegmentsToConsiderPerMove\": 1,\n"
                     + "  \"replicantLifetime\": 1,\n"
                     + "  \"replicationThrottleLimit\": 1,\n"
                     + "  \"balancerComputeThreads\": 2, \n"
                     + "  \"emitBalancingStats\": true,\n"
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
        1,
        2,
        true,
        ImmutableSet.of("test1", "test2"),
        false,
        1,
        ImmutableSet.of(),
        0,
        false
    );
  }

  @Test
  public void testSerdeHandleInvalidPercentOfSegmentsToConsiderPerMove() throws Exception
  {
    try {
      String jsonStr = "{\n"
                       + "  \"percentOfSegmentsToConsiderPerMove\": 0\n"
                       + "}\n";

      mapper.readValue(
          mapper.writeValueAsString(
              mapper.readValue(
                  jsonStr,
                  CoordinatorDynamicConfig.class
              )
          ),
          CoordinatorDynamicConfig.class
      );

      Assert.fail("deserialization should fail.");
    }
    catch (JsonMappingException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
    }

    try {
      String jsonStr = "{\n"
                       + "  \"percentOfSegmentsToConsiderPerMove\": -100\n"
                       + "}\n";

      mapper.readValue(
          mapper.writeValueAsString(
              mapper.readValue(
                  jsonStr,
                  CoordinatorDynamicConfig.class
              )
          ),
          CoordinatorDynamicConfig.class
      );

      Assert.fail("deserialization should fail.");
    }
    catch (JsonMappingException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
    }

    try {
      String jsonStr = "{\n"
                       + "  \"percentOfSegmentsToConsiderPerMove\": 105\n"
                       + "}\n";

      mapper.readValue(
          mapper.writeValueAsString(
              mapper.readValue(
                  jsonStr,
                  CoordinatorDynamicConfig.class
              )
          ),
          CoordinatorDynamicConfig.class
      );

      Assert.fail("deserialization should fail.");
    }
    catch (JsonMappingException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
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
                     + "  \"emitBalancingStats\": true,\n"
                     + "  \"killDataSourceWhitelist\": [\"test1\",\"test2\"],\n"
                     + "  \"maxSegmentsInNodeLoadingQueue\": 1,\n"
                     + "  \"decommissioningNodes\": [\"host1\", \"host2\"],\n"
                     + "  \"decommissioningMaxPercentOfMaxSegmentsToMove\": 9,\n"
                     + "  \"pauseCoordination\": false\n"
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
    assertConfig(actual, 1, 1, 1, 1, 100, 1, 1, 2, true, whitelist, false, 1, decommissioning, 9, false);
  }

  @Test
  public void testSerdeWithKillAllDataSources() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
                     + "  \"mergeBytesLimit\": 1,\n"
                     + "  \"mergeSegmentsLimit\" : 1,\n"
                     + "  \"maxSegmentsToMove\": 1,\n"
                     + "  \"percentOfSegmentsToConsiderPerMove\": 1,\n"
                     + "  \"replicantLifetime\": 1,\n"
                     + "  \"replicationThrottleLimit\": 1,\n"
                     + "  \"balancerComputeThreads\": 2, \n"
                     + "  \"emitBalancingStats\": true,\n"
                     + "  \"killAllDataSources\": true,\n"
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

    assertConfig(actual, 1, 1, 1, 1, 1, 1, 1, 2, true, ImmutableSet.of(), true, 1, ImmutableSet.of(), 0, false);

    //ensure whitelist is empty when killAllDataSources is true
    try {
      jsonStr = "{\n"
                + "  \"killDataSourceWhitelist\": [\"test1\",\"test2\"],\n"
                + "  \"killAllDataSources\": true,\n"
                + "  \"percentOfSegmentsToConsiderPerMove\": 1\n"
                + "}\n";
      mapper.readValue(
          jsonStr,
          CoordinatorDynamicConfig.class
      );

      Assert.fail("deserialization should fail.");
    }
    catch (JsonMappingException e) {
      Assert.assertTrue(e.getCause() instanceof IAE);
    }
  }

  @Test
  public void testDeserializeWithoutMaxSegmentsInNodeLoadingQueue() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"millisToWaitBeforeDeleting\": 1,\n"
                     + "  \"mergeBytesLimit\": 1,\n"
                     + "  \"mergeSegmentsLimit\" : 1,\n"
                     + "  \"maxSegmentsToMove\": 1,\n"
                     + "  \"percentOfSegmentsToConsiderPerMove\": 1,\n"
                     + "  \"replicantLifetime\": 1,\n"
                     + "  \"replicationThrottleLimit\": 1,\n"
                     + "  \"balancerComputeThreads\": 2, \n"
                     + "  \"emitBalancingStats\": true,\n"
                     + "  \"killAllDataSources\": true\n"
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

    assertConfig(actual, 1, 1, 1, 1, 1, 1, 1, 2, true, ImmutableSet.of(), true, 0, ImmutableSet.of(), 0, false);
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
        5,
        100,
        15,
        10,
        1,
        false,
        emptyList,
        false,
        0,
        emptyList,
        70,
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
        new CoordinatorDynamicConfig
            .Builder(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
            .build(current)
    );
  }

  @Test
  public void testEqualsAndHashCodeSanity()
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
      int expectedPercentOfSegmentsToConsiderPerMove,
      int expectedReplicantLifetime,
      int expectedReplicationThrottleLimit,
      int expectedBalancerComputeThreads,
      boolean expectedEmitingBalancingStats,
      Set<String> expectedSpecificDataSourcesToKillUnusedSegmentsIn,
      boolean expectedKillUnusedSegmentsInAllDataSources,
      int expectedMaxSegmentsInNodeLoadingQueue,
      Set<String> decommissioningNodes,
      int decommissioningMaxPercentOfMaxSegmentsToMove,
      boolean pauseCoordination
  )
  {
    Assert.assertEquals(
        expectedLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments,
        config.getLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments()
    );
    Assert.assertEquals(expectedMergeBytesLimit, config.getMergeBytesLimit());
    Assert.assertEquals(expectedMergeSegmentsLimit, config.getMergeSegmentsLimit());
    Assert.assertEquals(expectedMaxSegmentsToMove, config.getMaxSegmentsToMove());
    Assert.assertEquals(expectedPercentOfSegmentsToConsiderPerMove, config.getPercentOfSegmentsToConsiderPerMove(), 0);
    Assert.assertEquals(expectedReplicantLifetime, config.getReplicantLifetime());
    Assert.assertEquals(expectedReplicationThrottleLimit, config.getReplicationThrottleLimit());
    Assert.assertEquals(expectedBalancerComputeThreads, config.getBalancerComputeThreads());
    Assert.assertEquals(expectedEmitingBalancingStats, config.emitBalancingStats());
    Assert.assertEquals(
        expectedSpecificDataSourcesToKillUnusedSegmentsIn,
        config.getSpecificDataSourcesToKillUnusedSegmentsIn()
    );
    Assert.assertEquals(expectedKillUnusedSegmentsInAllDataSources, config.isKillUnusedSegmentsInAllDataSources());
    Assert.assertEquals(expectedMaxSegmentsInNodeLoadingQueue, config.getMaxSegmentsInNodeLoadingQueue());
    Assert.assertEquals(decommissioningNodes, config.getDecommissioningNodes());
    Assert.assertEquals(
        decommissioningMaxPercentOfMaxSegmentsToMove,
        config.getDecommissioningMaxPercentOfMaxSegmentsToMove()
    );
    Assert.assertEquals(pauseCoordination, config.getPauseCoordination());
  }
}

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

package io.druid.server.http;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.druid.java.util.common.IAE;
import io.druid.segment.TestHelper;
import io.druid.server.coordinator.CoordinatorDynamicConfig;
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
    assertConfig(actual, 1, 1, 1, 1, 1, 1, 2, true, ImmutableSet.of("test1", "test2"), false, 1);
  }

  @Test
  public void testSerdeWithStringinKillDataSourceWhitelist() throws Exception
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
                     + "  \"killDataSourceWhitelist\": \" test1 ,test2 \", \n"
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
    assertConfig(actual, 1, 1, 1, 1, 1, 1, 2, true, ImmutableSet.of("test1", "test2"), false, 1);
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

    assertConfig(actual, 1, 1, 1, 1, 1, 1, 2, true, ImmutableSet.of(), true, 1);

    //ensure whitelist is empty when killAllDataSources is true
    try {
      jsonStr = "{\n"
                + "  \"killDataSourceWhitelist\": [\"test1\",\"test2\"],\n"
                + "  \"killAllDataSources\": true\n"
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

    assertConfig(actual, 1, 1, 1, 1, 1, 1, 2, true, ImmutableSet.of(), true, 0);
  }

  @Test
  public void testBuilderDefaults()
  {

    CoordinatorDynamicConfig defaultConfig = CoordinatorDynamicConfig.builder().build();
    assertConfig(defaultConfig, 900000, 524288000, 100, 5, 15, 10, 1, false, ImmutableSet.of(), false, 0);
  }

  @Test
  public void testUpdate()
  {
    CoordinatorDynamicConfig current = CoordinatorDynamicConfig.builder()
                                                               .withKillDataSourceWhitelist(ImmutableSet.of("x"))
                                                               .build();

    Assert.assertEquals(
        current,
        new CoordinatorDynamicConfig.Builder(null, null, null, null, null, null, null, null, null, null, null).build(current)
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

  private void assertConfig(CoordinatorDynamicConfig config,
                            long expectedMillisToWaitBeforeDeleting,
                            long expectedMergeBytesLimit,
                            int expectedMergeSegmentsLimit,
                            int expectedMaxSegmentsToMove,
                            int expectedReplicantLifetime,
                            int expectedReplicationThrottleLimit,
                            int expectedBalancerComputeThreads,
                            boolean expectedEmitingBalancingStats,
                            Set<String> expectedKillDataSourceWhitelist,
                            boolean expectedKillAllDataSources,
                            int expectedMaxSegmentsInNodeLoadingQueue)
  {
    Assert.assertEquals(expectedMillisToWaitBeforeDeleting, config.getMillisToWaitBeforeDeleting());
    Assert.assertEquals(expectedMergeBytesLimit, config.getMergeBytesLimit());
    Assert.assertEquals(expectedMergeSegmentsLimit, config.getMergeSegmentsLimit());
    Assert.assertEquals(expectedMaxSegmentsToMove, config.getMaxSegmentsToMove());
    Assert.assertEquals(expectedReplicantLifetime, config.getReplicantLifetime());
    Assert.assertEquals(expectedReplicationThrottleLimit, config.getReplicationThrottleLimit());
    Assert.assertEquals(expectedBalancerComputeThreads, config.getBalancerComputeThreads());
    Assert.assertEquals(expectedEmitingBalancingStats, config.emitBalancingStats());
    Assert.assertEquals(expectedKillDataSourceWhitelist, config.getKillDataSourceWhitelist());
    Assert.assertEquals(expectedKillAllDataSources, config.isKillAllDataSources());
    Assert.assertEquals(expectedMaxSegmentsInNodeLoadingQueue, config.getMaxSegmentsInNodeLoadingQueue());
  }
}

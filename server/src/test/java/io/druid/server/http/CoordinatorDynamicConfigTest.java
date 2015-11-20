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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.segment.TestHelper;
import io.druid.server.coordinator.CoordinatorDynamicConfig;
import io.druid.server.coordinator.CoordinatorHadoopMergeSpec;
import io.druid.server.coordinator.DruidCoordinatorHadoopMergeConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class CoordinatorDynamicConfigTest
{
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
                     + "  \"emitBalancingStats\": true,\n"
                     + "  \"killDataSourceWhitelist\": [\"test\"],\n"
                     + "  \"hadoopMergeConfig\":{\n"
                     + "  \"keepGap\":true,\n"
                     + "  \"hadoopDependencyCoordinates\":[\"org.apache.hadoop:hadoop-client:2.3.0\"],\n"
                     + "  \"tuningConfig\":null,\n"
                     + "  \"hadoopMergeSpecs\":[\n"
                     + "  {\n"
                     + "  \"dataSource\":\"wiki\",\n"
                     + "  \"queryGranularity\":null,\n"
                     + "  \"dimensions\":null,\n"
                     + "  \"metricsSpec\":[\n"
                     + "  {\n"
                     + "  \"type\":\"count\",\n"
                     + "  \"name\":\"count\"\n"
                     + "  },\n"
                     + "  {\n"
                     + "  \"type\":\"doubleSum\",\n"
                     + "  \"name\":\"added\",\n"
                     + "  \"fieldName\":\"added\"\n"
                     + "  },\n"
                     + "  {\n"
                     + "  \"type\":\"doubleSum\",\n"
                     + "  \"name\":\"deleted\",\n"
                     + "  \"fieldName\":\"deleted\"\n"
                     + "  },\n"
                     + "  {\n"
                     + "  \"type\":\"doubleSum\",\n"
                     + "  \"name\":\"delta\",\n"
                     + "  \"fieldName\":\"delta\"\n"
                     + "  }\n"
                     + "  ]\n"
                     + "  },\n"
                     + "  {\n"
                     + "  \"dataSource\":\"wikipedia\",\n"
                     + "  \"queryGranularity\":\"DAY\",\n"
                     + "  \"dimensions\":[\"language\"],\n"
                     + "  \"metricsSpec\":[\n"
                     + "  {\n"
                     + "  \"type\":\"count\",\n"
                     + "  \"name\":\"count\"\n"
                     + "  },\n"
                     + "  {\n"
                     + "  \"type\":\"doubleSum\",\n"
                     + "  \"name\":\"added\",\n"
                     + "  \"fieldName\":\"added\"\n"
                     + "  },\n"
                     + "  {\n"
                     + "  \"type\":\"doubleSum\",\n"
                     + "  \"name\":\"deleted\",\n"
                     + "  \"fieldName\":\"deleted\"\n"
                     + "  },\n"
                     + "  {\n"
                     + "  \"type\":\"doubleSum\",\n"
                     + "  \"name\":\"delta\",\n"
                     + "  \"fieldName\":\"delta\"\n"
                     + "  }\n"
                     + "  ]\n"
                     + "  }\n"
                     + "  ]\n"
                     + "  }\n"
                     + "  }\n";

    ObjectMapper mapper = TestHelper.getObjectMapper();
    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );

    Assert.assertEquals(
        new CoordinatorDynamicConfig(
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            true,
            ImmutableSet.of("test"),
            new DruidCoordinatorHadoopMergeConfig(
                true,
                ImmutableList.<String>of("org.apache.hadoop:hadoop-client:2.3.0"),
                null,
                ImmutableList.<CoordinatorHadoopMergeSpec>of(
                    new CoordinatorHadoopMergeSpec(
                        "wiki",
                        null,
                        null,
                        ImmutableList.of(
                            new CountAggregatorFactory("count"),
                            new DoubleSumAggregatorFactory("added", "added"),
                            new DoubleSumAggregatorFactory("deleted", "deleted"),
                            new DoubleSumAggregatorFactory("delta", "delta")
                        ).toArray(new AggregatorFactory[4])
                    ),
                    new CoordinatorHadoopMergeSpec(
                        "wikipedia",
                        QueryGranularity.DAY,
                        ImmutableList.<String>of("language"),
                        ImmutableList.<AggregatorFactory>of(
                            new CountAggregatorFactory("count"),
                            new DoubleSumAggregatorFactory("added", "added"),
                            new DoubleSumAggregatorFactory("deleted", "deleted"),
                            new DoubleSumAggregatorFactory("delta", "delta")
                        ).toArray(new AggregatorFactory[4])
                    )
                )
            )
        ),
        actual
    );
  }

  @Test
  public void testBuilderDefaults()
  {
    Assert.assertEquals(
        new CoordinatorDynamicConfig(900000, 524288000, 100, 5, 15, 10, 1, false, null, null),
        new CoordinatorDynamicConfig.Builder().build()
    );
  }

  @Test
  public void testEqualsAndHashCodeSanity()
  {
    CoordinatorDynamicConfig config1 = new CoordinatorDynamicConfig(
        900000,
        524288000,
        100,
        5,
        15,
        10,
        1,
        false,
        null,
        new DruidCoordinatorHadoopMergeConfig(
            true,
            ImmutableList.<String>of("org.apache.hadoop:hadoop-client:2.3.0"),
            null,
            ImmutableList.<CoordinatorHadoopMergeSpec>of(
                new CoordinatorHadoopMergeSpec(
                    "wiki",
                    null,
                    null,
                    ImmutableList.of(
                        new CountAggregatorFactory("count"),
                        new DoubleSumAggregatorFactory("added", "added"),
                        new DoubleSumAggregatorFactory("deleted", "deleted"),
                        new DoubleSumAggregatorFactory("delta", "delta")
                    ).toArray(new AggregatorFactory[4])
                ),
                new CoordinatorHadoopMergeSpec(
                    "wikipedia",
                    QueryGranularity.DAY,
                    ImmutableList.<String>of("language"),
                    ImmutableList.<AggregatorFactory>of(
                        new CountAggregatorFactory("count"),
                        new DoubleSumAggregatorFactory("added", "added"),
                        new DoubleSumAggregatorFactory("deleted", "deleted"),
                        new DoubleSumAggregatorFactory("delta", "delta")
                    ).toArray(new AggregatorFactory[4])
                )
            )
        )
    );
    CoordinatorDynamicConfig config2 = new CoordinatorDynamicConfig(
        900000,
        524288000,
        100,
        5,
        15,
        10,
        1,
        false,
        null,
        new DruidCoordinatorHadoopMergeConfig(
            true,
            ImmutableList.<String>of("org.apache.hadoop:hadoop-client:2.3.0"),
            null,
            ImmutableList.<CoordinatorHadoopMergeSpec>of(
                new CoordinatorHadoopMergeSpec(
                    "wiki",
                    null,
                    null,
                    ImmutableList.of(
                        new CountAggregatorFactory("count"),
                        new DoubleSumAggregatorFactory("added", "added"),
                        new DoubleSumAggregatorFactory("deleted", "deleted"),
                        new DoubleSumAggregatorFactory("delta", "delta")
                    ).toArray(new AggregatorFactory[4])
                ),
                new CoordinatorHadoopMergeSpec(
                    "wikipedia",
                    QueryGranularity.DAY,
                    ImmutableList.<String>of("language"),
                    ImmutableList.<AggregatorFactory>of(
                        new CountAggregatorFactory("count"),
                        new DoubleSumAggregatorFactory("added", "added"),
                        new DoubleSumAggregatorFactory("deleted", "deleted"),
                        new DoubleSumAggregatorFactory("delta", "delta")
                    ).toArray(new AggregatorFactory[4])
                )
            )
        )
    );

    Assert.assertEquals(config1, config2);
    Assert.assertEquals(config1.hashCode(), config2.hashCode());
  }
}

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

package io.druid.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregationTest;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;


/**
 */
public class DruidCoordinatorHadoopMergeConfigTest
{

  private static final List<String> HADOOP_COORDINATES = ImmutableList.of("org.apache.hadoop:hadoop-client:2.3.0");
  private static final Map<String, Object> TUNING_CONFIG = ImmutableMap.<String, Object>builder()
      .put("type", "hadoop")
      .put(
          "partitionSpec",
          ImmutableMap.of(
              "assumeGrouped",
              true,
              "targetPartitionSize",
              1000,
              "type",
              "hashed"
          )
      )
      .put("rowFlushBoundary", 10000)
      .build();

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final DruidCoordinatorHadoopMergeConfig config = new DruidCoordinatorHadoopMergeConfig(
        true,
        HADOOP_COORDINATES,
        TUNING_CONFIG,
        ImmutableList.<CoordinatorHadoopMergeSpec>of(
            new CoordinatorHadoopMergeSpec(
                "datasource1",
                QueryGranularity.DAY,
                ImmutableList.<String>of("d1", "d2", "d3"),
                new AggregatorFactory[]{
                    new CountAggregatorFactory("count"),
                    new DoubleSumAggregatorFactory("added", "added"),
                    new LongSumAggregatorFactory("deleted", "deleted"),
                    new LongMinAggregatorFactory("delta", "delta")
                }
            ),
            new CoordinatorHadoopMergeSpec(
                "datasource2",
                QueryGranularity.HOUR,
                ImmutableList.<String>of("d4", "d5", "d6"),
                new AggregatorFactory[] {
                    new CountAggregatorFactory("count2"),
                    new DoubleMaxAggregatorFactory("added2", "added2"),
                    new LongSumAggregatorFactory("deleted2", "deleted2"),
                    new LongMinAggregatorFactory("delta2", "delta2")
                }
            )
        )
    );

    Assert.assertEquals(
        config,
        mapper.readValue(mapper.writeValueAsString(config), DruidCoordinatorHadoopMergeConfig.class)
    );
  }
}

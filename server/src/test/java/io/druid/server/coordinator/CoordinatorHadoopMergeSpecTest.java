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
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 */
public class CoordinatorHadoopMergeSpecTest
{

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final String dataSource = "wikipedia";
    final QueryGranularity queryGranularity = QueryGranularity.ALL;
    final List<String> dimensions = ImmutableList.of("a", "b", "c");
    final AggregatorFactory[] metricsSpec = new AggregatorFactory[]{
        new CountAggregatorFactory("count"),
        new DoubleSumAggregatorFactory("added", "added"),
        new LongSumAggregatorFactory("deleted", "deleted"),
        new LongMinAggregatorFactory("delta", "delta")
    };

    final CoordinatorHadoopMergeSpec mergeSpec = new CoordinatorHadoopMergeSpec(
        dataSource,
        queryGranularity,
        dimensions,
        metricsSpec
    );

    Assert.assertEquals(
        mergeSpec,
        mapper.readValue(mapper.writeValueAsString(mergeSpec), CoordinatorHadoopMergeSpec.class)
    );
  }
}
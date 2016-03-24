/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.client.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import io.druid.segment.indexing.granularity.GranularitySpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 */
public class ClientHadoopIndexTaskTest
{
  private static final String TASK_ID = "coordinator_index_task_1234";
  private static final String DATA_SOURCE = "wikipedia";

  private static final List<Interval> INTERVAL_LIST = ImmutableList.of(
      new Interval("2012-01-01/2012-01-03"),
      new Interval("2012-01-05/2012-01-08"),
      new Interval("2012-01-10/2012-01-14")
  );

  private static final List<String> DIMENSIONS = ImmutableList.of("a", "b", "c");

  private static final AggregatorFactory[] AGGREGATORS = new AggregatorFactory[]{
      new CountAggregatorFactory("name"),
      new DoubleSumAggregatorFactory(
          "added",
          "added"
      ),
      new DoubleSumAggregatorFactory(
          "deleted",
          "deleted"
      ),
      new DoubleSumAggregatorFactory(
          "delta",
          "delta"
      )
  };

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
  public void testInit() throws IOException
  {
    final ClientHadoopIndexTask clientHadoopIndexTask = new ClientHadoopIndexTask(
        TASK_ID,
        DATA_SOURCE,
        INTERVAL_LIST,
        AGGREGATORS,
        DIMENSIONS,
        QueryGranularity.DAY,
        TUNING_CONFIG,
        null,
        new DefaultObjectMapper()
    );

    final ClientHadoopIngestionSpec hadoopIngestionSpec = clientHadoopIndexTask.getHadoopIngestionSpec();
    final DataSchema dataSchema = hadoopIngestionSpec.getDataSchema();
    final GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    final ClientHadoopIOConfig hadoopIOConfig = hadoopIngestionSpec.getIOConfig();
    final Map<String, Object> tuningConfig = hadoopIngestionSpec.getTuningConfig();

    Assert.assertEquals("index_hadoop", clientHadoopIndexTask.getType());
    Assert.assertEquals(TASK_ID, clientHadoopIndexTask.getId());
    Assert.assertEquals(DATA_SOURCE, dataSchema.getDataSource());
    Assert.assertArrayEquals(AGGREGATORS, dataSchema.getAggregators());
    Assert.assertEquals(QueryGranularity.DAY, granularitySpec.getQueryGranularity());
    Assert.assertTrue(granularitySpec instanceof ArbitraryGranularitySpec);
    Assert.assertEquals(Sets.newHashSet(INTERVAL_LIST), granularitySpec.bucketIntervals().get());
    Assert.assertEquals(TUNING_CONFIG, tuningConfig);
    Assert.assertEquals(ImmutableMap.<String, Object>of(
        "type",
        "dataSource",
        "ingestionSpec",
        ImmutableMap.of("dataSource",
                        DATA_SOURCE,
                        "intervals",
                        INTERVAL_LIST,
                        "dimensions",
                        DIMENSIONS
        )
    ), hadoopIOConfig.getPathSpec());
  }

}

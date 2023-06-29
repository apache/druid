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

package org.apache.druid.query.aggregation.bloom;

import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

public class BloomFilterAggregatorFactoryTest
{
  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(
                  new CountAggregatorFactory("count"),
                  new BloomFilterAggregatorFactory("bloom", DefaultDimensionSpec.of("col"), 1024),
                  new BloomFilterMergeAggregatorFactory("bloomMerge", "bloom", 1024)
              )
              .postAggregators(
                  new FieldAccessPostAggregator("bloom-access", "bloom"),
                  new FinalizingFieldAccessPostAggregator("bloom-finalize", "bloom"),
                  new FieldAccessPostAggregator("bloomMerge-access", "bloomMerge"),
                  new FinalizingFieldAccessPostAggregator("bloomMerge-finalize", "bloomMerge")
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ColumnType.LONG)
                    .add("bloom", BloomFilterAggregatorFactory.TYPE)
                    .add("bloomMerge", BloomFilterAggregatorFactory.TYPE)
                    .add("bloom-access", BloomFilterAggregatorFactory.TYPE)
                    .add("bloom-finalize", BloomFilterAggregatorFactory.TYPE)
                    .add("bloomMerge-access", BloomFilterAggregatorFactory.TYPE)
                    .add("bloomMerge-finalize", BloomFilterAggregatorFactory.TYPE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }

  @Test
  public void testWithNameBloomFilterAggFactory()
  {
    BloomFilterAggregatorFactory factory = new BloomFilterAggregatorFactory(
        "bloom",
        DefaultDimensionSpec.of("col"),
        1024
    );
    Assert.assertEquals(factory, factory.withName("bloom"));
    Assert.assertEquals("newTest", factory.withName("newTest").getName());
  }


  @Test
  public void testWithNameBloomFilterMergeAggFactory()
  {
    BloomFilterMergeAggregatorFactory factory = new BloomFilterMergeAggregatorFactory("bloomMerge", "bloom", 1024);
    Assert.assertEquals(factory, factory.withName("bloomMerge"));
    Assert.assertEquals("newTest", factory.withName("newTest").getName());
  }
}

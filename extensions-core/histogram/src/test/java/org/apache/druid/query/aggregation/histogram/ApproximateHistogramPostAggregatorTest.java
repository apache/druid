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

package org.apache.druid.query.aggregation.histogram;

import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.HistogramAggregatorFactory;
import org.apache.druid.query.aggregation.TestFloatColumnSelector;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ApproximateHistogramPostAggregatorTest extends InitializedNullHandlingTest
{
  static final float[] VALUES = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  protected ApproximateHistogram buildHistogram(int size, float[] values)
  {
    ApproximateHistogram h = new ApproximateHistogram(size);
    for (float v : values) {
      h.offer(v);
    }
    return h;
  }

  @Test
  public void testApproxHistogramCompute()
  {
    ApproximateHistogram ah = buildHistogram(10, VALUES);
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(VALUES);

    ApproximateHistogramAggregator agg = new ApproximateHistogramAggregator(selector, 10, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY);
    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < VALUES.length; i++) {
      agg.aggregate();
      selector.increment();
    }

    Map<String, Object> metricValues = new HashMap<String, Object>();
    metricValues.put("price", agg.get());

    ApproximateHistogramPostAggregator approximateHistogramPostAggregator = new EqualBucketsPostAggregator(
        "approxHist",
        "price",
        5
    );
    Assert.assertEquals(ah.toHistogram(5), approximateHistogramPostAggregator.compute(metricValues));
  }

  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(new ApproximateHistogramAggregatorFactory("approxHisto", "col", null, null, null, null, false))
              .postAggregators(
                  new BucketsPostAggregator("bucket", "approxHisto", 100, 0),
                  new EqualBucketsPostAggregator("equal", "approxHisto", 5),
                  new CustomBucketsPostAggregator("custom", "approxHisto", new float[]{1.0f, 20.0f, 75.0f}),
                  new MinPostAggregator("min", "approxHisto"),
                  new MaxPostAggregator("max", "approxHisto"),
                  new QuantilePostAggregator("quantile", "approxHisto", 0.5f),
                  new QuantilesPostAggregator("quantiles", "approxHisto", new float[]{0.2f, 0.5f, 0.75f})
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("approxHisto", null)
                    .add("bucket", HistogramAggregatorFactory.TYPE)
                    .add("equal", HistogramAggregatorFactory.TYPE)
                    .add("custom", HistogramAggregatorFactory.TYPE)
                    .add("min", ColumnType.DOUBLE)
                    .add("max", ColumnType.DOUBLE)
                    .add("quantile", ColumnType.FLOAT)
                    .add("quantiles", ColumnType.UNKNOWN_COMPLEX)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}

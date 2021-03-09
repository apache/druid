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

package org.apache.druid.query.aggregation.post;

import org.apache.druid.com.google.common.collect.Lists;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

public class JavaScriptPostAggregatorTest
{
  private static final String ABS_PERCENT_FUNCTION = "function(delta, total) { return 100 * Math.abs(delta) / total; }";
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testCompute()
  {
    JavaScriptPostAggregator javaScriptPostAggregator;

    Map<String, Object> metricValues = new HashMap<>();
    metricValues.put("delta", -10.0);
    metricValues.put("total", 100.0);

    javaScriptPostAggregator = new JavaScriptPostAggregator(
        "absPercent",
        Lists.newArrayList("delta", "total"),
        ABS_PERCENT_FUNCTION,
        JavaScriptConfig.getEnabledInstance()
    );

    Assert.assertEquals(10.0, javaScriptPostAggregator.compute(metricValues));
  }

  @Test
  public void testComputeJavaScriptNotAllowed()
  {
    JavaScriptPostAggregator aggregator = new JavaScriptPostAggregator(
        "absPercent",
        Lists.newArrayList("delta", "total"),
        ABS_PERCENT_FUNCTION,
        new JavaScriptConfig(false)
    );

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("JavaScript is disabled");
    aggregator.compute(new HashMap<>());
  }

  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(
                  new LongSumAggregatorFactory("total", "total"),
                  new LongSumAggregatorFactory("delta", "delta")
              )
              .postAggregators(
                  new JavaScriptPostAggregator(
                      "a",
                      Lists.newArrayList("delta", "total"),
                      ABS_PERCENT_FUNCTION,
                      JavaScriptConfig.getEnabledInstance()
                  )
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("total", ValueType.LONG)
                    .add("delta", ValueType.LONG)
                    .add("a", ValueType.DOUBLE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}

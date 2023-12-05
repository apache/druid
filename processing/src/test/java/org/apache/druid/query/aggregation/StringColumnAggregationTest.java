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

package org.apache.druid.query.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.NoopInputRowParser;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Result;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StringColumnAggregationTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final String singleValue = "singleValue";
  private final String multiValue = "multiValue";

  private final int n = 10;

  // results after aggregation
  private long numRows;
  private double singleValueSum;
  private double multiValueSum;
  private double singleValueMax;
  private double multiValueMax;
  private double singleValueMin;
  private double multiValueMin;

  private List<Segment> segments;

  private AggregationTestHelper aggregationTestHelper;

  @Before
  public void setup() throws Exception
  {
    List<String> dimensions = ImmutableList.of(singleValue, multiValue);
    List<InputRow> inputRows = new ArrayList<>(n);
    for (int i = 1; i <= n; i++) {
      String val = String.valueOf(i * 1.0d);

      inputRows.add(new MapBasedInputRow(
          DateTime.now(DateTimeZone.UTC),
          dimensions,
          ImmutableMap.of(
              singleValue, val,
              multiValue, Lists.newArrayList(val, null, val)
          )
      ));
    }

    aggregationTestHelper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Collections.emptyList(),
        new GroupByQueryConfig(),
        tempFolder
    );

    IncrementalIndex index = AggregationTestHelper.createIncrementalIndex(
        inputRows.iterator(),
        new NoopInputRowParser(null),
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        0,
        Granularities.NONE,
        100,
        false
    );

    this.segments = ImmutableList.of(
        new IncrementalIndexSegment(index, SegmentId.dummy("test")),
        aggregationTestHelper.persistIncrementalIndex(index, null)
    );

    // we have ingested arithmetic progression from 1 to 10, so sums can be computed using following
    // All sum values are multiplied by 2 because we are running query on duplicated segment twice.
    numRows = 2 * n;
    singleValueSum = n * (n + 1);
    multiValueSum = 2 * n * (n + 1);
    singleValueMax = n;
    multiValueMax = n;
    singleValueMin = 1;
    multiValueMin = 1;
  }

  @After
  public void tearDown()
  {
    if (segments != null) {
      for (Segment seg : segments) {
        CloseableUtils.closeAndWrapExceptions(seg);
      }
    }
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource("test")
        .setGranularity(Granularities.ALL)
        .setInterval("1970/2050")
        .setAggregatorSpecs(
            new DoubleSumAggregatorFactory("singleDoubleSum", singleValue),
            new DoubleSumAggregatorFactory("multiDoubleSum", multiValue),
            new DoubleMaxAggregatorFactory("singleDoubleMax", singleValue),
            new DoubleMaxAggregatorFactory("multiDoubleMax", multiValue),
            new DoubleMinAggregatorFactory("singleDoubleMin", singleValue),
            new DoubleMinAggregatorFactory("multiDoubleMin", multiValue),

            new FloatSumAggregatorFactory("singleFloatSum", singleValue),
            new FloatSumAggregatorFactory("multiFloatSum", multiValue),
            new FloatMaxAggregatorFactory("singleFloatMax", singleValue),
            new FloatMaxAggregatorFactory("multiFloatMax", multiValue),
            new FloatMinAggregatorFactory("singleFloatMin", singleValue),
            new FloatMinAggregatorFactory("multiFloatMin", multiValue),

            new LongSumAggregatorFactory("singleLongSum", singleValue),
            new LongSumAggregatorFactory("multiLongSum", multiValue),
            new LongMaxAggregatorFactory("singleLongMax", singleValue),
            new LongMaxAggregatorFactory("multiLongMax", multiValue),
            new LongMinAggregatorFactory("singleLongMin", singleValue),
            new LongMinAggregatorFactory("multiLongMin", multiValue),

            new LongSumAggregatorFactory("count", "count")
        )
        .build();

    Sequence<ResultRow> seq = aggregationTestHelper.runQueryOnSegmentsObjs(segments, query);
    Row result = Iterables.getOnlyElement(seq.toList()).toMapBasedRow(query);

    Assert.assertEquals(numRows, result.getMetric("count").longValue());
    
    Assert.assertEquals(singleValueSum, result.getMetric("singleDoubleSum").doubleValue(), 0.0001d);
    Assert.assertEquals(multiValueSum, result.getMetric("multiDoubleSum").doubleValue(), 0.0001d);
    Assert.assertEquals(singleValueMax, result.getMetric("singleDoubleMax").doubleValue(), 0.0001d);
    Assert.assertEquals(multiValueMax, result.getMetric("multiDoubleMax").doubleValue(), 0.0001d);
    Assert.assertEquals(singleValueMin, result.getMetric("singleDoubleMin").doubleValue(), 0.0001d);
    Assert.assertEquals(multiValueMin, result.getMetric("multiDoubleMin").doubleValue(), 0.0001d);

    Assert.assertEquals(singleValueSum, result.getMetric("singleFloatSum").floatValue(), 0.0001f);
    Assert.assertEquals(multiValueSum, result.getMetric("multiFloatSum").floatValue(), 0.0001f);
    Assert.assertEquals(singleValueMax, result.getMetric("singleFloatMax").floatValue(), 0.0001f);
    Assert.assertEquals(multiValueMax, result.getMetric("multiFloatMax").floatValue(), 0.0001f);
    Assert.assertEquals(singleValueMin, result.getMetric("singleFloatMin").floatValue(), 0.0001f);
    Assert.assertEquals(multiValueMin, result.getMetric("multiFloatMin").floatValue(), 0.0001f);

    Assert.assertEquals((long) singleValueSum, result.getMetric("singleLongSum").longValue());
    Assert.assertEquals((long) multiValueSum, result.getMetric("multiLongSum").longValue());
    Assert.assertEquals((long) singleValueMax, result.getMetric("singleLongMax").longValue());
    Assert.assertEquals((long) multiValueMax, result.getMetric("multiLongMax").longValue());
    Assert.assertEquals((long) singleValueMin, result.getMetric("singleLongMin").longValue());
    Assert.assertEquals((long) multiValueMin, result.getMetric("multiLongMin").longValue());
  }

  @Test
  public void testTimeseries()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity(Granularities.ALL)
                                  .intervals("1970/2050")
                                  .aggregators(
                                      new DoubleSumAggregatorFactory("singleDoubleSum", singleValue),
                                      new DoubleSumAggregatorFactory("multiDoubleSum", multiValue),
                                      new DoubleMaxAggregatorFactory("singleDoubleMax", singleValue),
                                      new DoubleMaxAggregatorFactory("multiDoubleMax", multiValue),
                                      new DoubleMinAggregatorFactory("singleDoubleMin", singleValue),
                                      new DoubleMinAggregatorFactory("multiDoubleMin", multiValue),

                                      new FloatSumAggregatorFactory("singleFloatSum", singleValue),
                                      new FloatSumAggregatorFactory("multiFloatSum", multiValue),
                                      new FloatMaxAggregatorFactory("singleFloatMax", singleValue),
                                      new FloatMaxAggregatorFactory("multiFloatMax", multiValue),
                                      new FloatMinAggregatorFactory("singleFloatMin", singleValue),
                                      new FloatMinAggregatorFactory("multiFloatMin", multiValue),

                                      new LongSumAggregatorFactory("singleLongSum", singleValue),
                                      new LongSumAggregatorFactory("multiLongSum", multiValue),
                                      new LongMaxAggregatorFactory("singleLongMax", singleValue),
                                      new LongMaxAggregatorFactory("multiLongMax", multiValue),
                                      new LongMinAggregatorFactory("singleLongMin", singleValue),
                                      new LongMinAggregatorFactory("multiLongMin", multiValue),
                                      
                                      new LongSumAggregatorFactory("count", "count")
                                  )
                                  .build();

    Sequence seq = AggregationTestHelper.createTimeseriesQueryAggregationTestHelper(Collections.emptyList(), tempFolder)
                                        .runQueryOnSegmentsObjs(segments, query);
    TimeseriesResultValue result = ((Result<TimeseriesResultValue>) Iterables.getOnlyElement(seq.toList())).getValue();

    Assert.assertEquals(numRows, result.getLongMetric("count").longValue());
    Assert.assertEquals(singleValueSum, result.getDoubleMetric("singleDoubleSum").doubleValue(), 0.0001d);
    Assert.assertEquals(multiValueSum, result.getDoubleMetric("multiDoubleSum").doubleValue(), 0.0001d);
    Assert.assertEquals(singleValueMax, result.getDoubleMetric("singleDoubleMax").doubleValue(), 0.0001d);
    Assert.assertEquals(multiValueMax, result.getDoubleMetric("multiDoubleMax").doubleValue(), 0.0001d);
    Assert.assertEquals(singleValueMin, result.getDoubleMetric("singleDoubleMin").doubleValue(), 0.0001d);
    Assert.assertEquals(multiValueMin, result.getDoubleMetric("multiDoubleMin").doubleValue(), 0.0001d);

    Assert.assertEquals(singleValueSum, result.getFloatMetric("singleFloatSum").floatValue(), 0.0001f);
    Assert.assertEquals(multiValueSum, result.getFloatMetric("multiFloatSum").floatValue(), 0.0001f);
    Assert.assertEquals(singleValueMax, result.getFloatMetric("singleFloatMax").floatValue(), 0.0001f);
    Assert.assertEquals(multiValueMax, result.getFloatMetric("multiFloatMax").floatValue(), 0.0001f);
    Assert.assertEquals(singleValueMin, result.getFloatMetric("singleFloatMin").floatValue(), 0.0001f);
    Assert.assertEquals(multiValueMin, result.getFloatMetric("multiFloatMin").floatValue(), 0.0001f);

    Assert.assertEquals((long) singleValueSum, result.getLongMetric("singleLongSum").longValue());
    Assert.assertEquals((long) multiValueSum, result.getLongMetric("multiLongSum").longValue());
    Assert.assertEquals((long) singleValueMax, result.getLongMetric("singleLongMax").longValue());
    Assert.assertEquals((long) multiValueMax, result.getLongMetric("multiLongMax").longValue());
    Assert.assertEquals((long) singleValueMin, result.getLongMetric("singleLongMin").longValue());
    Assert.assertEquals((long) multiValueMin, result.getLongMetric("multiLongMin").longValue());
  }
}

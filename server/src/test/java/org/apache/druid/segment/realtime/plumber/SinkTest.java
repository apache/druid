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

package org.apache.druid.segment.realtime.plumber;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.indexing.TuningConfigs;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.FireHydrant;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class SinkTest extends InitializedNullHandlingTest
{
  @Test
  public void testSwap() throws Exception
  {
    final DataSchema schema = new DataSchema(
        "test",
        new TimestampSpec(null, null, null),
        DimensionsSpec.EMPTY,
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularities.HOUR, Granularities.MINUTE, null),
        null
    );

    final Interval interval = Intervals.of("2013-01-01/2013-01-02");
    final String version = DateTimes.nowUtc().toString();
    RealtimeTuningConfig tuningConfig = new RealtimeTuningConfig(
        100,
        null,
        new Period("P1Y"),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        0,
        0,
        null,
        null,
        null,
        null,
        null
    );
    final Sink sink = new Sink(
        interval,
        schema,
        tuningConfig.getShardSpec(),
        version,
        tuningConfig.getMaxRowsInMemory(),
        TuningConfigs.getMaxBytesInMemoryOrDefault(tuningConfig.getMaxBytesInMemory()),
        tuningConfig.isReportParseExceptions(),
        tuningConfig.getDedupColumn()
    );

    sink.add(
        new InputRow()
        {
          @Override
          public List<String> getDimensions()
          {
            return new ArrayList<>();
          }

          @Override
          public long getTimestampFromEpoch()
          {
            return DateTimes.of("2013-01-01").getMillis();
          }

          @Override
          public DateTime getTimestamp()
          {
            return DateTimes.of("2013-01-01");
          }

          @Override
          public List<String> getDimension(String dimension)
          {
            return new ArrayList<>();
          }

          @Override
          public Number getMetric(String metric)
          {
            return 0;
          }

          @Override
          public Object getRaw(String dimension)
          {
            return null;
          }

          @Override
          public int compareTo(Row o)
          {
            return 0;
          }
        },
        false
    );

    FireHydrant currHydrant = sink.getCurrHydrant();
    Assert.assertEquals(Intervals.of("2013-01-01/PT1M"), currHydrant.getIndex().getInterval());


    FireHydrant swapHydrant = sink.swap();

    sink.add(
        new InputRow()
        {
          @Override
          public List<String> getDimensions()
          {
            return new ArrayList<>();
          }

          @Override
          public long getTimestampFromEpoch()
          {
            return DateTimes.of("2013-01-01").getMillis();
          }

          @Override
          public DateTime getTimestamp()
          {
            return DateTimes.of("2013-01-01");
          }

          @Override
          public List<String> getDimension(String dimension)
          {
            return new ArrayList<>();
          }

          @Override
          public Number getMetric(String metric)
          {
            return 0;
          }

          @Override
          public Object getRaw(String dimension)
          {
            return null;
          }

          @Override
          public int compareTo(Row o)
          {
            return 0;
          }
        },
        false
    );

    Assert.assertEquals(currHydrant, swapHydrant);
    Assert.assertNotSame(currHydrant, sink.getCurrHydrant());
    Assert.assertEquals(Intervals.of("2013-01-01/PT1M"), sink.getCurrHydrant().getIndex().getInterval());

    Assert.assertEquals(2, Iterators.size(sink.iterator()));
  }

  @Test
  public void testDedup() throws Exception
  {
    final DataSchema schema = new DataSchema(
        "test",
        new TimestampSpec(null, null, null),
        DimensionsSpec.EMPTY,
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularities.HOUR, Granularities.MINUTE, null),
        null
    );

    final Interval interval = Intervals.of("2013-01-01/2013-01-02");
    final String version = DateTimes.nowUtc().toString();
    RealtimeTuningConfig tuningConfig = new RealtimeTuningConfig(
        100,
        null,
        new Period("P1Y"),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        0,
        0,
        null,
        null,
        null,
        null,
        "dedupColumn"
    );
    final Sink sink = new Sink(
        interval,
        schema,
        tuningConfig.getShardSpec(),
        version,
        tuningConfig.getMaxRowsInMemory(),
        TuningConfigs.getMaxBytesInMemoryOrDefault(tuningConfig.getMaxBytesInMemory()),
        tuningConfig.isReportParseExceptions(),
        tuningConfig.getDedupColumn()
    );

    int rows = sink.add(new MapBasedInputRow(
        DateTimes.of("2013-01-01"),
        ImmutableList.of("field", "dedupColumn"),
        ImmutableMap.of("field1", "value1", "dedupColumn", "v1")
    ), false).getRowCount();
    Assert.assertTrue(rows > 0);

    // dedupColumn is null
    rows = sink.add(new MapBasedInputRow(
        DateTimes.of("2013-01-01"),
        ImmutableList.of("field", "dedupColumn"),
        ImmutableMap.of("field1", "value2")
    ), false).getRowCount();
    Assert.assertTrue(rows > 0);

    // dedupColumn is null
    rows = sink.add(new MapBasedInputRow(
        DateTimes.of("2013-01-01"),
        ImmutableList.of("field", "dedupColumn"),
        ImmutableMap.of("field1", "value3")
    ), false).getRowCount();
    Assert.assertTrue(rows > 0);

    rows = sink.add(new MapBasedInputRow(
        DateTimes.of("2013-01-01"),
        ImmutableList.of("field", "dedupColumn"),
        ImmutableMap.of("field1", "value4", "dedupColumn", "v2")
    ), false).getRowCount();
    Assert.assertTrue(rows > 0);

    rows = sink.add(new MapBasedInputRow(
        DateTimes.of("2013-01-01"),
        ImmutableList.of("field", "dedupColumn"),
        ImmutableMap.of("field1", "value5", "dedupColumn", "v1")
    ), false).getRowCount();
    Assert.assertTrue(rows == -2);
  }
}

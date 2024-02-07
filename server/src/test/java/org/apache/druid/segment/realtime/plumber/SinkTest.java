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
import com.google.common.collect.Maps;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.FireHydrant;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 *
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
        null,
        100,
        null,
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
        0,
        0,
        null,
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
        tuningConfig.getAppendableIndexSpec(),
        tuningConfig.getMaxRowsInMemory(),
        tuningConfig.getMaxBytesInMemoryOrDefault(),
        true,
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
        null,
        100,
        null,
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
        0,
        0,
        null,
        null,
        null,
        null,
        "dedupColumn",
        null
    );
    final Sink sink = new Sink(
        interval,
        schema,
        tuningConfig.getShardSpec(),
        version,
        tuningConfig.getAppendableIndexSpec(),
        tuningConfig.getMaxRowsInMemory(),
        tuningConfig.getMaxBytesInMemoryOrDefault(),
        true,
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

  @Test
  public void testAcquireSegmentReferences_empty()
  {
    Assert.assertEquals(
        Collections.emptyList(),
        Sink.acquireSegmentReferences(Collections.emptyList(), Function.identity(), false)
    );
  }

  @Test
  public void testAcquireSegmentReferences_two() throws IOException
  {
    final List<FireHydrant> hydrants = twoHydrants();
    final List<SinkSegmentReference> references = Sink.acquireSegmentReferences(hydrants, Function.identity(), false);
    Assert.assertNotNull(references);
    Assert.assertEquals(2, references.size());
    Assert.assertEquals(0, references.get(0).getHydrantNumber());
    Assert.assertFalse(references.get(0).isImmutable());
    Assert.assertEquals(1, references.get(1).getHydrantNumber());
    Assert.assertTrue(references.get(1).isImmutable());
    CloseableUtils.closeAll(references);
  }

  @Test
  public void testAcquireSegmentReferences_two_skipIncremental() throws IOException
  {
    final List<FireHydrant> hydrants = twoHydrants();
    final List<SinkSegmentReference> references = Sink.acquireSegmentReferences(hydrants, Function.identity(), true);
    Assert.assertNotNull(references);
    Assert.assertEquals(1, references.size());
    Assert.assertEquals(1, references.get(0).getHydrantNumber());
    Assert.assertTrue(references.get(0).isImmutable());
    CloseableUtils.closeAll(references);
  }

  @Test
  public void testAcquireSegmentReferences_twoWithOneSwappedToNull()
  {
    // One segment has been swapped out. (Happens when sinks are being closed.)
    final List<FireHydrant> hydrants = twoHydrants();
    hydrants.get(1).swapSegment(null);

    final List<SinkSegmentReference> references = Sink.acquireSegmentReferences(hydrants, Function.identity(), false);
    Assert.assertNull(references);
  }

  @Test
  public void testGetSinkSignature() throws IndexSizeExceededException
  {
    final DataSchema schema = new DataSchema(
        "test",
        new TimestampSpec(null, null, null),
        new DimensionsSpec(
            Arrays.asList(
                new StringDimensionSchema("dim1"),
                new LongDimensionSchema("dimLong")
            )),
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularities.HOUR, Granularities.MINUTE, null),
        null
    );

    final Interval interval = Intervals.of("2013-01-01/2013-01-02");
    final String version = DateTimes.nowUtc().toString();
    RealtimeTuningConfig tuningConfig = new RealtimeTuningConfig(
        null,
        2,
        null,
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
        0,
        0,
        null,
        null,
        null,
        null,
        "dedupColumn",
        null
    );
    final Sink sink = new Sink(
        interval,
        schema,
        tuningConfig.getShardSpec(),
        version,
        tuningConfig.getAppendableIndexSpec(),
        tuningConfig.getMaxRowsInMemory(),
        tuningConfig.getMaxBytesInMemoryOrDefault(),
        true,
        tuningConfig.getDedupColumn()
    );

    sink.add(new MapBasedInputRow(
        DateTimes.of("2013-01-01"),
        ImmutableList.of("dim1", "dimLong"),
        ImmutableMap.of("dim1", "value1", "dimLong", "20")
    ), false);

    Map<String, ColumnType> expectedColumnTypeMap = Maps.newLinkedHashMap();
    expectedColumnTypeMap.put("__time", ColumnType.LONG);
    expectedColumnTypeMap.put("dim1", ColumnType.STRING);
    expectedColumnTypeMap.put("dimLong", ColumnType.LONG);
    expectedColumnTypeMap.put("rows", ColumnType.LONG);

    RowSignature signature = sink.getSignature();
    Assert.assertEquals(toRowSignature(expectedColumnTypeMap), signature);

    sink.add(new MapBasedInputRow(
        DateTimes.of("2013-01-01"),
        ImmutableList.of("dim1", "dimLong", "newCol1"),
        ImmutableMap.of("dim1", "value2", "dimLong", "30", "newCol1", "value")
    ), false);

    expectedColumnTypeMap.remove("rows");
    expectedColumnTypeMap.put("newCol1", ColumnType.STRING);
    expectedColumnTypeMap.put("rows", ColumnType.LONG);
    signature = sink.getSignature();
    Assert.assertEquals(toRowSignature(expectedColumnTypeMap), signature);

    sink.swap();

    sink.add(new MapBasedInputRow(
        DateTimes.of("2013-01-01"),
        ImmutableList.of("dim1", "dimLong", "newCol2"),
        ImmutableMap.of("dim1", "value3", "dimLong", "30", "newCol2", "value")
    ), false);

    expectedColumnTypeMap.put("newCol2", ColumnType.STRING);
    signature = sink.getSignature();
    Assert.assertEquals(toRowSignature(expectedColumnTypeMap), signature);

    sink.add(new MapBasedInputRow(
        DateTimes.of("2013-01-01"),
        ImmutableList.of("dim1", "dimLong", "newCol3"),
        ImmutableMap.of("dim1", "value3", "dimLong", "30", "newCol3", "value")
    ), false);

    expectedColumnTypeMap.put("newCol3", ColumnType.STRING);
    signature = sink.getSignature();
    Assert.assertEquals(toRowSignature(expectedColumnTypeMap), signature);
    sink.swap();

    sink.add(new MapBasedInputRow(
        DateTimes.of("2013-01-01"),
        ImmutableList.of("dim1", "dimLong", "newCol4"),
        ImmutableMap.of("dim1", "value3", "dimLong", "30", "newCol4", "value")
    ), false);

    expectedColumnTypeMap.put("newCol4", ColumnType.STRING);
    signature = sink.getSignature();
    Assert.assertEquals(toRowSignature(expectedColumnTypeMap), signature);
  }

  private RowSignature toRowSignature(Map<String, ColumnType> columnTypeMap)
  {
    RowSignature.Builder builder = RowSignature.builder();

    for (Map.Entry<String, ColumnType> entry : columnTypeMap.entrySet()) {
      builder.add(entry.getKey(), entry.getValue());
    }

    return builder.build();
  }

  /**
   * Generate one in-memory hydrant, one not-in-memory hydrant.
   */
  private static List<FireHydrant> twoHydrants()
  {
    final SegmentId segmentId = SegmentId.dummy("foo");
    return Arrays.asList(
        new FireHydrant(EasyMock.createMock(IncrementalIndex.class), 0, segmentId),
        new FireHydrant(
            new RowBasedSegment<>(
                segmentId,
                Sequences.empty(),
                RowAdapters.standardRow(),
                RowSignature.empty()
            ),
            1
        )
    );
  }
}

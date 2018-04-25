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

package io.druid.segment.incremental;

import com.google.common.collect.Maps;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.filter.SelectorDimFilter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import static io.druid.segment.incremental.IncrementalIndex.TimeAndDims;
import java.util.function.Consumer;

public class OffheapOakIncrementalIndexTest
{

  private static final Logger log = new Logger(OffheapOakIncrementalIndexTest.class);

  @Test
  public void testOffHeapOakIncrementalIndexBasics() throws Exception
  {
    OffheapOakIncrementalIndex index = getIndex();
    MapBasedInputRow[] rows = new MapBasedInputRow[6];
    long minTime = System.currentTimeMillis() - 1000 * rows.length;

    // creating rows
    rows[0] = toMapRow(minTime);
    rows[1] = toMapRow(minTime + 1000, "StringA", "A");
    rows[2] = toMapRow(minTime + 1000, "StringA", "B");
    rows[3] = toMapRow(minTime + 4000, "StringA", "A", "StringB", "B", "StringC", "C", "StringD", "D");
    rows[4] = toMapRow(minTime + 2000, "StringA", "A", "StringB", "B");
    rows[5] = toMapRow(minTime + 3000, "StringD", "D");

    for (int i = 0; i < rows.length; i++) {
      for (int j = 0; j < 5; j++) {
        index.add(rows[i]);
      }
    }

    Assert.assertEquals(index.size(), rows.length);
    Assert.assertEquals(index.getMinTimeMillis(), minTime);
    Assert.assertEquals(index.getMaxTimeMillis(), minTime + 4000);
  }

  @Test
  public void testOffHeapOakIncrementalIndexNoSchema() throws Exception
  {
    IncrementalIndex index = new OffheapOakIncrementalIndex.Builder()
            .setSimpleTestingIndexSchema(new CountAggregatorFactory("cnt"))
            .setMaxRowCount(1000)
            .buildOffheapOak();

    MapBasedInputRow[] rows = new MapBasedInputRow[6];
    long minTime = System.currentTimeMillis() - 1000 * rows.length;

    // creating rows
    rows[0] = toMapRow(minTime);
    rows[1] = toMapRow(minTime + 1000, "StringA", "A");
    rows[2] = toMapRow(minTime + 1000, "StringA", "B");
    rows[3] = toMapRow(minTime + 4000, "StringA", "A", "StringB", "B", "StringC", "C", "StringD", "D");
    rows[4] = toMapRow(minTime + 2000, "StringA", "A", "StringB", "B");
    rows[5] = toMapRow(minTime + 3000, "StringD", "D");

    for (int i = 0; i < rows.length; i++) {
      for (int j = 0; j < 5; j++) {
        index.add(rows[i]);
      }
    }

    Assert.assertEquals(index.size(), rows.length);
    Assert.assertEquals(index.getMinTimeMillis(), minTime);
    Assert.assertEquals(index.getMaxTimeMillis(), minTime + 4000);
    Assert.assertEquals(index.getDimensionNames().size(), 4);
  }

  @Test
  public void testOffHeapOakIncrementalIndexKeysIterator() throws Exception
  {
    OffheapOakIncrementalIndex index = getIndex();
    MapBasedInputRow[] rows = new MapBasedInputRow[10];

    long time = System.currentTimeMillis();

    // creating rows
    rows[0] = toMapRow(time - 5000);
    rows[1] = toMapRow(time - 3000);
    rows[2] = toMapRow(time - 4000, "StringA", "A");
    rows[3] = toMapRow(time - 3000, "StringA", "ABC", "StringB", "B");
    rows[4] = toMapRow(time - 3000, "StringD", "A", "StringC", "B");
    rows[5] = toMapRow(time - 2000, "StringD", "D");
    rows[6] = toMapRow(time - 1000, "StringA", "AA", "StringB", "BH", "StringC", "C", "StringD", "D");
    rows[7] = toMapRow(time - 4000, "StringC", "C");
    rows[8] = toMapRow(time - 1000, "StringA", "C", "StringB", "A", "StringC", "BGD", "StringD", "B");
    rows[9] = toMapRow(time - 3000, "StringD", "A", "StringB", "B");

    for (int j = 0; j < 5; j++) {
      for (int i = 0; i < rows.length; i++) {
        index.add(rows[i]);
      }
    }
    Assert.assertEquals(index.size(), rows.length);

    Iterable<TimeAndDims> keySet = index.keySet();
    Consumer<TimeAndDims> keySetConsumer = new Consumer<TimeAndDims>() {

      TimeAndDims prev = index.toTimeAndDims(toMapRow(time - 6000));

      @Override
      public void accept(TimeAndDims timeAndDims)
      {
        Assert.assertTrue(0 > index.dimsComparator().compare(prev, timeAndDims));
        prev = timeAndDims;
      }
    };

    keySet.forEach(keySetConsumer);
  }

  @Test
  public void testOffHeapOakIncrementalIndexKeysTimeRangeIterable() throws Exception
  {
    OffheapOakIncrementalIndex index = getIndex();
    MapBasedInputRow[] rows = new MapBasedInputRow[10];

    long time = System.currentTimeMillis();

    // creating rows
    rows[0] = toMapRow(time - 5000);
    rows[1] = toMapRow(time - 3000);
    rows[2] = toMapRow(time - 4000, "StringA", "A");
    rows[3] = toMapRow(time - 3000, "StringA", "ABC", "StringB", "B");
    rows[4] = toMapRow(time - 3000, "StringD", "A", "StringC", "B");
    rows[5] = toMapRow(time - 2000, "StringD", "D");
    rows[6] = toMapRow(time - 1000, "StringA", "AA", "StringB", "BH", "StringC", "C", "StringD", "D");
    rows[7] = toMapRow(time - 3000, "StringB", "ABC", "StringA", "B");
    rows[8] = toMapRow(time - 1000, "StringA", "C", "StringB", "A", "StringC", "BGD", "StringD", "B");
    rows[9] = toMapRow(time - 3000, "StringD", "A", "StringB", "B");

    for (int j = 0; j < 5; j++) {
      for (int i = 0; i < rows.length; i++) {
        index.add(rows[i]);
      }
    }

    Iterable<TimeAndDims> timeRangeIterable;
    Consumer<TimeAndDims> timeRangeConsumer;

    // An ascending iterator
    timeRangeIterable = index.timeRangeIterable(false, time - 5000, time - 2000);
    timeRangeConsumer = new Consumer<TimeAndDims>() {

      TimeAndDims prev = index.toTimeAndDims(toMapRow(time - 6000));

      @Override
      public void accept(TimeAndDims timeAndDims)
      {
        Assert.assertTrue(0 > index.dimsComparator().compare(prev, timeAndDims));
        Assert.assertTrue(time - 1999 > timeAndDims.getTimestamp());
        Assert.assertTrue(time - 5001 < timeAndDims.getTimestamp());
        prev = timeAndDims;
      }
    };

    timeRangeIterable.forEach(timeRangeConsumer);

    // A descending iterator
    timeRangeIterable = index.timeRangeIterable(true, time - 5000, time - 2000);
    timeRangeConsumer = new Consumer<TimeAndDims>() {

      TimeAndDims prev = index.toTimeAndDims(toMapRow(time - 1000));

      @Override
      public void accept(TimeAndDims timeAndDims)
      {
        Assert.assertTrue(0 < index.dimsComparator().compare(prev, timeAndDims));
        Assert.assertTrue(time - 1999 > timeAndDims.getTimestamp());
        Assert.assertTrue(time - 5001 < timeAndDims.getTimestamp());
        prev = timeAndDims;
      }
    };

    timeRangeIterable.forEach(timeRangeConsumer);

  }

  @Test
  public void testOffHeapOakIncrementalIndexAggs() throws Exception
  {
    OffheapOakIncrementalIndex index = getIndex();
    MapBasedInputRow[] rows = new MapBasedInputRow[10];
    int insertionTrials = 5;

    long time = System.currentTimeMillis();

    // creating rows
    rows[0] = toMapRow(time - 5000);
    rows[1] = toMapRow(time - 3000);
    rows[2] = toMapRow(time - 4000, "StringA", "A");
    rows[3] = toMapRow(time - 3000, "StringA", "ABC", "StringB", "B");
    rows[4] = toMapRow(time - 3000, "StringD", "A", "StringC", "B");
    rows[5] = toMapRow(time - 2000, "StringD", "D");
    rows[6] = toMapRow(time - 1000, "StringA", "AA", "StringB", "BH", "StringC", "C", "StringD", "D");
    rows[7] = toMapRow(time - 3000, "StringB", "ABC", "StringA", "B");
    rows[8] = toMapRow(time - 1000, "StringA", "C", "StringB", "A", "StringC", "BGD", "StringD", "B");
    rows[9] = toMapRow(time - 3000, "StringD", "A", "StringB", "B");

    for (int j = 0; j < insertionTrials; j++) {
      for (int i = 0; i < rows.length; i++) {
        index.add(rows[i]);
      }
    }

    Iterable<Row> iterable = index.iterableWithPostAggregations(null, false);
    Consumer<Row> rowConsumer = new Consumer<Row>() {

      @Override
      public void accept(Row row)
      {
        // insertion trials counters
        long count = Long.valueOf(row.getDimension("Count").get(0));
        long countA = Long.valueOf(row.getDimension("CountStringA=A").get(0));
        long countB = Long.valueOf(row.getDimension("CountStringB=B").get(0));
        long countC = Long.valueOf(row.getDimension("CountStringC=C").get(0));
        long countD = Long.valueOf(row.getDimension("CountStringD=D").get(0));

        Assert.assertEquals(insertionTrials, count);
        Assert.assertEquals(countA, row.getDimension("StringA").size() > 0 && row.getDimension("StringA").get(0) == "A" ? insertionTrials : 0);
        Assert.assertEquals(countB, row.getDimension("StringB").size() > 0 && row.getDimension("StringB").get(0) == "B" ? insertionTrials : 0);
        Assert.assertEquals(countC, row.getDimension("StringC").size() > 0 && row.getDimension("StringC").get(0) == "C" ? insertionTrials : 0);
        Assert.assertEquals(countD, row.getDimension("StringD").size() > 0 && row.getDimension("StringD").get(0) == "D" ? insertionTrials : 0);
      }
    };

    iterable.forEach(rowConsumer);
  }

  private OffheapOakIncrementalIndex getIndex()
  {
    DimensionsSpec dimensions = new DimensionsSpec(
        Arrays.<DimensionSchema>asList(
          new StringDimensionSchema("StringA"),
          new StringDimensionSchema("StringB"),
          new StringDimensionSchema("StringC"),
          new StringDimensionSchema("StringD")
        ), null, null
    );

    AggregatorFactory[] metrics = {
        new CountAggregatorFactory("Count"),
        new FilteredAggregatorFactory(
          new CountAggregatorFactory("CountStringA=A"),
          new SelectorDimFilter("StringA", "A", null)
        ),
        new FilteredAggregatorFactory(
          new CountAggregatorFactory("CountStringB=B"),
          new SelectorDimFilter("StringB", "B", null)
        ),
        new FilteredAggregatorFactory(
          new CountAggregatorFactory("CountStringC=C"),
          new SelectorDimFilter("StringC", "C", null)
        ),
        new FilteredAggregatorFactory(
          new CountAggregatorFactory("CountStringD=D"),
          new SelectorDimFilter("StringD", "D", null)
        )
    };

    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(dimensions)
        .withMetrics(metrics)
        .build();

    OffheapOakIncrementalIndex index = (OffheapOakIncrementalIndex) new IncrementalIndex.Builder()
            .setIndexSchema(schema)
            .setDeserializeComplexMetrics(false)
            .setMaxRowCount(1000)
            .buildOffheapOak();

    return index;
  }

  private MapBasedInputRow toMapRow(long time, Object... dimAndVal)
  {
    Map<String, Object> data = Maps.newHashMap();
    List<String> dims = new ArrayList<>();
    for (int i = 0; i < dimAndVal.length; i += 2) {
      data.put((String) dimAndVal[i], dimAndVal[i + 1]);
      dims.add((String) dimAndVal[i]);
    }

    return new MapBasedInputRow(time, dims, data);
  }
}

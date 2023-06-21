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

package org.apache.druid.query.aggregation.last;

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongDouble;
import org.apache.druid.query.aggregation.TestDoubleColumnSelectorImpl;
import org.apache.druid.query.aggregation.TestLongColumnSelector;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class DoubleLastAggregationTest extends InitializedNullHandlingTest
{
  private DoubleLastAggregatorFactory doubleLastAggFactory;
  private DoubleLastAggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestLongColumnSelector timeSelector;
  private TestLongColumnSelector customTimeSelector;
  private TestDoubleColumnSelectorImpl valueSelector;
  private TestObjectColumnSelector objectSelector;

  private double[] doubles = {1.1897d, 0.001d, 86.23d, 166.228d};
  private long[] times = {8224, 6879, 2436, 7888};
  private long[] customTimes = {1, 4, 3, 2};
  private SerializablePairLongDouble[] pairs = {
      new SerializablePairLongDouble(52782L, 134.3d),
      new SerializablePairLongDouble(65492L, 1232.212d),
      new SerializablePairLongDouble(69134L, 18.1233d),
      new SerializablePairLongDouble(11111L, 233.5232d)
  };

  @Before
  public void setup()
  {
    doubleLastAggFactory = new DoubleLastAggregatorFactory("billy", "nilly", null);
    combiningAggFactory = (DoubleLastAggregatorFactory) doubleLastAggFactory.getCombiningFactory();
    timeSelector = new TestLongColumnSelector(times);
    customTimeSelector = new TestLongColumnSelector(customTimes);
    valueSelector = new TestDoubleColumnSelectorImpl(doubles);
    objectSelector = new TestObjectColumnSelector<>(pairs);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME)).andReturn(timeSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("customTime")).andReturn(customTimeSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("billy")).andReturn(objectSelector);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.DOUBLE));
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("billy")).andReturn(null);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testDoubleLastAggregator()
  {
    Aggregator agg = doubleLastAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get();

    Assert.assertEquals(times[0], result.lhs.longValue());
    Assert.assertEquals(doubles[0], result.rhs, 0.0001);
    Assert.assertEquals((long) doubles[0], agg.getLong());
    Assert.assertEquals(doubles[0], agg.getDouble(), 0.0001);
  }

  @Test
  public void testDoubleLastAggregatorWithTimeColumn()
  {
    Aggregator agg = new DoubleLastAggregatorFactory("billy", "nilly", "customTime").factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get();

    Assert.assertEquals(customTimes[1], result.lhs.longValue());
    Assert.assertEquals(doubles[1], result.rhs, 0.0001);
    Assert.assertEquals((long) doubles[1], agg.getLong());
    Assert.assertEquals(doubles[1], agg.getDouble(), 0.0001);
  }

  @Test
  public void testDoubleLastBufferAggregator()
  {
    BufferAggregator agg = doubleLastAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[doubleLastAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get(buffer, 0);

    Assert.assertEquals(times[0], result.lhs.longValue());
    Assert.assertEquals(doubles[0], result.rhs, 0.0001);
    Assert.assertEquals((long) doubles[0], agg.getLong(buffer, 0));
    Assert.assertEquals(doubles[0], agg.getDouble(buffer, 0), 0.0001);
  }

  @Test
  public void testDoubleLastBufferAggregatorWithTimeColumn()
  {
    BufferAggregator agg = new DoubleLastAggregatorFactory("billy", "nilly", "customTime").factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[doubleLastAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get(buffer, 0);

    Assert.assertEquals(customTimes[1], result.lhs.longValue());
    Assert.assertEquals(doubles[1], result.rhs, 0.0001);
    Assert.assertEquals((long) doubles[1], agg.getLong(buffer, 0));
    Assert.assertEquals(doubles[1], agg.getDouble(buffer, 0), 0.0001);
  }

  @Test
  public void testCombine()
  {
    SerializablePairLongDouble pair1 = new SerializablePairLongDouble(1467225000L, 3.621);
    SerializablePairLongDouble pair2 = new SerializablePairLongDouble(1467240000L, 785.4);
    Assert.assertEquals(pair2, doubleLastAggFactory.combine(pair1, pair2));
  }

  @Test
  public void testComparatorWithNulls()
  {
    SerializablePairLongDouble pair1 = new SerializablePairLongDouble(1467225000L, 3.621);
    SerializablePairLongDouble pair2 = new SerializablePairLongDouble(1467240000L, null);
    Comparator comparator = doubleLastAggFactory.getComparator();
    Assert.assertEquals(1, comparator.compare(pair1, pair2));
    Assert.assertEquals(0, comparator.compare(pair1, pair1));
    Assert.assertEquals(0, comparator.compare(pair2, pair2));
    Assert.assertEquals(-1, comparator.compare(pair2, pair1));
  }

  @Test
  public void testDoubleLastCombiningAggregator()
  {
    Aggregator agg = combiningAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get();
    Pair<Long, Double> expected = (Pair<Long, Double>) pairs[2];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs, 0.0001);
    Assert.assertEquals(expected.rhs.longValue(), agg.getLong());
    Assert.assertEquals(expected.rhs, agg.getDouble(), 0.0001);
  }

  @Test
  public void testDoubleLastCombiningBufferAggregator()
  {
    BufferAggregator agg = combiningAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[doubleLastAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get(buffer, 0);
    Pair<Long, Double> expected = (Pair<Long, Double>) pairs[2];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs, 0.0001);
    Assert.assertEquals(expected.rhs.longValue(), agg.getLong(buffer, 0));
    Assert.assertEquals(expected.rhs, agg.getDouble(buffer, 0), 0.0001);
  }


  @Test
  public void testSerde() throws Exception
  {
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    String doubleSpecJson = "{\"type\":\"doubleLast\",\"name\":\"billy\",\"fieldName\":\"nilly\"}";
    AggregatorFactory deserialized = mapper.readValue(doubleSpecJson, AggregatorFactory.class);
    Assert.assertEquals(doubleLastAggFactory, deserialized);
    Assert.assertArrayEquals(doubleLastAggFactory.getCacheKey(), deserialized.getCacheKey());
  }

  @Test
  public void testDoubleLastAggregateCombiner()
  {
    TestObjectColumnSelector columnSelector = new TestObjectColumnSelector<>(pairs);
    AggregateCombiner doubleLastAggregateCombiner = combiningAggFactory.makeAggregateCombiner();
    doubleLastAggregateCombiner.reset(columnSelector);

    Assert.assertEquals(pairs[0], doubleLastAggregateCombiner.getObject());

    columnSelector.increment();
    doubleLastAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(pairs[1], doubleLastAggregateCombiner.getObject());

    columnSelector.increment();
    doubleLastAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(pairs[2], doubleLastAggregateCombiner.getObject());

    columnSelector.increment();
    doubleLastAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(pairs[2], doubleLastAggregateCombiner.getObject());

    doubleLastAggregateCombiner.reset(columnSelector);
    Assert.assertEquals(pairs[3], doubleLastAggregateCombiner.getObject());
  }


  private void aggregate(
      Aggregator agg
  )
  {
    agg.aggregate();
    timeSelector.increment();
    customTimeSelector.increment();
    valueSelector.increment();
    objectSelector.increment();
  }

  private void aggregate(
      BufferAggregator agg,
      ByteBuffer buff,
      int position
  )
  {
    agg.aggregate(buff, position);
    timeSelector.increment();
    customTimeSelector.increment();
    valueSelector.increment();
    objectSelector.increment();
  }
}

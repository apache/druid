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

package org.apache.druid.query.aggregation.first;

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

public class DoubleFirstAggregationTest extends InitializedNullHandlingTest
{
  private DoubleFirstAggregatorFactory doubleFirstAggFactory;
  private DoubleFirstAggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestLongColumnSelector timeSelector;
  private TestLongColumnSelector customTimeSelector;
  private TestDoubleColumnSelectorImpl valueSelector;
  private TestObjectColumnSelector objectSelector;

  private double[] doubleValues = {1.1d, 2.7d, 3.5d, 1.3d};
  private long[] times = {12, 10, 5344, 7899999};
  private long[] customTimes = {2, 1, 3, 4};
  private SerializablePairLongDouble[] pairs = {
      new SerializablePairLongDouble(1467225096L, 134.3d),
      new SerializablePairLongDouble(23163L, 1232.212d),
      new SerializablePairLongDouble(742L, 18d),
      new SerializablePairLongDouble(111111L, 233.5232d)
  };

  @Before
  public void setup()
  {
    doubleFirstAggFactory = new DoubleFirstAggregatorFactory("billy", "nilly", null);
    combiningAggFactory = (DoubleFirstAggregatorFactory) doubleFirstAggFactory.getCombiningFactory();
    timeSelector = new TestLongColumnSelector(times);
    customTimeSelector = new TestLongColumnSelector(customTimes);
    valueSelector = new TestDoubleColumnSelectorImpl(doubleValues);
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
  public void testDoubleFirstAggregator()
  {
    Aggregator agg = doubleFirstAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get();

    Assert.assertEquals(times[1], result.lhs.longValue());
    Assert.assertEquals(doubleValues[1], result.rhs, 0.0001);
    Assert.assertEquals((long) doubleValues[1], agg.getLong());
    Assert.assertEquals(doubleValues[1], agg.getDouble(), 0.0001);
  }

  @Test
  public void testDoubleFirstAggregatorWithTimeColumn()
  {
    Aggregator agg = new DoubleFirstAggregatorFactory("billy", "nilly", "customTime").factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get();

    Assert.assertEquals(customTimes[1], result.lhs.longValue());
    Assert.assertEquals(doubleValues[1], result.rhs, 0.0001);
    Assert.assertEquals((long) doubleValues[1], agg.getLong());
    Assert.assertEquals(doubleValues[1], agg.getDouble(), 0.0001);
  }

  @Test
  public void testDoubleFirstBufferAggregator()
  {
    BufferAggregator agg = doubleFirstAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[doubleFirstAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get(buffer, 0);

    Assert.assertEquals(times[1], result.lhs.longValue());
    Assert.assertEquals(doubleValues[1], result.rhs, 0.0001);
    Assert.assertEquals((long) doubleValues[1], agg.getLong(buffer, 0));
    Assert.assertEquals(doubleValues[1], agg.getDouble(buffer, 0), 0.0001);
  }

  @Test
  public void testDoubleFirstBufferAggregatorWithTimeColumn()
  {
    BufferAggregator agg = new DoubleFirstAggregatorFactory("billy", "nilly", "customTime").factorizeBuffered(colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[doubleFirstAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get(buffer, 0);

    Assert.assertEquals(customTimes[1], result.lhs.longValue());
    Assert.assertEquals(doubleValues[1], result.rhs, 0.0001);
    Assert.assertEquals((long) doubleValues[1], agg.getLong(buffer, 0));
    Assert.assertEquals(doubleValues[1], agg.getDouble(buffer, 0), 0.0001);
  }

  @Test
  public void testCombine()
  {
    SerializablePairLongDouble pair1 = new SerializablePairLongDouble(1467225000L, 3.621);
    SerializablePairLongDouble pair2 = new SerializablePairLongDouble(1467240000L, 785.4);
    Assert.assertEquals(pair1, doubleFirstAggFactory.combine(pair1, pair2));
  }

  @Test
  public void testComparator()
  {
    SerializablePairLongDouble pair1 = new SerializablePairLongDouble(1467225000L, 3.621);
    SerializablePairLongDouble pair2 = new SerializablePairLongDouble(1467240000L, 785.4);
    Comparator comparator = doubleFirstAggFactory.getComparator();
    Assert.assertEquals(-1, comparator.compare(pair1, pair2));
    Assert.assertEquals(0, comparator.compare(pair1, pair1));
    Assert.assertEquals(0, comparator.compare(pair2, pair2));
    Assert.assertEquals(1, comparator.compare(pair2, pair1));
  }

  @Test
  public void testComparatorWithNulls()
  {
    SerializablePairLongDouble pair1 = new SerializablePairLongDouble(1467225000L, 3.621);
    SerializablePairLongDouble pair2 = new SerializablePairLongDouble(1467240000L, null);
    Comparator comparator = doubleFirstAggFactory.getComparator();
    Assert.assertEquals(1, comparator.compare(pair1, pair2));
    Assert.assertEquals(0, comparator.compare(pair1, pair1));
    Assert.assertEquals(0, comparator.compare(pair2, pair2));
    Assert.assertEquals(-1, comparator.compare(pair2, pair1));
  }


  @Test
  public void testDoubleFirstCombiningAggregator()
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
  public void testDoubleFirstCombiningBufferAggregator()
  {
    BufferAggregator agg = combiningAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[doubleFirstAggFactory.getMaxIntermediateSizeWithNulls()]);
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
    String doubleSpecJson = "{\"type\":\"doubleFirst\",\"name\":\"billy\",\"fieldName\":\"nilly\"}";
    AggregatorFactory deserialized = mapper.readValue(doubleSpecJson, AggregatorFactory.class);
    Assert.assertEquals(doubleFirstAggFactory, deserialized);
    Assert.assertArrayEquals(doubleFirstAggFactory.getCacheKey(), deserialized.getCacheKey());
  }

  @Test
  public void testDoubleFirstAggregateCombiner()
  {
    TestObjectColumnSelector columnSelector = new TestObjectColumnSelector<>(pairs);
    AggregateCombiner doubleFirstAggregateCombiner = combiningAggFactory.makeAggregateCombiner();
    doubleFirstAggregateCombiner.reset(columnSelector);

    Assert.assertEquals(pairs[0], doubleFirstAggregateCombiner.getObject());

    columnSelector.increment();
    doubleFirstAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(pairs[1], doubleFirstAggregateCombiner.getObject());

    columnSelector.increment();
    doubleFirstAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(pairs[2], doubleFirstAggregateCombiner.getObject());

    doubleFirstAggregateCombiner.reset(columnSelector);
    Assert.assertEquals(pairs[2], doubleFirstAggregateCombiner.getObject());
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

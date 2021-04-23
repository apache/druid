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

import org.apache.druid.collections.SerializablePair;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.TestFloatColumnSelector;
import org.apache.druid.query.aggregation.TestLongColumnSelector;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class FloatFirstAggregationTest extends InitializedNullHandlingTest
{
  private FloatFirstAggregatorFactory floatFirstAggregatorFactory;
  private FloatFirstAggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestLongColumnSelector timeSelector;
  private TestFloatColumnSelector valueSelector;
  private TestObjectColumnSelector objectSelector;

  private float[] floats = {1.1f, 2.7f, 3.5f, 1.3f};
  private long[] times = {12, 10, 5344, 7899999};
  private SerializablePair[] pairs = {
      new SerializablePair<>(1567899920L, null),
      new SerializablePair<>(1467225096L, 134.3f),
      new SerializablePair<>(23163L, 1232.212f),
      new SerializablePair<>(742L, 18f),
      new SerializablePair<>(111111L, 233.5232f),
      new SerializablePair<>(500L, null),
      new SerializablePair<>(500L, 400.f)
  };

  @Before
  public void setup()
  {
    floatFirstAggregatorFactory = new FloatFirstAggregatorFactory("billy", "nilly");
    combiningAggFactory = (FloatFirstAggregatorFactory) floatFirstAggregatorFactory.getCombiningFactory();
    timeSelector = new TestLongColumnSelector(times);
    valueSelector = new TestFloatColumnSelector(floats);
    objectSelector = new TestObjectColumnSelector<>(pairs);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME)).andReturn(timeSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector).atLeastOnce();
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("billy")).andReturn(objectSelector).atLeastOnce();
  }

  /**
   * test aggregator on value selector column
   */
  @Test
  public void testDoubleFirstAggregator()
  {
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly")).andReturn(new ColumnCapabilitiesImpl().setType(
        ValueType.FLOAT));
    EasyMock.replay(colSelectorFactory);

    Aggregator agg = floatFirstAggregatorFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Float> result = (Pair<Long, Float>) agg.get();

    Assert.assertEquals(times[1], result.lhs.longValue());
    Assert.assertEquals(floats[1], result.rhs, 0.0001);
    Assert.assertEquals((long) floats[1], agg.getLong());
    Assert.assertEquals(floats[1], agg.getFloat(), 0.0001);
  }

  /**
   * test aggregator on value selector column
   */
  @Test
  public void testDoubleFirstBufferAggregator()
  {
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly")).andReturn(new ColumnCapabilitiesImpl().setType(
        ValueType.FLOAT));
    EasyMock.replay(colSelectorFactory);

    BufferAggregator agg = floatFirstAggregatorFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[floatFirstAggregatorFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Float> result = (Pair<Long, Float>) agg.get(buffer, 0);

    Assert.assertEquals(times[1], result.lhs.longValue());
    Assert.assertEquals(floats[1], result.rhs, 0.0001);
    Assert.assertEquals((long) floats[1], agg.getLong(buffer, 0));
    Assert.assertEquals(floats[1], agg.getFloat(buffer, 0), 0.0001);
  }

  @Test
  public void testCombine()
  {
    SerializablePair pair1 = new SerializablePair<>(1467225000L, 3.621f);
    SerializablePair pair2 = new SerializablePair<>(1467240000L, 785.4f);
    Assert.assertEquals(pair1, floatFirstAggregatorFactory.combine(pair1, pair2));
  }

  @Test
  public void testComparatorWithNulls()
  {
    SerializablePair pair1 = new SerializablePair<>(1467225000L, 3.621f);
    SerializablePair pair2 = new SerializablePair<>(1467240000L, null);
    Comparator comparator = floatFirstAggregatorFactory.getComparator();
    Assert.assertEquals(1, comparator.compare(pair1, pair2));
    Assert.assertEquals(0, comparator.compare(pair1, pair1));
    Assert.assertEquals(0, comparator.compare(pair2, pair2));
    Assert.assertEquals(-1, comparator.compare(pair2, pair1));
  }

  /**
   * test aggregator on an object selector column
   */
  @Test
  public void testDoubleFirstCombiningAggregator()
  {
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("billy")).andReturn(new ColumnCapabilitiesImpl().setType(
        ValueType.COMPLEX));
    EasyMock.replay(colSelectorFactory);

    Aggregator agg = combiningAggFactory.factorize(colSelectorFactory);

    //
    // aggregate first 5 events, pair[3] is supposed to be the first
    //
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    agg.aggregate();
    objectSelector.increment();

    Pair<Long, Float> result = (Pair<Long, Float>) agg.get();
    Pair<Long, Float> expected = (Pair<Long, Float>) pairs[3];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs, 0.0001);
    Assert.assertEquals(expected.rhs.longValue(), agg.getLong());
    Assert.assertEquals(expected.rhs, agg.getFloat(), 0.0001);

    //
    // aggregator once more, pair[5] will be the first
    //
    agg.aggregate();
    objectSelector.increment();
    result = (Pair<Long, Float>) agg.get();
    expected = (Pair<Long, Float>) pairs[5];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertNull(result.rhs);

    //
    // aggregate once more, pair[6] has the same timestamp as pair[5], but it won't be the first
    //
    agg.aggregate();
    objectSelector.increment();
    result = (Pair<Long, Float>) agg.get();
    expected = (Pair<Long, Float>) pairs[5];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertNull(result.rhs);
  }

  /**
   * test aggregator on an object column
   */
  @Test
  public void testDoubleFirstCombiningBufferAggregator()
  {
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("billy")).andReturn(new ColumnCapabilitiesImpl().setType(
        ValueType.COMPLEX));
    EasyMock.replay(colSelectorFactory);

    BufferAggregator agg = combiningAggFactory.factorizeBuffered(colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[floatFirstAggregatorFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    //
    // aggregate first 5 events, pair[3] is the first
    //
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Float> result = (Pair<Long, Float>) agg.get(buffer, 0);
    Pair<Long, Float> expected = (Pair<Long, Float>) pairs[3];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs, 0.0001);
    Assert.assertEquals(expected.rhs.longValue(), agg.getLong(buffer, 0));
    Assert.assertEquals(expected.rhs, agg.getFloat(buffer, 0), 0.0001);

    //
    // aggregate once more, pair[5] is the first
    //
    agg.aggregate(buffer, 0);
    objectSelector.increment();
    result = (Pair<Long, Float>) agg.get(buffer, 0);
    expected = (Pair<Long, Float>) pairs[5];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertNull(result.rhs);

    //
    // aggregate once more, pair[6] has the same timestamp as pair[5], but it won't be the first
    //
    agg.aggregate(buffer, 0);
    objectSelector.increment();
    result = (Pair<Long, Float>) agg.get(buffer, 0);
    expected = (Pair<Long, Float>) pairs[5];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertNull(result.rhs);
  }

  @Test
  public void testSerde() throws Exception
  {
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    String doubleSpecJson = "{\"type\":\"floatFirst\",\"name\":\"billy\",\"fieldName\":\"nilly\"}";
    Assert.assertEquals(floatFirstAggregatorFactory, mapper.readValue(doubleSpecJson, AggregatorFactory.class));
  }

  @Test
  public void testFloatFirstAggregateCombiner()
  {
    AggregateCombiner floatFirstAggregateCombiner = combiningAggFactory.makeAggregateCombiner();

    SerializablePair[] inputPairs = {
        new SerializablePair<>(6L, null),
        new SerializablePair<>(6L, 134.3f),
        new SerializablePair<>(5L, 1232.212f),
        new SerializablePair<>(4L, 18f),
        new SerializablePair<>(7L, 233.5232f),
        new SerializablePair<>(0L, null)
    };
    TestObjectColumnSelector columnSelector = new TestObjectColumnSelector<>(inputPairs);
    floatFirstAggregateCombiner.reset(columnSelector);
    Assert.assertEquals(inputPairs[0], floatFirstAggregateCombiner.getObject());

    // inputPairs[1].first > inputPair[0].first, it should NOT be the first
    columnSelector.increment();
    floatFirstAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(inputPairs[0], floatFirstAggregateCombiner.getObject());

    // inputPairs[2].first < inputPair[0].first, it should be the first
    columnSelector.increment();
    floatFirstAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(inputPairs[2], floatFirstAggregateCombiner.getObject());

    // inputPairs[3].first is the lowest, it should be the first
    columnSelector.increment();
    floatFirstAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(inputPairs[3], floatFirstAggregateCombiner.getObject());

    // inputPairs[4] is not the lowest, it should NOT be the first
    columnSelector.increment();
    floatFirstAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(inputPairs[3], floatFirstAggregateCombiner.getObject());

    columnSelector.increment();
    floatFirstAggregateCombiner.fold(columnSelector);

    Assert.assertEquals(inputPairs[5], floatFirstAggregateCombiner.getObject());
  }

  private void aggregate(
      Aggregator agg
  )
  {
    agg.aggregate();
    timeSelector.increment();
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
    valueSelector.increment();
    objectSelector.increment();
  }
}

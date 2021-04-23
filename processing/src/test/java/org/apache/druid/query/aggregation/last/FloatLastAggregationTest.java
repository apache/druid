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

public class FloatLastAggregationTest extends InitializedNullHandlingTest
{
  private FloatLastAggregatorFactory floatLastAggregatorFactory;
  private FloatLastAggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestLongColumnSelector timeSelector;
  private TestFloatColumnSelector valueSelector;
  private TestObjectColumnSelector objectSelector;

  private float[] floats = {1.1897f, 0.001f, 86.23f, 166.228f};
  private long[] times = {8224, 6879, 2436, 7888};
  private SerializablePair[] pairs = {
      new SerializablePair<>(111L, null),
      new SerializablePair<>(52782L, 134.3f),
      new SerializablePair<>(65492L, 1232.212f),
      new SerializablePair<>(69134L, 18.1233f),
      new SerializablePair<>(11111L, 233.5232f),
      new SerializablePair<>(99999L, 99999.f),
      new SerializablePair<>(99999L, null)
  };

  @Before
  public void setup()
  {
    floatLastAggregatorFactory = new FloatLastAggregatorFactory("billy", "nilly");
    combiningAggFactory = (FloatLastAggregatorFactory) floatLastAggregatorFactory.getCombiningFactory();
    timeSelector = new TestLongColumnSelector(times);
    valueSelector = new TestFloatColumnSelector(floats);
    objectSelector = new TestObjectColumnSelector<>(pairs);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME)).andReturn(timeSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("billy")).andReturn(objectSelector);
  }

  @Test
  public void testDoubleLastAggregator()
  {
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly")).andReturn(new ColumnCapabilitiesImpl().setType(
        ValueType.FLOAT));
    EasyMock.replay(colSelectorFactory);
    Aggregator agg = floatLastAggregatorFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Float> result = (Pair<Long, Float>) agg.get();

    Assert.assertEquals(times[0], result.lhs.longValue());
    Assert.assertEquals(floats[0], result.rhs, 0.0001);
    Assert.assertEquals((long) floats[0], agg.getLong());
    Assert.assertEquals(floats[0], agg.getFloat(), 0.0001);


  }

  @Test
  public void testDoubleLastBufferAggregator()
  {
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly")).andReturn(new ColumnCapabilitiesImpl().setType(
        ValueType.FLOAT));
    EasyMock.replay(colSelectorFactory);
    BufferAggregator agg = floatLastAggregatorFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[floatLastAggregatorFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Float> result = (Pair<Long, Float>) agg.get(buffer, 0);

    Assert.assertEquals(times[0], result.lhs.longValue());
    Assert.assertEquals(floats[0], result.rhs, 0.0001);
    Assert.assertEquals((long) floats[0], agg.getLong(buffer, 0));
    Assert.assertEquals(floats[0], agg.getFloat(buffer, 0), 0.0001);
  }

  @Test
  public void testCombine()
  {
    SerializablePair pair1 = new SerializablePair<>(1467225000L, 3.621f);
    SerializablePair pair2 = new SerializablePair<>(1467240000L, 785.4f);
    Assert.assertEquals(pair2, floatLastAggregatorFactory.combine(pair1, pair2));
  }

  @Test
  public void testComparatorWithNulls()
  {
    SerializablePair pair1 = new SerializablePair<>(1467225000L, 3.621f);
    SerializablePair pair2 = new SerializablePair<>(1467240000L, null);
    Comparator comparator = floatLastAggregatorFactory.getComparator();
    Assert.assertEquals(1, comparator.compare(pair1, pair2));
    Assert.assertEquals(0, comparator.compare(pair1, pair1));
    Assert.assertEquals(0, comparator.compare(pair2, pair2));
    Assert.assertEquals(-1, comparator.compare(pair2, pair1));
  }

  @Test
  public void testDoubleLastCombiningAggregator()
  {
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("billy")).andReturn(new ColumnCapabilitiesImpl().setType(
        ValueType.COMPLEX));
    EasyMock.replay(colSelectorFactory);

    Aggregator agg = combiningAggFactory.factorize(colSelectorFactory);

    agg.aggregate();
    objectSelector.increment();
    agg.aggregate();
    objectSelector.increment();
    agg.aggregate();
    objectSelector.increment();
    agg.aggregate();
    objectSelector.increment();
    agg.aggregate();
    objectSelector.increment();

    Pair<Long, Float> result = (Pair<Long, Float>) agg.get();
    // pair[3] has the largest timestamp in first 5 events
    Pair<Long, Float> expected = (Pair<Long, Float>) pairs[3];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs, 0.0001);
    Assert.assertEquals(expected.rhs.longValue(), agg.getLong());
    Assert.assertEquals(expected.rhs, agg.getFloat(), 0.0001);

    // aggregate once more, the last will change to pair[5] event
    agg.aggregate();
    objectSelector.increment();
    result = (Pair<Long, Float>) agg.get();
    expected = (Pair<Long, Float>) pairs[5];
    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs, 0.0001);
    Assert.assertEquals(expected.rhs.longValue(), agg.getLong());
    Assert.assertEquals(expected.rhs, agg.getFloat(), 0.0001);

    // aggregate once more, now the last event has the same timestamp as the last-1 event, it will be the last
    agg.aggregate();
    objectSelector.increment();
    result = (Pair<Long, Float>) agg.get();
    expected = (Pair<Long, Float>) pairs[6];
    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(result.rhs, null);
  }

  @Test
  public void testDoubleLastCombiningBufferAggregator()
  {
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("billy")).andReturn(new ColumnCapabilitiesImpl().setType(
        ValueType.COMPLEX));
    EasyMock.replay(colSelectorFactory);

    BufferAggregator agg = combiningAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[floatLastAggregatorFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    // aggregate first 5 events, pair[3] is the last
    agg.aggregate(buffer, 0);
    objectSelector.increment();
    agg.aggregate(buffer, 0);
    objectSelector.increment();
    agg.aggregate(buffer, 0);
    objectSelector.increment();
    agg.aggregate(buffer, 0);
    objectSelector.increment();
    agg.aggregate(buffer, 0);
    objectSelector.increment();

    Pair<Long, Float> result = (Pair<Long, Float>) agg.get(buffer, 0);
    Pair<Long, Float> expected = (Pair<Long, Float>) pairs[3];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs, 0.0001);
    Assert.assertEquals(expected.rhs.longValue(), agg.getLong(buffer, 0));
    Assert.assertEquals(expected.rhs, agg.getFloat(buffer, 0), 0.0001);

    // aggregate once more, pair[5] is the last
    agg.aggregate(buffer, 0);
    objectSelector.increment();
    result = (Pair<Long, Float>) agg.get(buffer, 0);
    expected = (Pair<Long, Float>) pairs[5];
    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs, 0.0001);
    Assert.assertEquals(expected.rhs.longValue(), agg.getLong(buffer, 0));
    Assert.assertEquals(expected.rhs, agg.getFloat(buffer, 0), 0.0001);

    // aggregate once more, pair[6] has the same timestamp with pair[5], it will be the last
    agg.aggregate(buffer, 0);
    objectSelector.increment();
    result = (Pair<Long, Float>) agg.get(buffer, 0);
    expected = (Pair<Long, Float>) pairs[5];
    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(result.rhs, null);
  }

  @Test
  public void testSerde() throws Exception
  {
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    String doubleSpecJson = "{\"type\":\"floatLast\",\"name\":\"billy\",\"fieldName\":\"nilly\"}";
    Assert.assertEquals(floatLastAggregatorFactory, mapper.readValue(doubleSpecJson, AggregatorFactory.class));
  }

  @Test
  public void testDoubleLastAggregateCombiner()
  {
    AggregateCombiner floatLastAggregateCombiner = combiningAggFactory.makeAggregateCombiner();

    SerializablePair[] inputPairs = {
        new SerializablePair<>(3L, 18f),
        new SerializablePair<>(5L, 134.3f),
        new SerializablePair<>(6L, 1232.212f),
        new SerializablePair<>(1L, 233.5232f)
    };
    TestObjectColumnSelector columnSelector = new TestObjectColumnSelector<>(inputPairs);
    floatLastAggregateCombiner.reset(columnSelector);
    Assert.assertEquals(inputPairs[0], floatLastAggregateCombiner.getObject());

    // inputPairs[1] has larger time value, it should be the last
    columnSelector.increment();
    floatLastAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(inputPairs[1], floatLastAggregateCombiner.getObject());

    // inputPairs[2] has larger time value, it should be the last
    columnSelector.increment();
    floatLastAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(inputPairs[2], floatLastAggregateCombiner.getObject());

    // inputPairs[3] has the min time value, it should NOT be the first
    columnSelector.increment();
    floatLastAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(inputPairs[2], floatLastAggregateCombiner.getObject());

    floatLastAggregateCombiner.reset(columnSelector);
    Assert.assertEquals(inputPairs[3], floatLastAggregateCombiner.getObject());
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

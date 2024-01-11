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
import org.apache.druid.query.aggregation.SerializablePairLongFloat;
import org.apache.druid.query.aggregation.TestFloatColumnSelector;
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

public class FloatLastAggregationTest extends InitializedNullHandlingTest
{
  private FloatLastAggregatorFactory floatLastAggregatorFactory;
  private FloatLastAggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestLongColumnSelector timeSelector;
  private TestLongColumnSelector customTimeSelector;
  private TestFloatColumnSelector valueSelector;
  private TestObjectColumnSelector objectSelector;

  private float[] floats = {1.1897f, 0.001f, 86.23f, 166.228f};
  private long[] times = {8224, 6879, 2436, 7888};
  private long[] customTimes = {1, 4, 3, 2};
  private SerializablePairLongFloat[] pairs = {
      new SerializablePairLongFloat(52782L, 134.3f),
      new SerializablePairLongFloat(65492L, 1232.212f),
      new SerializablePairLongFloat(69134L, 18.1233f),
      new SerializablePairLongFloat(11111L, 233.5232f)
  };

  @Before
  public void setup()
  {
    floatLastAggregatorFactory = new FloatLastAggregatorFactory("billy", "nilly", null);
    combiningAggFactory = (FloatLastAggregatorFactory) floatLastAggregatorFactory.getCombiningFactory();
    timeSelector = new TestLongColumnSelector(times);
    customTimeSelector = new TestLongColumnSelector(customTimes);
    valueSelector = new TestFloatColumnSelector(floats);
    objectSelector = new TestObjectColumnSelector<>(pairs);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME)).andReturn(timeSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("customTime")).andReturn(customTimeSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("billy")).andReturn(objectSelector);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.FLOAT));
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("billy")).andReturn(null);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testFloatLastAggregator()
  {
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
  public void testFloatLastAggregatorWithTimeColumn()
  {
    Aggregator agg = new FloatLastAggregatorFactory("billy", "nilly", "customTime").factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Float> result = (Pair<Long, Float>) agg.get();

    Assert.assertEquals(customTimes[1], result.lhs.longValue());
    Assert.assertEquals(floats[1], result.rhs, 0.0001);
    Assert.assertEquals((long) floats[1], agg.getLong());
    Assert.assertEquals(floats[1], agg.getFloat(), 0.0001);
  }

  @Test
  public void testFloatLastBufferAggregator()
  {
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
  public void testFloatLastBufferAggregatorWithTimeColumn()
  {
    BufferAggregator agg = new FloatLastAggregatorFactory("billy", "nilly", "customTime").factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[floatLastAggregatorFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Float> result = (Pair<Long, Float>) agg.get(buffer, 0);

    Assert.assertEquals(customTimes[1], result.lhs.longValue());
    Assert.assertEquals(floats[1], result.rhs, 0.0001);
    Assert.assertEquals((long) floats[1], agg.getLong(buffer, 0));
    Assert.assertEquals(floats[1], agg.getFloat(buffer, 0), 0.0001);
  }

  @Test
  public void testCombine()
  {
    SerializablePairLongFloat pair1 = new SerializablePairLongFloat(1467225000L, 3.621f);
    SerializablePairLongFloat pair2 = new SerializablePairLongFloat(1467240000L, 785.4f);
    Assert.assertEquals(pair2, floatLastAggregatorFactory.combine(pair1, pair2));
  }

  @Test
  public void testComparatorWithNulls()
  {
    SerializablePairLongFloat pair1 = new SerializablePairLongFloat(1467225000L, 3.621f);
    SerializablePairLongFloat pair2 = new SerializablePairLongFloat(1467240000L, null);
    Comparator comparator = floatLastAggregatorFactory.getComparator();
    Assert.assertEquals(1, comparator.compare(pair1, pair2));
    Assert.assertEquals(0, comparator.compare(pair1, pair1));
    Assert.assertEquals(0, comparator.compare(pair2, pair2));
    Assert.assertEquals(-1, comparator.compare(pair2, pair1));
  }

  @Test
  public void testFloatLastCombiningAggregator()
  {
    Aggregator agg = combiningAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Float> result = (Pair<Long, Float>) agg.get();
    Pair<Long, Float> expected = (Pair<Long, Float>) pairs[2];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs, 0.0001);
    Assert.assertEquals(expected.rhs.longValue(), agg.getLong());
    Assert.assertEquals(expected.rhs, agg.getFloat(), 0.0001);
  }

  @Test
  public void testFloatLastCombiningBufferAggregator()
  {
    BufferAggregator agg = combiningAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[floatLastAggregatorFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Float> result = (Pair<Long, Float>) agg.get(buffer, 0);
    Pair<Long, Float> expected = (Pair<Long, Float>) pairs[2];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs, 0.0001);
    Assert.assertEquals(expected.rhs.longValue(), agg.getLong(buffer, 0));
    Assert.assertEquals(expected.rhs, agg.getFloat(buffer, 0), 0.0001);
  }


  @Test
  public void testSerde() throws Exception
  {
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    String floatSpecJson = "{\"type\":\"floatLast\",\"name\":\"billy\",\"fieldName\":\"nilly\"}";
    AggregatorFactory deserialized = mapper.readValue(floatSpecJson, AggregatorFactory.class);
    Assert.assertEquals(floatLastAggregatorFactory, deserialized);
    Assert.assertArrayEquals(floatLastAggregatorFactory.getCacheKey(), deserialized.getCacheKey());
  }

  @Test
  public void testFloatLastAggregateCombiner()
  {
    TestObjectColumnSelector columnSelector = new TestObjectColumnSelector<>(pairs);
    AggregateCombiner floatLastAggregateCombiner = combiningAggFactory.makeAggregateCombiner();
    floatLastAggregateCombiner.reset(columnSelector);

    Assert.assertEquals(pairs[0], floatLastAggregateCombiner.getObject());

    columnSelector.increment();
    floatLastAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(pairs[1], floatLastAggregateCombiner.getObject());

    columnSelector.increment();
    floatLastAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(pairs[2], floatLastAggregateCombiner.getObject());

    columnSelector.increment();
    floatLastAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(pairs[2], floatLastAggregateCombiner.getObject());

    floatLastAggregateCombiner.reset(columnSelector);
    Assert.assertEquals(pairs[3], floatLastAggregateCombiner.getObject());
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

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
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.TestFloatColumnSelector;
import org.apache.druid.query.aggregation.TestLongColumnSelector;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnHolder;
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
      new SerializablePair<>(1467225096L, 134.3f),
      new SerializablePair<>(23163L, 1232.212f),
      new SerializablePair<>(742L, 18f),
      new SerializablePair<>(111111L, 233.5232f)
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
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testDoubleFirstAggregator()
  {
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

  @Test
  public void testDoubleFirstBufferAggregator()
  {
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

  @Test
  public void testDoubleFirstCombiningAggregator()
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
  public void testDoubleFirstCombiningBufferAggregator()
  {
    BufferAggregator agg = combiningAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[floatFirstAggregatorFactory.getMaxIntermediateSizeWithNulls()]);
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
    String doubleSpecJson = "{\"type\":\"floatFirst\",\"name\":\"billy\",\"fieldName\":\"nilly\"}";
    Assert.assertEquals(floatFirstAggregatorFactory, mapper.readValue(doubleSpecJson, AggregatorFactory.class));
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

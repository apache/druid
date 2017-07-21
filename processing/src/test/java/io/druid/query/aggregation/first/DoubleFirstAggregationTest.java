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

package io.druid.query.aggregation.first;

import io.druid.collections.SerializablePair;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Pair;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.TestDoubleColumnSelectorImpl;
import io.druid.query.aggregation.TestLongColumnSelector;
import io.druid.query.aggregation.TestObjectColumnSelector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.Column;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class DoubleFirstAggregationTest
{
  private DoubleFirstAggregatorFactory doubleFirstAggFactory;
  private DoubleFirstAggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestLongColumnSelector timeSelector;
  private TestDoubleColumnSelectorImpl valueSelector;
  private TestObjectColumnSelector objectSelector;

  private double[] doubleValues = {1.1d, 2.7d, 3.5d, 1.3d};
  private long[] times = {12, 10, 5344, 7899999};
  private SerializablePair[] pairs = {
      new SerializablePair<>(1467225096L, 134.3d),
      new SerializablePair<>(23163L, 1232.212d),
      new SerializablePair<>(742L, 18d),
      new SerializablePair<>(111111L, 233.5232d)
  };

  @Before
  public void setup()
  {
    doubleFirstAggFactory = new DoubleFirstAggregatorFactory("billy", "nilly");
    combiningAggFactory = (DoubleFirstAggregatorFactory) doubleFirstAggFactory.getCombiningFactory();
    timeSelector = new TestLongColumnSelector(times);
    valueSelector = new TestDoubleColumnSelectorImpl(doubleValues);
    objectSelector = new TestObjectColumnSelector(pairs);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeLongColumnSelector(Column.TIME_COLUMN_NAME)).andReturn(timeSelector);
    EasyMock.expect(colSelectorFactory.makeDoubleColumnSelector("nilly")).andReturn(valueSelector);
    EasyMock.expect(colSelectorFactory.makeObjectColumnSelector("billy")).andReturn(objectSelector);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testDoubleFirstAggregator()
  {
    DoubleFirstAggregator agg = (DoubleFirstAggregator) doubleFirstAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get();

    Assert.assertEquals(times[1], result.lhs.longValue());
    Assert.assertEquals(doubleValues[1], result.rhs, 0.0001);
    Assert.assertEquals((long) doubleValues[1], agg.getLong());
    Assert.assertEquals(doubleValues[1], agg.getDouble(), 0.0001);

    agg.reset();
    Assert.assertEquals(0, ((Pair<Long, Double>) agg.get()).rhs, 0.0001);
  }

  @Test
  public void testDoubleFirstBufferAggregator()
  {
    DoubleFirstBufferAggregator agg = (DoubleFirstBufferAggregator) doubleFirstAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[doubleFirstAggFactory.getMaxIntermediateSize()]);
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
  public void testCombine()
  {
    SerializablePair pair1 = new SerializablePair<>(1467225000L, 3.621);
    SerializablePair pair2 = new SerializablePair<>(1467240000L, 785.4);
    Assert.assertEquals(pair1, doubleFirstAggFactory.combine(pair1, pair2));
  }

  @Test
  public void testDoubleFirstCombiningAggregator()
  {
    DoubleFirstAggregator agg = (DoubleFirstAggregator) combiningAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get();
    Pair<Long, Double> expected = (Pair<Long, Double>)pairs[2];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs, 0.0001);
    Assert.assertEquals(expected.rhs.longValue(), agg.getLong());
    Assert.assertEquals(expected.rhs, agg.getDouble(), 0.0001);

    agg.reset();
    Assert.assertEquals(0, ((Pair<Long, Double>) agg.get()).rhs, 0.0001);
  }

  @Test
  public void testDoubleFirstCombiningBufferAggregator()
  {
    DoubleFirstBufferAggregator agg = (DoubleFirstBufferAggregator) combiningAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[doubleFirstAggFactory.getMaxIntermediateSize()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get(buffer, 0);
    Pair<Long, Double> expected = (Pair<Long, Double>)pairs[2];

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
    Assert.assertEquals(doubleFirstAggFactory, mapper.readValue(doubleSpecJson, AggregatorFactory.class));
  }

  private void aggregate(
      DoubleFirstAggregator agg
  )
  {
    agg.aggregate();
    timeSelector.increment();
    valueSelector.increment();
    objectSelector.increment();
  }

  private void aggregate(
      DoubleFirstBufferAggregator agg,
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

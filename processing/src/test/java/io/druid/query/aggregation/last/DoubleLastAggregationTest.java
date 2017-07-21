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

package io.druid.query.aggregation.last;

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

public class DoubleLastAggregationTest
{
  private DoubleLastAggregatorFactory doubleLastAggFactory;
  private DoubleLastAggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestLongColumnSelector timeSelector;
  private TestDoubleColumnSelectorImpl valueSelector;
  private TestObjectColumnSelector objectSelector;

  private double[] doubles = {1.1897d, 0.001d, 86.23d, 166.228d};
  private long[] times = {8224, 6879, 2436, 7888};
  private SerializablePair[] pairs = {
      new SerializablePair<>(52782L, 134.3d),
      new SerializablePair<>(65492L, 1232.212d),
      new SerializablePair<>(69134L, 18.1233d),
      new SerializablePair<>(11111L, 233.5232d)
  };

  @Before
  public void setup()
  {
    doubleLastAggFactory = new DoubleLastAggregatorFactory("billy", "nilly");
    combiningAggFactory = (DoubleLastAggregatorFactory) doubleLastAggFactory.getCombiningFactory();
    timeSelector = new TestLongColumnSelector(times);
    valueSelector = new TestDoubleColumnSelectorImpl(doubles);
    objectSelector = new TestObjectColumnSelector(pairs);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeLongColumnSelector(Column.TIME_COLUMN_NAME)).andReturn(timeSelector);
    EasyMock.expect(colSelectorFactory.makeDoubleColumnSelector("nilly")).andReturn(valueSelector);
    EasyMock.expect(colSelectorFactory.makeObjectColumnSelector("billy")).andReturn(objectSelector);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testDoubleLastAggregator()
  {
    DoubleLastAggregator agg = (DoubleLastAggregator) doubleLastAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get();

    Assert.assertEquals(times[0], result.lhs.longValue());
    Assert.assertEquals(doubles[0], result.rhs, 0.0001);
    Assert.assertEquals((long) doubles[0], agg.getLong());
    Assert.assertEquals(doubles[0], agg.getDouble(), 0.0001);

    agg.reset();
    Assert.assertEquals(0, ((Pair<Long, Double>) agg.get()).rhs, 0.0001);
  }

  @Test
  public void testDoubleLastBufferAggregator()
  {
    DoubleLastBufferAggregator agg = (DoubleLastBufferAggregator) doubleLastAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[doubleLastAggFactory.getMaxIntermediateSize()]);
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
  public void testCombine()
  {
    SerializablePair pair1 = new SerializablePair<>(1467225000L, 3.621);
    SerializablePair pair2 = new SerializablePair<>(1467240000L, 785.4);
    Assert.assertEquals(pair2, doubleLastAggFactory.combine(pair1, pair2));
  }

  @Test
  public void testDoubleLastCombiningAggregator()
  {
    DoubleLastAggregator agg = (DoubleLastAggregator) combiningAggFactory.factorize(colSelectorFactory);

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
  public void testDoubleLastCombiningBufferAggregator()
  {
    DoubleLastBufferAggregator agg = (DoubleLastBufferAggregator) combiningAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[doubleLastAggFactory.getMaxIntermediateSize()]);
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
    String doubleSpecJson = "{\"type\":\"doubleLast\",\"name\":\"billy\",\"fieldName\":\"nilly\"}";
    Assert.assertEquals(doubleLastAggFactory, mapper.readValue(doubleSpecJson, AggregatorFactory.class));
  }

  private void aggregate(
      DoubleLastAggregator agg
  )
  {
    agg.aggregate();
    timeSelector.increment();
    valueSelector.increment();
    objectSelector.increment();
  }

  private void aggregate(
      DoubleLastBufferAggregator agg,
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

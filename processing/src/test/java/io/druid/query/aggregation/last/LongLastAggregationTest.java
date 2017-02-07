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
import io.druid.query.aggregation.TestLongColumnSelector;
import io.druid.query.aggregation.TestObjectColumnSelector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.Column;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class LongLastAggregationTest
{
  private LongLastAggregatorFactory longLastAggFactory;
  private LongLastAggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestLongColumnSelector timeSelector;
  private TestLongColumnSelector valueSelector;
  private TestObjectColumnSelector objectSelector;

  private long[] longValues = {23216, 8635, 1547123, Long.MAX_VALUE};
  private long[] times = {1467935723, 1467225653, 1601848932, 72515};
  private SerializablePair[] pairs = {
      new SerializablePair<>(12531L, 113267L),
      new SerializablePair<>(123L, 5437384L),
      new SerializablePair<>(125755L, 34583458L),
      new SerializablePair<>(124L, 34283452L)
  };

  @Before
  public void setup()
  {
    longLastAggFactory = new LongLastAggregatorFactory("billy", "nilly");
    combiningAggFactory = (LongLastAggregatorFactory) longLastAggFactory.getCombiningFactory();
    timeSelector = new TestLongColumnSelector(times);
    valueSelector = new TestLongColumnSelector(longValues);
    objectSelector = new TestObjectColumnSelector(pairs);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeLongColumnSelector(Column.TIME_COLUMN_NAME)).andReturn(timeSelector);
    EasyMock.expect(colSelectorFactory.makeLongColumnSelector("nilly")).andReturn(valueSelector);
    EasyMock.expect(colSelectorFactory.makeObjectColumnSelector("billy")).andReturn(objectSelector);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testLongLastAggregator()
  {
    LongLastAggregator agg = (LongLastAggregator) longLastAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Long> result = (Pair<Long, Long>) agg.get();

    Assert.assertEquals(times[2], result.lhs.longValue());
    Assert.assertEquals(longValues[2], result.rhs.longValue());
    Assert.assertEquals(longValues[2], agg.getLong());
    Assert.assertEquals(longValues[2], agg.getFloat(), 1);

    agg.reset();
    Assert.assertEquals(0, ((Pair<Long, Long>) agg.get()).rhs.longValue());
  }

  @Test
  public void testLongLastBufferAggregator()
  {
    LongLastBufferAggregator agg = (LongLastBufferAggregator) longLastAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[longLastAggFactory.getMaxIntermediateSize()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Long> result = (Pair<Long, Long>) agg.get(buffer, 0);

    Assert.assertEquals(times[2], result.lhs.longValue());
    Assert.assertEquals(longValues[2], result.rhs.longValue());
    Assert.assertEquals(longValues[2], agg.getLong(buffer, 0));
    Assert.assertEquals(longValues[2], agg.getFloat(buffer, 0), 1);
  }

  @Test
  public void testCombine()
  {
    SerializablePair pair1 = new SerializablePair<>(1467225000L, 64432L);
    SerializablePair pair2 = new SerializablePair<>(1467240000L, 99999L);
    Assert.assertEquals(pair2, longLastAggFactory.combine(pair1, pair2));
  }

  @Test
  public void testLongLastCombiningAggregator()
  {
    LongLastAggregator agg = (LongLastAggregator) combiningAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Long> result = (Pair<Long, Long>) agg.get();
    Pair<Long, Long> expected = (Pair<Long, Long>)pairs[2];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs);
    Assert.assertEquals(expected.rhs.longValue(), agg.getLong());
    Assert.assertEquals(expected.rhs, agg.getFloat(), 1);

    agg.reset();
    Assert.assertEquals(0, ((Pair<Long, Long>) agg.get()).rhs.longValue());
  }

  @Test
  public void testLongLastCombiningBufferAggregator()
  {
    LongLastBufferAggregator agg = (LongLastBufferAggregator) combiningAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[longLastAggFactory.getMaxIntermediateSize()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Long> result = (Pair<Long, Long>) agg.get(buffer, 0);
    Pair<Long, Long> expected = (Pair<Long, Long>)pairs[2];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs);
    Assert.assertEquals(expected.rhs.longValue(), agg.getLong(buffer, 0));
    Assert.assertEquals(expected.rhs, agg.getFloat(buffer, 0), 1);
  }


  @Test
  public void testSerde() throws Exception
  {
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    String longSpecJson = "{\"type\":\"longLast\",\"name\":\"billy\",\"fieldName\":\"nilly\"}";
    Assert.assertEquals(longLastAggFactory, mapper.readValue(longSpecJson, AggregatorFactory.class));
  }

  private void aggregate(
      LongLastAggregator agg
  )
  {
    agg.aggregate();
    timeSelector.increment();
    valueSelector.increment();
    objectSelector.increment();
  }

  private void aggregate(
      LongLastBufferAggregator agg,
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

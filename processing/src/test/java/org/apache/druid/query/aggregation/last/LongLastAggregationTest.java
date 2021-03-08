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

public class LongLastAggregationTest extends InitializedNullHandlingTest
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
    objectSelector = new TestObjectColumnSelector<>(pairs);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME)).andReturn(timeSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("billy")).andReturn(objectSelector);
  }

  @Test
  public void testLongLastAggregator()
  {
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly")).andReturn(new ColumnCapabilitiesImpl().setType(
        ValueType.LONG));
    EasyMock.replay(colSelectorFactory);

    Aggregator agg = longLastAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Long> result = (Pair<Long, Long>) agg.get();

    Assert.assertEquals(times[2], result.lhs.longValue());
    Assert.assertEquals(longValues[2], result.rhs.longValue());
    Assert.assertEquals(longValues[2], agg.getLong());
    Assert.assertEquals(longValues[2], agg.getFloat(), 1);
  }

  @Test
  public void testLongLastBufferAggregator()
  {
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly")).andReturn(new ColumnCapabilitiesImpl().setType(
        ValueType.LONG));
    EasyMock.replay(colSelectorFactory);

    BufferAggregator agg = longLastAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[longLastAggFactory.getMaxIntermediateSizeWithNulls()]);
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
  public void testComparatorWithNulls()
  {
    SerializablePair pair1 = new SerializablePair<>(1467225000L, 1263L);
    SerializablePair pair2 = new SerializablePair<>(1467240000L, null);
    Comparator comparator = longLastAggFactory.getComparator();
    Assert.assertEquals(1, comparator.compare(pair1, pair2));
    Assert.assertEquals(0, comparator.compare(pair1, pair1));
    Assert.assertEquals(0, comparator.compare(pair2, pair2));
    Assert.assertEquals(-1, comparator.compare(pair2, pair1));
  }

  @Test
  public void testLongLastCombiningAggregator()
  {
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("billy")).andReturn(new ColumnCapabilitiesImpl().setType(
        ValueType.COMPLEX));
    EasyMock.replay(colSelectorFactory);

    Aggregator agg = combiningAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Long> result = (Pair<Long, Long>) agg.get();
    Pair<Long, Long> expected = (Pair<Long, Long>) pairs[2];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs);
    Assert.assertEquals(expected.rhs.longValue(), agg.getLong());
    Assert.assertEquals(expected.rhs, agg.getFloat(), 1);
  }

  @Test
  public void testLongLastCombiningBufferAggregator()
  {
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("billy")).andReturn(new ColumnCapabilitiesImpl().setType(
        ValueType.COMPLEX));
    EasyMock.replay(colSelectorFactory);

    BufferAggregator agg = combiningAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[longLastAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Long> result = (Pair<Long, Long>) agg.get(buffer, 0);
    Pair<Long, Long> expected = (Pair<Long, Long>) pairs[2];

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

  @Test
  public void testLongLastAggregateCombiner()
  {
    AggregateCombiner longLastAggregateCombiner = combiningAggFactory.makeAggregateCombiner();

    SerializablePair[] inputPairs = {
        new SerializablePair<>(3L, 18L),
        new SerializablePair<>(5L, 134L),
        new SerializablePair<>(6L, 1232L),
        new SerializablePair<>(1L, 2332L)
    };
    TestObjectColumnSelector columnSelector = new TestObjectColumnSelector<>(inputPairs);
    longLastAggregateCombiner.reset(columnSelector);
    Assert.assertEquals(inputPairs[0], longLastAggregateCombiner.getObject());

    // inputPairs[1] has larger time value, it should be the last
    columnSelector.increment();
    longLastAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(inputPairs[1], longLastAggregateCombiner.getObject());

    // inputPairs[2] has larger time value, it should be the last
    columnSelector.increment();
    longLastAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(inputPairs[2], longLastAggregateCombiner.getObject());

    // inputPairs[3] has the min time value, it should NOT be the first
    columnSelector.increment();
    longLastAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(inputPairs[2], longLastAggregateCombiner.getObject());

    longLastAggregateCombiner.reset(columnSelector);
    Assert.assertEquals(inputPairs[3], longLastAggregateCombiner.getObject());
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

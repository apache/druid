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
import org.apache.druid.query.aggregation.SerializablePairLongLong;
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

public class LongLastAggregationTest extends InitializedNullHandlingTest
{
  private LongLastAggregatorFactory longLastAggFactory;
  private LongLastAggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestLongColumnSelector timeSelector;
  private TestLongColumnSelector customTimeSelector;
  private TestLongColumnSelector valueSelector;
  private TestObjectColumnSelector objectSelector;

  private long[] longValues = {23216, 8635, 1547123, Long.MAX_VALUE};
  private long[] times = {1467935723, 1467225653, 1601848932, 72515};
  private long[] customTimes = {1, 4, 3, 2};
  private SerializablePairLongLong[] pairs = {
      new SerializablePairLongLong(12531L, 113267L),
      new SerializablePairLongLong(12534L, null),
      new SerializablePairLongLong(123L, 5437384L),
      new SerializablePairLongLong(125755L, 34583458L),
      new SerializablePairLongLong(124L, 34283452L)
  };

  @Before
  public void setup()
  {
    longLastAggFactory = new LongLastAggregatorFactory("billy", "nilly", null);
    combiningAggFactory = (LongLastAggregatorFactory) longLastAggFactory.getCombiningFactory();
    timeSelector = new TestLongColumnSelector(times);
    customTimeSelector = new TestLongColumnSelector(customTimes);
    valueSelector = new TestLongColumnSelector(longValues);
    objectSelector = new TestObjectColumnSelector<>(pairs);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME)).andReturn(timeSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("customTime")).andReturn(customTimeSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("billy")).andReturn(objectSelector);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.LONG));
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("billy")).andReturn(null);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testLongLastAggregator()
  {
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
  public void testLongLastAggregatorWithTimeColumn()
  {
    Aggregator agg = new LongLastAggregatorFactory("billy", "nilly", "customTime").factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Long> result = (Pair<Long, Long>) agg.get();

    Assert.assertEquals(customTimes[1], result.lhs.longValue());
    Assert.assertEquals(longValues[1], result.rhs.longValue());
    Assert.assertEquals(longValues[1], agg.getLong());
    Assert.assertEquals(longValues[1], agg.getFloat(), 1);
  }

  @Test
  public void testLongLastBufferAggregator()
  {
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
  public void testLongLastBufferAggregatorWithTimeColumn()
  {
    BufferAggregator agg = new LongLastAggregatorFactory("billy", "nilly", "customTime").factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[longLastAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Long> result = (Pair<Long, Long>) agg.get(buffer, 0);

    Assert.assertEquals(customTimes[1], result.lhs.longValue());
    Assert.assertEquals(longValues[1], result.rhs.longValue());
    Assert.assertEquals(longValues[1], agg.getLong(buffer, 0));
    Assert.assertEquals(longValues[1], agg.getFloat(buffer, 0), 1);
  }

  @Test
  public void testCombine()
  {
    SerializablePairLongLong pair1 = new SerializablePairLongLong(1467225000L, 64432L);
    SerializablePairLongLong pair2 = new SerializablePairLongLong(1467240000L, 99999L);
    Assert.assertEquals(pair2, longLastAggFactory.combine(pair1, pair2));
  }

  @Test
  public void testComparatorWithNulls()
  {
    SerializablePairLongLong pair1 = new SerializablePairLongLong(1467225000L, 1263L);
    SerializablePairLongLong pair2 = new SerializablePairLongLong(1467240000L, null);
    Comparator comparator = longLastAggFactory.getComparator();
    Assert.assertEquals(1, comparator.compare(pair1, pair2));
    Assert.assertEquals(0, comparator.compare(pair1, pair1));
    Assert.assertEquals(0, comparator.compare(pair2, pair2));
    Assert.assertEquals(-1, comparator.compare(pair2, pair1));
  }

  @Test
  public void testLongLastCombiningAggregator()
  {
    Aggregator agg = combiningAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, Long> result = (Pair<Long, Long>) agg.get();
    Pair<Long, Long> expected = (Pair<Long, Long>) pairs[3];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs);
    Assert.assertEquals(expected.rhs.longValue(), agg.getLong());
    Assert.assertEquals(expected.rhs, agg.getFloat(), 1);
  }

  @Test
  public void testLongLastCombiningBufferAggregator()
  {
    BufferAggregator agg = combiningAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[longLastAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, Long> result = (Pair<Long, Long>) agg.get(buffer, 0);
    Pair<Long, Long> expected = (Pair<Long, Long>) pairs[3];

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
    AggregatorFactory deserialized = mapper.readValue(longSpecJson, AggregatorFactory.class);
    Assert.assertEquals(longLastAggFactory, deserialized);
    Assert.assertArrayEquals(longLastAggFactory.getCacheKey(), deserialized.getCacheKey());
  }

  @Test
  public void testLongLastAggregateCombiner()
  {
    TestObjectColumnSelector columnSelector = new TestObjectColumnSelector<>(pairs);
    AggregateCombiner longLastAggregateCombiner = combiningAggFactory.makeAggregateCombiner();
    longLastAggregateCombiner.reset(columnSelector);

    Assert.assertEquals(pairs[0], longLastAggregateCombiner.getObject());

    columnSelector.increment();
    longLastAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(pairs[1], longLastAggregateCombiner.getObject());

    columnSelector.increment();
    longLastAggregateCombiner.fold(columnSelector);
    Assert.assertEquals(pairs[1], longLastAggregateCombiner.getObject());

    longLastAggregateCombiner.reset(columnSelector);
    Assert.assertEquals(pairs[2], longLastAggregateCombiner.getObject());
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

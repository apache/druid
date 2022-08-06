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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongString;
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

public class StringFirstAggregationTest extends InitializedNullHandlingTest
{
  private final Integer MAX_STRING_SIZE = 1024;
  private AggregatorFactory stringFirstAggFactory;
  private AggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestLongColumnSelector timeSelector;
  private TestLongColumnSelector customTimeSelector;
  private TestObjectColumnSelector<String> valueSelector;
  private TestObjectColumnSelector objectSelector;

  private String[] strings = {"1111", "2222", "3333", null, "4444"};
  private long[] times = {8224, 6879, 2436, 3546, 7888};
  private long[] customTimes = {2, 1, 3, 4, 5};
  private SerializablePairLongString[] pairs = {
      new SerializablePairLongString(52782L, "AAAA"),
      new SerializablePairLongString(65492L, "BBBB"),
      new SerializablePairLongString(69134L, "CCCC"),
      new SerializablePairLongString(11111L, "DDDD"),
      new SerializablePairLongString(51223L, null)
  };

  @Before
  public void setup()
  {
    NullHandling.initializeForTests();
    stringFirstAggFactory = new StringFirstAggregatorFactory("billy", "nilly", null, MAX_STRING_SIZE);
    combiningAggFactory = stringFirstAggFactory.getCombiningFactory();
    timeSelector = new TestLongColumnSelector(times);
    customTimeSelector = new TestLongColumnSelector(customTimes);
    valueSelector = new TestObjectColumnSelector<>(strings);
    objectSelector = new TestObjectColumnSelector<>(pairs);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME)).andReturn(timeSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("customTime")).andReturn(customTimeSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("billy")).andReturn(objectSelector);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.STRING));
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("billy")).andReturn(null);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testStringFirstAggregator()
  {
    Aggregator agg = stringFirstAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, String> result = (Pair<Long, String>) agg.get();

    Assert.assertEquals(strings[2], result.rhs);
  }

  @Test
  public void testStringFirstAggregatorWithTimeColumn()
  {
    Aggregator agg = new StringFirstAggregatorFactory("billy", "nilly", "customTime", MAX_STRING_SIZE).factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, String> result = (Pair<Long, String>) agg.get();

    Assert.assertEquals(strings[1], result.rhs);
  }

  @Test
  public void testStringFirstBufferAggregator()
  {
    BufferAggregator agg = new StringFirstAggregatorFactory("billy", "nilly", "customTime", MAX_STRING_SIZE)
        .factorizeBuffered(colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[stringFirstAggFactory.getMaxIntermediateSize()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, String> result = (Pair<Long, String>) agg.get(buffer, 0);

    Assert.assertEquals(strings[1], result.rhs);
  }

  @Test
  public void testCombineLeftLower()
  {
    SerializablePairLongString pair1 = new SerializablePairLongString(1467225000L, "AAAA");
    SerializablePairLongString pair2 = new SerializablePairLongString(1467240000L, "BBBB");
    Assert.assertEquals(pair1, stringFirstAggFactory.combine(pair1, pair2));
  }

  @Test
  public void testCombineRightLower()
  {
    SerializablePairLongString pair1 = new SerializablePairLongString(1467240000L, "AAAA");
    SerializablePairLongString pair2 = new SerializablePairLongString(1467225000L, "BBBB");
    Assert.assertEquals(pair2, stringFirstAggFactory.combine(pair1, pair2));
  }

  @Test
  public void testCombineLeftRightSame()
  {
    SerializablePairLongString pair1 = new SerializablePairLongString(1467225000L, "AAAA");
    SerializablePairLongString pair2 = new SerializablePairLongString(1467225000L, "BBBB");
    Assert.assertEquals(pair1, stringFirstAggFactory.combine(pair1, pair2));
  }

  @Test
  public void testStringFirstCombiningAggregator()
  {
    Aggregator agg = combiningAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, String> result = (Pair<Long, String>) agg.get();
    Pair<Long, String> expected = pairs[3];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs);
  }

  @Test
  public void testStringFirstCombiningBufferAggregator()
  {
    BufferAggregator agg = combiningAggFactory.factorizeBuffered(colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[stringFirstAggFactory.getMaxIntermediateSize()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, String> result = (Pair<Long, String>) agg.get(buffer, 0);
    Pair<Long, String> expected = pairs[3];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs);
  }

  @Test
  public void testStringFirstAggregateCombiner()
  {
    TestObjectColumnSelector columnSelector = new TestObjectColumnSelector<>(pairs);

    AggregateCombiner stringFirstAggregateCombiner =
        combiningAggFactory.makeAggregateCombiner();

    stringFirstAggregateCombiner.reset(columnSelector);

    Assert.assertEquals(pairs[0], stringFirstAggregateCombiner.getObject());

    columnSelector.increment();
    stringFirstAggregateCombiner.fold(columnSelector);

    Assert.assertEquals(pairs[0], stringFirstAggregateCombiner.getObject());

    stringFirstAggregateCombiner.reset(columnSelector);

    Assert.assertEquals(pairs[1], stringFirstAggregateCombiner.getObject());
  }

  @Test
  @SuppressWarnings("EqualsWithItself")
  public void testStringLastAggregatorComparator()
  {
    Comparator<SerializablePairLongString> comparator =
        (Comparator<SerializablePairLongString>) stringFirstAggFactory.getComparator();
    SerializablePairLongString pair1 = new SerializablePairLongString(1L, "Z");
    SerializablePairLongString pair2 = new SerializablePairLongString(2L, "A");
    SerializablePairLongString pair3 = new SerializablePairLongString(3L, null);

    // check non null values
    Assert.assertEquals(0, comparator.compare(pair1, pair1));
    Assert.assertTrue(comparator.compare(pair1, pair2) > 0);
    Assert.assertTrue(comparator.compare(pair2, pair1) < 0);

    // check non null value with null value (null values first comparator)
    Assert.assertEquals(0, comparator.compare(pair3, pair3));
    Assert.assertTrue(comparator.compare(pair1, pair3) > 0);
    Assert.assertTrue(comparator.compare(pair3, pair1) < 0);

    // check non null pair with null pair (null pairs first comparator)
    Assert.assertEquals(0, comparator.compare(null, null));
    Assert.assertTrue(comparator.compare(pair1, null) > 0);
    Assert.assertTrue(comparator.compare(null, pair1) < 0);
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

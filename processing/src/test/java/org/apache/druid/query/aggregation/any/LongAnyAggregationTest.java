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

package org.apache.druid.query.aggregation.any;

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.TestLongColumnSelector;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class LongAnyAggregationTest extends InitializedNullHandlingTest
{

  private LongAnyAggregatorFactory longAnyAggFactory;
  private LongAnyAggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestLongColumnSelector valueSelector;
  private TestObjectColumnSelector objectSelector;

  private long[] longs = {185, -216, -128751132, Long.MIN_VALUE};
  private Long[] objects = {1123126751L, 1784247991L, 1854329816L, 1000000000L};

  @Before
  public void setup()
  {
    longAnyAggFactory = new LongAnyAggregatorFactory("billy", "nilly");
    combiningAggFactory = (LongAnyAggregatorFactory) longAnyAggFactory.getCombiningFactory();
    valueSelector = new TestLongColumnSelector(longs);
    objectSelector = new TestObjectColumnSelector<>(objects);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("billy")).andReturn(objectSelector);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testLongAnyAggregator()
  {
    Aggregator agg = longAnyAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Long result = (Long) agg.get();

    Assert.assertEquals((Long) longs[0], result);
    Assert.assertEquals((long) longs[0], agg.getLong());
    Assert.assertEquals(longs[0], agg.getLong(), 0.0001);
  }

  @Test
  public void testLongAnyBufferAggregator()
  {
    BufferAggregator agg = longAnyAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[longAnyAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Long result = (Long) agg.get(buffer, 0);

    Assert.assertEquals(longs[0], result, 0.0001);
    Assert.assertEquals((long) longs[0], agg.getLong(buffer, 0));
    Assert.assertEquals(longs[0], agg.getLong(buffer, 0), 0.0001);
  }

  @Test
  public void testCombine()
  {
    Long l1 = 3L;
    Long l2 = 4L;
    Assert.assertEquals(l1, longAnyAggFactory.combine(l1, l2));
  }

  @Test
  public void testComparatorWithNulls()
  {
    Long l1 = 3L;
    Long l2 = null;
    Comparator comparator = longAnyAggFactory.getComparator();
    Assert.assertEquals(1, comparator.compare(l1, l2));
    Assert.assertEquals(0, comparator.compare(l1, l1));
    Assert.assertEquals(0, comparator.compare(l2, l2));
    Assert.assertEquals(-1, comparator.compare(l2, l1));
  }

  @Test
  public void testLongAnyCombiningAggregator()
  {
    Aggregator agg = combiningAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Long result = (Long) agg.get();

    Assert.assertEquals(objects[0], result, 0.0001);
    Assert.assertEquals(objects[0].longValue(), agg.getLong());
    Assert.assertEquals(objects[0], agg.getLong(), 0.0001);
  }

  @Test
  public void testLongAnyCombiningBufferAggregator()
  {
    BufferAggregator agg = combiningAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[longAnyAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Long result = (Long) agg.get(buffer, 0);

    Assert.assertEquals(objects[0], result, 0.0001);
    Assert.assertEquals(objects[0].longValue(), agg.getLong(buffer, 0));
    Assert.assertEquals(objects[0], agg.getLong(buffer, 0), 0.0001);
  }

  @Test
  public void testSerde() throws Exception
  {
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    String longSpecJson = "{\"type\":\"longAny\",\"name\":\"billy\",\"fieldName\":\"nilly\"}";
    Assert.assertEquals(longAnyAggFactory, mapper.readValue(longSpecJson, AggregatorFactory.class));
  }

  private void aggregate(
      Aggregator agg
  )
  {
    agg.aggregate();
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
    valueSelector.increment();
    objectSelector.increment();
  }
}

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

package io.druid.query.aggregation;

import com.metamx.common.Pair;
import io.druid.collections.SerializablePair;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.Column;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class LastAggregationTest
{
  private LastAggregatorFactory doubleLastAggFactory;
  private LastAggregatorFactory longLastAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestLongColumnSelector timeSelector;
  private TestFloatColumnSelector floatSelector;
  private TestLongColumnSelector longSelector;

  private long[] longValues = {62, 8, 54, 2};
  private float[] floatValues = {1.1f, 2.7f, 3.5f, 1.3f};
  private long[] times = {1467225096, 1467225098, 1467225099, 1467225111};

  public LastAggregationTest() throws Exception
  {
    String doubleSpecJson = "{\"type\": \"last\", \"name\": \"billy\", \"fieldName\": \"nilly\", \"value\": \"double\"}";
    String longSpecJson = "{\"type\": \"last\", \"name\": \"bill\", \"fieldName\": \"nnn\", \"value\": \"long\"}";
    doubleLastAggFactory = new DefaultObjectMapper().readValue(doubleSpecJson, LastAggregatorFactory.class);
    longLastAggFactory = new DefaultObjectMapper().readValue(longSpecJson, LastAggregatorFactory.class);
  }

  @Before
  public void setup()
  {
    timeSelector = new TestLongColumnSelector(times);
    floatSelector = new TestFloatColumnSelector(floatValues);
    longSelector = new TestLongColumnSelector(longValues);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeLongColumnSelector(Column.TIME_COLUMN_NAME)).andReturn(timeSelector);
    EasyMock.expect(colSelectorFactory.makeFloatColumnSelector("nilly")).andReturn(floatSelector);
    EasyMock.expect(colSelectorFactory.makeLongColumnSelector("nnn")).andReturn(longSelector);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testDoubleLastAggregator()
  {
    DoubleLastAggregator agg = (DoubleLastAggregator) doubleLastAggFactory.factorize(colSelectorFactory);

    Assert.assertEquals("billy", agg.getName());

    aggregate(timeSelector, floatSelector, agg);
    aggregate(timeSelector, floatSelector, agg);
    aggregate(timeSelector, floatSelector, agg);
    aggregate(timeSelector, floatSelector, agg);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get();

    Assert.assertEquals(times[3], result.lhs.longValue());
    Assert.assertEquals(floatValues[3], result.rhs, 0.0001);
    Assert.assertEquals((long) floatValues[3], agg.getLong());
    Assert.assertEquals(floatValues[3], agg.getFloat(), 0.0001);

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

    aggregate(timeSelector, floatSelector, agg, buffer, 0);
    aggregate(timeSelector, floatSelector, agg, buffer, 0);
    aggregate(timeSelector, floatSelector, agg, buffer, 0);
    aggregate(timeSelector, floatSelector, agg, buffer, 0);

    Pair<Long, Double> result = (Pair<Long, Double>) agg.get(buffer, 0);

    Assert.assertEquals(times[3], result.lhs.longValue());
    Assert.assertEquals(floatValues[3], result.rhs, 0.0001);
    Assert.assertEquals((long) floatValues[3], agg.getLong(buffer, 0));
    Assert.assertEquals(floatValues[3], agg.getFloat(buffer, 0), 0.0001);
  }

  @Test
  public void testLongLastAggregator()
  {
    LongLastAggregator agg = (LongLastAggregator) longLastAggFactory.factorize(colSelectorFactory);

    Assert.assertEquals("bill", agg.getName());

    aggregate(timeSelector, longSelector, agg);
    aggregate(timeSelector, longSelector, agg);
    aggregate(timeSelector, longSelector, agg);
    aggregate(timeSelector, longSelector, agg);

    Pair<Long, Long> result = (Pair<Long, Long>) agg.get();

    Assert.assertEquals(times[3], result.lhs.longValue());
    Assert.assertEquals(longValues[3], result.rhs.longValue());
    Assert.assertEquals(longValues[3], agg.getLong());
    Assert.assertEquals(longValues[3], agg.getFloat(), 0.0001);

    agg.reset();
    Assert.assertEquals(0, ((Pair<Long, Long>) agg.get()).rhs.longValue());
  }

  @Test
  public void testLongLastBufferAggregator()
  {
    LongLastBufferAggregator agg = (LongLastBufferAggregator) longLastAggFactory.factorizeBuffered(colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[longLastAggFactory.getMaxIntermediateSize()]);
    agg.init(buffer, 0);

    aggregate(timeSelector, longSelector, agg, buffer, 0);
    aggregate(timeSelector, longSelector, agg, buffer, 0);
    aggregate(timeSelector, longSelector, agg, buffer, 0);
    aggregate(timeSelector, longSelector, agg, buffer, 0);

    Pair<Long, Long> result = (Pair<Long, Long>) agg.get(buffer, 0);

    Assert.assertEquals(times[3], result.lhs.longValue());
    Assert.assertEquals(longValues[3], result.rhs.longValue());
    Assert.assertEquals(longValues[3], agg.getLong(buffer, 0));
    Assert.assertEquals(longValues[3], agg.getFloat(buffer, 0), 0.0001);
  }

  @Test
  public void testCombine()
  {
    SerializablePair pair1 = new SerializablePair<>(1467225000L, 3.621);
    SerializablePair pair2 = new SerializablePair<>(1467240000L, 785.4);
    Assert.assertEquals(pair2, doubleLastAggFactory.combine(pair1, pair2));
  }


  @Test
  public void testEqualsAndHashCode() throws Exception
  {
    LastAggregatorFactory one = new LastAggregatorFactory("name1", "fieldName1", "double");
    LastAggregatorFactory oneAgain = new LastAggregatorFactory("name1", "fieldName1", "double");
    LastAggregatorFactory two = new LastAggregatorFactory("name1", "fieldName1", "long");
    LastAggregatorFactory three = new LastAggregatorFactory("name2", "fieldName2", "double");

    Assert.assertEquals(one.hashCode(), oneAgain.hashCode());

    Assert.assertTrue(one.equals(oneAgain));
    Assert.assertFalse(one.equals(two));
    Assert.assertFalse(one.equals(three));
  }

  private void aggregate(
      TestLongColumnSelector timeSelector,
      TestFloatColumnSelector selector,
      DoubleLastAggregator agg
  )
  {
    agg.aggregate();
    timeSelector.increment();
    selector.increment();
  }

  private void aggregate(
      TestLongColumnSelector timeSelector,
      TestFloatColumnSelector selector,
      DoubleLastBufferAggregator agg,
      ByteBuffer buff,
      int position
  )
  {
    agg.aggregate(buff, position);
    timeSelector.increment();
    selector.increment();
  }

  private void aggregate(
      TestLongColumnSelector timeSelector,
      TestLongColumnSelector selector,
      LongLastAggregator agg
  )
  {
    agg.aggregate();
    timeSelector.increment();
    selector.increment();
  }

  private void aggregate(
      TestLongColumnSelector timeSelector,
      TestLongColumnSelector selector,
      LongLastBufferAggregator agg,
      ByteBuffer buff,
      int position
  )
  {
    agg.aggregate(buff, position);
    timeSelector.increment();
    selector.increment();
  }
}

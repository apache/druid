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

import com.google.common.primitives.Longs;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.TestHelper;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
  */
public class LongMaxAggregationTest
{
  private LongMaxAggregatorFactory longMaxAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestLongColumnSelector selector;

  private long[] values = {9223372036854775802L, 9223372036854775803L, 9223372036854775806L, 9223372036854775805L};

  public LongMaxAggregationTest() throws Exception
  {
    String aggSpecJson = "{\"type\": \"longMax\", \"name\": \"billy\", \"fieldName\": \"nilly\"}";
    longMaxAggFactory = TestHelper.getJsonMapper().readValue(aggSpecJson, LongMaxAggregatorFactory.class);
  }

  @Before
  public void setup()
  {
    selector = new TestLongColumnSelector(values);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeLongColumnSelector("nilly")).andReturn(selector);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testLongMaxAggregator()
  {
    LongMaxAggregator agg = (LongMaxAggregator)longMaxAggFactory.factorize(colSelectorFactory);

    aggregate(selector, agg);
    aggregate(selector, agg);
    aggregate(selector, agg);
    aggregate(selector, agg);

    Assert.assertEquals(values[2], ((Long) agg.get()).longValue());
    Assert.assertEquals(values[2], agg.getLong());
    Assert.assertEquals((float) values[2], agg.getFloat(), 0.0001);

    agg.reset();
    Assert.assertEquals(Long.MIN_VALUE, agg.getLong());
  }

  @Test
  public void testLongMaxBufferAggregator()
  {
    LongMaxBufferAggregator agg = (LongMaxBufferAggregator)longMaxAggFactory.factorizeBuffered(colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[Longs.BYTES]);
    agg.init(buffer, 0);

    aggregate(selector, agg, buffer, 0);
    aggregate(selector, agg, buffer, 0);
    aggregate(selector, agg, buffer, 0);
    aggregate(selector, agg, buffer, 0);

    Assert.assertEquals(values[2], ((Long) agg.get(buffer, 0)).longValue());
    Assert.assertEquals(values[2], agg.getLong(buffer, 0));
    Assert.assertEquals((float)values[2], agg.getFloat(buffer, 0), 0.0001);
  }

  @Test
  public void testCombine()
  {
    Assert.assertEquals(9223372036854775803L, longMaxAggFactory.combine(9223372036854775800L, 9223372036854775803L));
  }

  @Test
  public void testEqualsAndHashCode() throws Exception
  {
    LongMaxAggregatorFactory one = new LongMaxAggregatorFactory("name1", "fieldName1");
    LongMaxAggregatorFactory oneMore = new LongMaxAggregatorFactory("name1", "fieldName1");
    LongMaxAggregatorFactory two = new LongMaxAggregatorFactory("name2", "fieldName2");

    Assert.assertEquals(one.hashCode(), oneMore.hashCode());

    Assert.assertTrue(one.equals(oneMore));
    Assert.assertFalse(one.equals(two));
  }

  private void aggregate(TestLongColumnSelector selector, LongMaxAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  private void aggregate(TestLongColumnSelector selector, LongMaxBufferAggregator agg, ByteBuffer buff, int position)
  {
    agg.aggregate(buff, position);
    selector.increment();
  }
}

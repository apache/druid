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

import com.google.common.primitives.Doubles;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.TestHelper;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
  */
public class DoubleMaxAggregationTest
{
  private DoubleMaxAggregatorFactory doubleMaxAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestDoubleColumnSelectorImpl selector;

  private double[] values = {1.1d, 2.7d, 3.5d, 1.3d};

  public DoubleMaxAggregationTest() throws Exception
  {
    String aggSpecJson = "{\"type\": \"doubleMax\", \"name\": \"billy\", \"fieldName\": \"nilly\"}";
    doubleMaxAggFactory = TestHelper.getJsonMapper().readValue(aggSpecJson, DoubleMaxAggregatorFactory.class);
  }

  @Before
  public void setup()
  {
    selector = new TestDoubleColumnSelectorImpl(values);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeDoubleColumnSelector("nilly")).andReturn(selector);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testDoubleMaxAggregator()
  {
    DoubleMaxAggregator agg = (DoubleMaxAggregator) doubleMaxAggFactory.factorize(colSelectorFactory);

    aggregate(selector, agg);
    aggregate(selector, agg);
    aggregate(selector, agg);
    aggregate(selector, agg);

    Assert.assertEquals(values[2], ((Double) agg.get()).doubleValue(), 0.0001);
    Assert.assertEquals((long)values[2], agg.getLong());
    Assert.assertEquals(values[2], agg.getFloat(), 0.0001);

    agg.reset();
    Assert.assertEquals(Double.NEGATIVE_INFINITY, (Double) agg.get(), 0.0001);
  }

  @Test
  public void testDoubleMaxBufferAggregator()
  {
    DoubleMaxBufferAggregator agg = (DoubleMaxBufferAggregator) doubleMaxAggFactory.factorizeBuffered(colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[Doubles.BYTES]);
    agg.init(buffer, 0);

    aggregate(selector, agg, buffer, 0);
    aggregate(selector, agg, buffer, 0);
    aggregate(selector, agg, buffer, 0);
    aggregate(selector, agg, buffer, 0);

    Assert.assertEquals(values[2], ((Double) agg.get(buffer, 0)).doubleValue(), 0.0001);
    Assert.assertEquals((long) values[2], agg.getLong(buffer, 0));
    Assert.assertEquals(values[2], agg.getFloat(buffer, 0), 0.0001);
  }

  @Test
  public void testCombine()
  {
    Assert.assertEquals(3.4d, ((Double) doubleMaxAggFactory.combine(1.2, 3.4)).doubleValue(), 0.0001);
  }

  @Test
  public void testEqualsAndHashCode() throws Exception
  {
    DoubleMaxAggregatorFactory one = new DoubleMaxAggregatorFactory("name1", "fieldName1");
    DoubleMaxAggregatorFactory oneMore = new DoubleMaxAggregatorFactory("name1", "fieldName1");
    DoubleMaxAggregatorFactory two = new DoubleMaxAggregatorFactory("name2", "fieldName2");

    Assert.assertEquals(one.hashCode(), oneMore.hashCode());

    Assert.assertTrue(one.equals(oneMore));
    Assert.assertFalse(one.equals(two));
  }

  private void aggregate(TestDoubleColumnSelectorImpl selector, DoubleMaxAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  private void aggregate(TestDoubleColumnSelectorImpl selector, DoubleMaxBufferAggregator agg, ByteBuffer buff, int position)
  {
    agg.aggregate(buff, position);
    selector.increment();
  }
}

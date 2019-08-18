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

package org.apache.druid.query.aggregation;

import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.TestHelper;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 */
public class DoubleMinAggregationTest
{
  private DoubleMinAggregatorFactory doubleMinAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestDoubleColumnSelectorImpl selector;

  private double[] values = {3.5d, 2.7d, 1.1d, 1.3d};

  public DoubleMinAggregationTest() throws Exception
  {
    String aggSpecJson = "{\"type\": \"doubleMin\", \"name\": \"billy\", \"fieldName\": \"nilly\"}";
    doubleMinAggFactory = TestHelper.makeJsonMapper().readValue(aggSpecJson, DoubleMinAggregatorFactory.class);
  }

  @Before
  public void setup()
  {
    selector = new TestDoubleColumnSelectorImpl(values);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(selector);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly")).andReturn(null);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testDoubleMinAggregator()
  {
    Aggregator agg = doubleMinAggFactory.factorize(colSelectorFactory);

    aggregate(selector, agg);
    aggregate(selector, agg);
    aggregate(selector, agg);
    aggregate(selector, agg);

    Assert.assertEquals(values[2], ((Double) agg.get()).doubleValue(), 0.0001);
    Assert.assertEquals((long) values[2], agg.getLong());
    Assert.assertEquals(values[2], agg.getFloat(), 0.0001);
  }

  @Test
  public void testDoubleMinBufferAggregator()
  {
    BufferAggregator agg = doubleMinAggFactory.factorizeBuffered(colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[Double.BYTES + Byte.BYTES]);
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
    Assert.assertEquals(1.2d, ((Double) doubleMinAggFactory.combine(1.2, 3.4)).doubleValue(), 0.0001);
  }

  @Test
  public void testEqualsAndHashCode()
  {
    DoubleMinAggregatorFactory one = new DoubleMinAggregatorFactory("name1", "fieldName1");
    DoubleMinAggregatorFactory oneMore = new DoubleMinAggregatorFactory("name1", "fieldName1");
    DoubleMinAggregatorFactory two = new DoubleMinAggregatorFactory("name2", "fieldName2");

    Assert.assertEquals(one.hashCode(), oneMore.hashCode());

    Assert.assertTrue(one.equals(oneMore));
    Assert.assertFalse(one.equals(two));
  }

  private void aggregate(TestDoubleColumnSelectorImpl selector, Aggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  private void aggregate(TestDoubleColumnSelectorImpl selector, BufferAggregator agg, ByteBuffer buff, int position)
  {
    agg.aggregate(buff, position);
    selector.increment();
  }
}

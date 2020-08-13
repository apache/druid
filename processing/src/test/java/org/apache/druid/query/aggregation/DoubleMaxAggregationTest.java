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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;
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
  private DoubleMaxAggregatorFactory doubleMaxVectorAggFactory;

  private ColumnSelectorFactory colSelectorFactory;
  private VectorColumnSelectorFactory vectorColumnSelectorFactory;
  private TestDoubleColumnSelectorImpl selector;

  private double[] values = {1.1d, 2.7d, 3.5d, 1.3d};
  private final double[] doubleValues1 = {5d, 2d, 4d, 100d, 1d, 5d, -2d, -3d, 0d, 55d};

  public DoubleMaxAggregationTest() throws Exception
  {
    String aggSpecJson = "{\"type\": \"doubleMax\", \"name\": \"billy\", \"fieldName\": \"nilly\"}";
    doubleMaxAggFactory = TestHelper.makeJsonMapper().readValue(aggSpecJson, DoubleMaxAggregatorFactory.class);

    String vectorAggSpecJson = "{\"type\": \"doubleMax\", \"name\": \"dbl\", \"fieldName\": \"dblFld\"}";
    doubleMaxVectorAggFactory = TestHelper.makeJsonMapper().readValue(vectorAggSpecJson, DoubleMaxAggregatorFactory.class);
  }

  @Before
  public void setup()
  {
    NullHandling.initializeForTests();
    selector = new TestDoubleColumnSelectorImpl(values);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(selector);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly")).andReturn(null);
    EasyMock.replay(colSelectorFactory);

    VectorValueSelector vectorValueSelector = EasyMock.createMock(VectorValueSelector.class);
    EasyMock.expect(vectorValueSelector.getDoubleVector()).andReturn(doubleValues1).anyTimes();
    EasyMock.expect(vectorValueSelector.getNullVector()).andReturn(null).anyTimes();
    EasyMock.replay(vectorValueSelector);

    vectorColumnSelectorFactory = EasyMock.createMock(VectorColumnSelectorFactory.class);
    EasyMock.expect(vectorColumnSelectorFactory.getColumnCapabilities("dblFld"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ValueType.DOUBLE).setDictionaryEncoded(true)).anyTimes();
    EasyMock.expect(vectorColumnSelectorFactory.makeValueSelector("dblFld")).andReturn(vectorValueSelector).anyTimes();
    EasyMock.replay(vectorColumnSelectorFactory);
  }

  @Test
  public void testDoubleMaxAggregator()
  {
    Aggregator agg = doubleMaxAggFactory.factorize(colSelectorFactory);

    aggregate(selector, agg);
    aggregate(selector, agg);
    aggregate(selector, agg);
    aggregate(selector, agg);

    Assert.assertEquals(values[2], ((Double) agg.get()).doubleValue(), 0.0001);
    Assert.assertEquals((long) values[2], agg.getLong());
    Assert.assertEquals(values[2], agg.getFloat(), 0.0001);
  }

  @Test
  public void testDoubleMaxBufferAggregator()
  {
    BufferAggregator agg = doubleMaxAggFactory.factorizeBuffered(colSelectorFactory);

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
  public void testDoubleMaxVectorAggregator()
  {
    // Some sanity.
    Assert.assertTrue(doubleMaxVectorAggFactory.canVectorize(vectorColumnSelectorFactory));
    VectorValueSelector vectorValueSelector = doubleMaxVectorAggFactory.vectorSelector(vectorColumnSelectorFactory);
    Assert.assertEquals(doubleValues1, vectorValueSelector.getDoubleVector());

    VectorAggregator vectorAggregator = doubleMaxVectorAggFactory.factorizeVector(vectorColumnSelectorFactory);

    final ByteBuffer buf = ByteBuffer.allocate(doubleMaxVectorAggFactory.getMaxIntermediateSizeWithNulls() * 3);
    vectorAggregator.init(buf, 0);
    vectorAggregator.aggregate(buf, 0, 0, 3);
    Assert.assertEquals(doubleValues1[0], vectorAggregator.get(buf, 0));

    vectorAggregator.init(buf, 8);
    vectorAggregator.aggregate(buf, 8, 0, 3);
    Assert.assertEquals(doubleValues1[0], vectorAggregator.get(buf, 8));

    vectorAggregator.init(buf, 16);
    vectorAggregator.aggregate(buf, 16, 4, 7);
    Assert.assertEquals(doubleValues1[5], vectorAggregator.get(buf, 16));

    vectorAggregator.init(buf, 0);
    vectorAggregator.aggregate(buf, 0, 0, 10);
    Assert.assertEquals(doubleValues1[3], vectorAggregator.get(buf, 0));
  }

  @Test
  public void testCombine()
  {
    Assert.assertEquals(3.4d, ((Double) doubleMaxAggFactory.combine(1.2, 3.4)).doubleValue(), 0.0001);
  }

  @Test
  public void testEqualsAndHashCode()
  {
    DoubleMaxAggregatorFactory one = new DoubleMaxAggregatorFactory("name1", "fieldName1");
    DoubleMaxAggregatorFactory oneMore = new DoubleMaxAggregatorFactory("name1", "fieldName1");
    DoubleMaxAggregatorFactory two = new DoubleMaxAggregatorFactory("name2", "fieldName2");

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

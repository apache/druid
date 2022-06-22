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
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;
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
  private DoubleMinAggregatorFactory doubleMinVectorAggFactory;

  private ColumnSelectorFactory colSelectorFactory;
  private VectorColumnSelectorFactory vectorColumnSelectorFactory;
  private TestDoubleColumnSelectorImpl selector;

  private final double[] values = {3.5d, 2.7d, 1.1d, 1.3d};
  private final double[] doubleValues1 = {5d, 2d, 4d, 100d, 1d, 5d, -2d, -3d, 0d, 55d};

  public DoubleMinAggregationTest() throws Exception
  {
    String aggSpecJson = "{\"type\": \"doubleMin\", \"name\": \"billy\", \"fieldName\": \"nilly\"}";
    doubleMinAggFactory = TestHelper.makeJsonMapper().readValue(aggSpecJson, DoubleMinAggregatorFactory.class);

    String vectorAggSpecJson = "{\"type\": \"doubleMin\", \"name\": \"dbl\", \"fieldName\": \"dblFld\"}";
    doubleMinVectorAggFactory = TestHelper.makeJsonMapper().readValue(vectorAggSpecJson, DoubleMinAggregatorFactory.class);
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
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.DOUBLE).setDictionaryEncoded(true)).anyTimes();
    EasyMock.expect(vectorColumnSelectorFactory.makeValueSelector("dblFld")).andReturn(vectorValueSelector).anyTimes();
    EasyMock.replay(vectorColumnSelectorFactory);
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
  public void testDoubleMinVectorAggregator()
  {
    // Some sanity.
    Assert.assertTrue(doubleMinVectorAggFactory.canVectorize(vectorColumnSelectorFactory));
    VectorValueSelector vectorValueSelector = doubleMinVectorAggFactory.vectorSelector(vectorColumnSelectorFactory);
    Assert.assertEquals(doubleValues1, vectorValueSelector.getDoubleVector());

    VectorAggregator vectorAggregator = doubleMinVectorAggFactory.factorizeVector(vectorColumnSelectorFactory);

    final ByteBuffer buf = ByteBuffer.allocate(doubleMinVectorAggFactory.getMaxIntermediateSizeWithNulls() * 3);
    vectorAggregator.init(buf, 0);
    vectorAggregator.aggregate(buf, 0, 0, 3);
    Assert.assertEquals(doubleValues1[1], vectorAggregator.get(buf, 0));

    vectorAggregator.init(buf, 8);
    vectorAggregator.aggregate(buf, 8, 0, 3);
    Assert.assertEquals(doubleValues1[1], vectorAggregator.get(buf, 8));

    vectorAggregator.init(buf, 16);
    vectorAggregator.aggregate(buf, 16, 3, 7);
    Assert.assertEquals(doubleValues1[6], vectorAggregator.get(buf, 16));

    vectorAggregator.init(buf, 0);
    vectorAggregator.aggregate(buf, 0, 0, 10);
    Assert.assertEquals(doubleValues1[7], vectorAggregator.get(buf, 0));
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

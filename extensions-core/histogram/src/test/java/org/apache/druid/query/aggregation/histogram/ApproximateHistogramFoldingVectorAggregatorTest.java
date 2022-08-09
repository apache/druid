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

package org.apache.druid.query.aggregation.histogram;

import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;

public class ApproximateHistogramFoldingVectorAggregatorTest
{
  private static final float[] FLOATS = {23, 19, 10, 16, 36, 2, 9, 32, 30, 45};
  private VectorColumnSelectorFactory vectorColumnSelectorFactory;
  private ApproximateHistogram h1;
  private ApproximateHistogram h2;

  @Before
  public void setup()
  {

    h1 = new ApproximateHistogram(5);
    h2 = new ApproximateHistogram(5);

    for (int i = 0; i < 5; ++i) {
      h1.offer(FLOATS[i]);
    }
    for (int i = 5; i < FLOATS.length; ++i) {
      h2.offer(FLOATS[i]);
    }

    VectorObjectSelector vectorObjectSelector = createMock(VectorObjectSelector.class);
    expect(vectorObjectSelector.getObjectVector()).andReturn(new Object[]{h1, null, h2, null}).anyTimes();

    EasyMock.replay(vectorObjectSelector);

    vectorColumnSelectorFactory = createMock(VectorColumnSelectorFactory.class);
    expect(vectorColumnSelectorFactory.makeObjectSelector("field"))
        .andReturn(vectorObjectSelector).anyTimes();
    expect(vectorColumnSelectorFactory.getColumnCapabilities("field")).andReturn(
        new ColumnCapabilitiesImpl().setType(ApproximateHistogramAggregatorFactory.TYPE)
    );
    expect(vectorColumnSelectorFactory.getColumnCapabilities("string_field")).andReturn(
        new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
    );
    expect(vectorColumnSelectorFactory.getColumnCapabilities("double_field")).andReturn(
        new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
    );
    EasyMock.replay(vectorColumnSelectorFactory);
  }

  @Test
  public void doNotVectorizedNonComplexTypes()
  {
    ApproximateHistogramFoldingAggregatorFactory factory = buildHistogramFactory("string_field");
    Assert.assertFalse(factory.canVectorize(vectorColumnSelectorFactory));

    factory = buildHistogramFactory("double_field");
    Assert.assertFalse(factory.canVectorize(vectorColumnSelectorFactory));
  }

  @Test
  public void testAggregateSinglePosition()
  {
    ApproximateHistogramFoldingAggregatorFactory factory = buildHistogramFactory();
    ByteBuffer byteBuffer = ByteBuffer.allocate(factory.getMaxIntermediateSize());
    Assert.assertTrue(factory.canVectorize(vectorColumnSelectorFactory));
    VectorAggregator vectorAggregator = factory.factorizeVector(vectorColumnSelectorFactory);
    vectorAggregator.init(byteBuffer, 0);
    vectorAggregator.aggregate(byteBuffer, 0, 0, 4);
    ApproximateHistogram h = (ApproximateHistogram) vectorAggregator.get(byteBuffer, 0);

    Assert.assertArrayEquals(new float[]{19.6f, 45.0f}, h.positions(), 0.1f);
    Assert.assertArrayEquals(new long[]{9, 1}, h.bins());
    Assert.assertEquals(10, h.count());
    Assert.assertEquals(2.0f, h.min(), 0.1f);
    Assert.assertEquals(45.0f, h.max(), 0.1f);
  }

  @Test
  public void testAggregateMultiPositions()
  {
    ApproximateHistogramFoldingAggregatorFactory factory = buildHistogramFactory();
    ByteBuffer byteBuffer = ByteBuffer.allocate(factory.getMaxIntermediateSize() * 2);
    int[] positions = new int[]{0, factory.getMaxIntermediateSize()};
    VectorAggregator vectorAggregator = factory.factorizeVector(vectorColumnSelectorFactory);
    vectorAggregator.init(byteBuffer, 0);
    vectorAggregator.init(byteBuffer, positions[1]);

    vectorAggregator.aggregate(byteBuffer, 2, positions, null, 0);
    vectorAggregator.aggregate(byteBuffer, 2, positions, new int[]{1, 2}, 0);  // indirection
    ApproximateHistogram actualH1 = (ApproximateHistogram) vectorAggregator.get(byteBuffer, 0);
    ApproximateHistogram actualH2 = (ApproximateHistogram) vectorAggregator.get(byteBuffer, positions[1]);

    Assert.assertEquals(actualH1, h1);
    Assert.assertEquals(actualH2, h2);

  }

  @Test
  public void testWithName()
  {
    ApproximateHistogramFoldingAggregatorFactory factory = buildHistogramFactory();
    Assert.assertEquals(factory, factory.withName("approximateHistoFold"));
    Assert.assertEquals("newTest", factory.withName("newTest").getName());
  }

  private ApproximateHistogramFoldingAggregatorFactory buildHistogramFactory()
  {
    return buildHistogramFactory("field");
  }

  private ApproximateHistogramFoldingAggregatorFactory buildHistogramFactory(String fieldName)
  {
    return new ApproximateHistogramFoldingAggregatorFactory(
        "approximateHistoFold",
        fieldName,
        5,
        5,
        0f,
        50.0f,
        false
    );
  }
}

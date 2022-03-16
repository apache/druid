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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;

public class ApproximateHistogramVectorAggregatorTest
{
  private static final float[] FLOATS = {23, 19, 10, 16, 36, 2, 9, 32, 30, 45, 33};   // Last value is never included
  private static final boolean[] NULL_VECTOR =
      {false, false, false, false, false, false, false, false, false, false, true};
  private VectorColumnSelectorFactory vectorColumnSelectorFactory;

  @Before
  public void setup()
  {
    NullHandling.initializeForTests();
    VectorValueSelector vectorValueSelector_1 = createMock(VectorValueSelector.class);
    expect(vectorValueSelector_1.getFloatVector()).andReturn(FLOATS).anyTimes();
    expect(vectorValueSelector_1.getNullVector()).andReturn(NULL_VECTOR).anyTimes();

    VectorValueSelector vectorValueSelector_2 = createMock(VectorValueSelector.class);
    expect(vectorValueSelector_2.getFloatVector()).andReturn(FLOATS).anyTimes();
    expect(vectorValueSelector_2.getNullVector()).andReturn(null).anyTimes();

    EasyMock.replay(vectorValueSelector_1);
    EasyMock.replay(vectorValueSelector_2);

    ColumnCapabilities columnCapabilities
        = ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE);
    vectorColumnSelectorFactory = createMock(VectorColumnSelectorFactory.class);
    expect(vectorColumnSelectorFactory.getColumnCapabilities("field_1")).andReturn(columnCapabilities).anyTimes();
    expect(vectorColumnSelectorFactory.makeValueSelector("field_1"))
        .andReturn(vectorValueSelector_1).anyTimes();
    expect(vectorColumnSelectorFactory.getColumnCapabilities("field_2")).andReturn(columnCapabilities).anyTimes();
    expect(vectorColumnSelectorFactory.makeValueSelector("field_2"))
        .andReturn(vectorValueSelector_2).anyTimes();
    expect(vectorColumnSelectorFactory.getColumnCapabilities("string_field")).andReturn(
        new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
    );
    expect(vectorColumnSelectorFactory.getColumnCapabilities("complex_field")).andReturn(
        new ColumnCapabilitiesImpl().setType(ApproximateHistogramAggregatorFactory.TYPE)
    );
    EasyMock.replay(vectorColumnSelectorFactory);
  }

  @Test
  public void doNotVectorizedNonNumericTypes()
  {
    ApproximateHistogramAggregatorFactory factory = buildHistogramAggFactory("string_field");
    Assert.assertFalse(factory.canVectorize(vectorColumnSelectorFactory));

    factory = buildHistogramAggFactory("complex_field");
    Assert.assertFalse(factory.canVectorize(vectorColumnSelectorFactory));
  }

  @Test
  public void testAggregateSinglePosition()
  {
    ApproximateHistogramAggregatorFactory factory = buildHistogramAggFactory("field_1");
    ByteBuffer byteBuffer = ByteBuffer.allocate(factory.getMaxIntermediateSizeWithNulls());
    Assert.assertTrue(factory.canVectorize(vectorColumnSelectorFactory));
    VectorAggregator vectorAggregator = factory.factorizeVector(vectorColumnSelectorFactory);
    vectorAggregator.init(byteBuffer, 0);
    vectorAggregator.aggregate(byteBuffer, 0, 0, 11);
    ApproximateHistogram h = (ApproximateHistogram) vectorAggregator.get(byteBuffer, 0);

    // (2, 1), (9.5, 2), (19.33, 3), (32.67, 3), (45, 1)
    Assert.assertArrayEquals(new float[]{2, 9.5f, 19.33f, 32.67f, 45f}, h.positions(), 0.1f);
    Assert.assertArrayEquals(new long[]{1, 2, 3, 3, 1}, h.bins());

    factory = buildHistogramAggFactory("field_2");
    vectorAggregator = factory.factorizeVector(vectorColumnSelectorFactory);
    vectorAggregator.init(byteBuffer, 0);
    vectorAggregator.aggregate(byteBuffer, 0, 0, 10);
    h = (ApproximateHistogram) vectorAggregator.get(byteBuffer, 0);

    Assert.assertArrayEquals(new float[]{2, 9.5f, 19.33f, 32.67f, 45f}, h.positions(), 0.1f);
    Assert.assertArrayEquals(new long[]{1, 2, 3, 3, 1}, h.bins());

  }

  @Test
  public void testAggregateMultiPositions()
  {
    ApproximateHistogramAggregatorFactory factory = buildHistogramAggFactory("field_2");
    int size = factory.getMaxIntermediateSize();
    ByteBuffer byteBuffer = ByteBuffer.allocate(size * 2);
    VectorAggregator vectorAggregator = factory.factorizeVector(vectorColumnSelectorFactory);
    int[] positions = new int[]{0, size};
    vectorAggregator.init(byteBuffer, positions[0]);
    vectorAggregator.init(byteBuffer, positions[1]);
    vectorAggregator.aggregate(byteBuffer, 2, positions, null, 0);
    // Put rest of 10 elements using the access indirection. Second vector gets the same element always
    for (int i = 1; i < 10; i++) {
      vectorAggregator.aggregate(byteBuffer, 2, positions, new int[]{i, 1}, 0);
    }

    ApproximateHistogram h0 = (ApproximateHistogram) vectorAggregator.get(byteBuffer, 0);
    Assert.assertArrayEquals(new float[]{2, 9.5f, 19.33f, 32.67f, 45f}, h0.positions(), 0.1f);
    Assert.assertArrayEquals(new long[]{1, 2, 3, 3, 1}, h0.bins());

    ApproximateHistogram h2 = (ApproximateHistogram) vectorAggregator.get(byteBuffer, size);
    Assert.assertArrayEquals(new float[]{19}, h2.positions(), 0.1f);
    Assert.assertArrayEquals(new long[]{10}, h2.bins());
  }

  private ApproximateHistogramAggregatorFactory buildHistogramAggFactory(String fieldName)
  {
    return new ApproximateHistogramAggregatorFactory(
        "approxHisto",
        fieldName,
        5,
        5,
        0.0f,
        45.0f,
        false
    );
  }
}

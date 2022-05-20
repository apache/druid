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

public class FixedBucketsHistogramVectorAggregatorTest
{
  private static final double[] DOUBLES = {1.0, 12.0, 3.0, 14.0, 15.0, 16.0};
  private static final boolean[] NULL_VECTOR = {false, false, false, false, true, false};
  private VectorColumnSelectorFactory vectorColumnSelectorFactory;

  @Before
  public void setup()
  {
    NullHandling.initializeForTests();
    VectorValueSelector vectorValueSelector_1 = createMock(VectorValueSelector.class);
    expect(vectorValueSelector_1.getDoubleVector()).andReturn(DOUBLES).anyTimes();
    expect(vectorValueSelector_1.getNullVector()).andReturn(NULL_VECTOR).anyTimes();

    VectorValueSelector vectorValueSelector_2 = createMock(VectorValueSelector.class);
    expect(vectorValueSelector_2.getDoubleVector()).andReturn(DOUBLES).anyTimes();
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
    EasyMock.replay(vectorColumnSelectorFactory);
  }

  @Test
  public void testAggregateSinglePosition()
  {
    ByteBuffer byteBuffer = ByteBuffer.allocate(FixedBucketsHistogram.getFullStorageSize(2));
    FixedBucketsHistogramAggregatorFactory factory = buildHistogramAggFactory("field_1");
    Assert.assertTrue(factory.canVectorize(vectorColumnSelectorFactory));
    VectorAggregator vectorAggregator = factory.factorizeVector(vectorColumnSelectorFactory);
    vectorAggregator.init(byteBuffer, 0);
    vectorAggregator.aggregate(byteBuffer, 0, 0, 6);
    FixedBucketsHistogram h = (FixedBucketsHistogram) vectorAggregator.get(byteBuffer, 0);

    Assert.assertEquals(2, h.getNumBuckets());
    Assert.assertEquals(10.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(1, h.getLowerLimit(), 0.01);
    Assert.assertEquals(21, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 3}, h.getHistogram());
    Assert.assertEquals(5, h.getCount());
    Assert.assertEquals(1.0, h.getMin(), 0.01);
    Assert.assertEquals(16.0, h.getMax(), 0.01);
    // Default value of null is 0 which is an outlier.
    Assert.assertEquals(NullHandling.replaceWithDefault() ? 0 : 1, h.getMissingValueCount());
    Assert.assertEquals(NullHandling.replaceWithDefault() ? 1 : 0, h.getLowerOutlierCount());
    Assert.assertEquals(0, h.getUpperOutlierCount());

    factory = buildHistogramAggFactory("field_2");
    vectorAggregator = factory.factorizeVector(vectorColumnSelectorFactory);
    vectorAggregator.init(byteBuffer, 0);
    vectorAggregator.aggregate(byteBuffer, 0, 0, 6);
    h = (FixedBucketsHistogram) vectorAggregator.get(byteBuffer, 0);

    Assert.assertEquals(2, h.getNumBuckets());
    Assert.assertEquals(10.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(1, h.getLowerLimit(), 0.01);
    Assert.assertEquals(21, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 4}, h.getHistogram());
    Assert.assertEquals(6, h.getCount());
    Assert.assertEquals(1.0, h.getMin(), 0.01);
    Assert.assertEquals(16.0, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(0, h.getLowerOutlierCount());
    Assert.assertEquals(0, h.getUpperOutlierCount());
  }

  @Test
  public void testAggregateMultiPositions()
  {
    int size = FixedBucketsHistogram.getFullStorageSize(2);
    ByteBuffer byteBuffer = ByteBuffer.allocate(size * 2);
    FixedBucketsHistogramAggregatorFactory factory = buildHistogramAggFactory("field_2");
    VectorAggregator vectorAggregator = factory.factorizeVector(vectorColumnSelectorFactory);
    int[] positions = new int[]{0, size};
    vectorAggregator.init(byteBuffer, positions[0]);
    vectorAggregator.init(byteBuffer, positions[1]);
    vectorAggregator.aggregate(byteBuffer, 2, positions, null, 0);

    FixedBucketsHistogram h0 = (FixedBucketsHistogram) vectorAggregator.get(byteBuffer, 0);

    Assert.assertEquals(2, h0.getNumBuckets());
    Assert.assertEquals(10.0, h0.getBucketSize(), 0.01);
    Assert.assertEquals(1, h0.getLowerLimit(), 0.01);
    Assert.assertEquals(21, h0.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h0.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 0}, h0.getHistogram());
    Assert.assertEquals(1, h0.getCount());
    Assert.assertEquals(1.0, h0.getMin(), 0.01);
    Assert.assertEquals(1.0, h0.getMax(), 0.01);
    Assert.assertEquals(0, h0.getMissingValueCount());
    Assert.assertEquals(0, h0.getLowerOutlierCount());
    Assert.assertEquals(0, h0.getUpperOutlierCount());

    FixedBucketsHistogram h1 = (FixedBucketsHistogram) vectorAggregator.get(byteBuffer, positions[1]);

    Assert.assertEquals(2, h1.getNumBuckets());
    Assert.assertEquals(10.0, h1.getBucketSize(), 0.01);
    Assert.assertEquals(1, h1.getLowerLimit(), 0.01);
    Assert.assertEquals(21, h1.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h1.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{0, 1}, h1.getHistogram());
    Assert.assertEquals(1, h1.getCount());
    Assert.assertEquals(12.0, h1.getMin(), 0.01);
    Assert.assertEquals(12.0, h1.getMax(), 0.01);
    Assert.assertEquals(0, h1.getMissingValueCount());
    Assert.assertEquals(0, h1.getLowerOutlierCount());
    Assert.assertEquals(0, h1.getUpperOutlierCount());

    // Tests when there is a level of indirection in accessing the vector
    byteBuffer = ByteBuffer.allocate(size * 2);
    vectorAggregator.init(byteBuffer, positions[0]);
    vectorAggregator.init(byteBuffer, positions[1]);
    vectorAggregator.aggregate(byteBuffer, 2, positions, new int[]{2, 3}, 0);

    FixedBucketsHistogram h2 = (FixedBucketsHistogram) vectorAggregator.get(byteBuffer, 0);

    Assert.assertEquals(2, h2.getNumBuckets());
    Assert.assertEquals(10.0, h2.getBucketSize(), 0.01);
    Assert.assertEquals(1, h2.getLowerLimit(), 0.01);
    Assert.assertEquals(21, h2.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h2.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 0}, h2.getHistogram());
    Assert.assertEquals(1, h2.getCount());
    Assert.assertEquals(3.0, h2.getMin(), 0.01);
    Assert.assertEquals(3.0, h2.getMax(), 0.01);
    Assert.assertEquals(0, h2.getMissingValueCount());
    Assert.assertEquals(0, h2.getLowerOutlierCount());
    Assert.assertEquals(0, h2.getUpperOutlierCount());

    FixedBucketsHistogram h3 = (FixedBucketsHistogram) vectorAggregator.get(byteBuffer, positions[1]);

    Assert.assertEquals(2, h3.getNumBuckets());
    Assert.assertEquals(10.0, h3.getBucketSize(), 0.01);
    Assert.assertEquals(1, h3.getLowerLimit(), 0.01);
    Assert.assertEquals(21, h3.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h3.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{0, 1}, h3.getHistogram());
    Assert.assertEquals(1, h3.getCount());
    Assert.assertEquals(14.0, h3.getMin(), 0.01);
    Assert.assertEquals(14.0, h3.getMax(), 0.01);
    Assert.assertEquals(0, h3.getMissingValueCount());
    Assert.assertEquals(0, h3.getLowerOutlierCount());
    Assert.assertEquals(0, h3.getUpperOutlierCount());

  }

  private FixedBucketsHistogramAggregatorFactory buildHistogramAggFactory(String fieldName)
  {
    return new FixedBucketsHistogramAggregatorFactory(
        "fixedHisto",
        fieldName,
        2,
        1,
        21,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        false
    );
  }
}

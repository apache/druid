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

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.TestFloatColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class FixedBucketsHistogramBufferAggregatorTest
{
  private void aggregateBuffer(TestFloatColumnSelector selector, BufferAggregator agg, ByteBuffer buf, int position)
  {
    agg.aggregate(buf, position);
    selector.increment();
  }

  @Test
  public void testBufferAggregate()
  {
    final float[] values = {23, 19, 10, 16, 36, 2, 9, 32, 30, 45};

    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    FixedBucketsHistogramAggregatorFactory factory = new FixedBucketsHistogramAggregatorFactory(
        "billy",
        "billy",
        5,
        0,
        50,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        false
    );

    FixedBucketsHistogramBufferAggregator agg = new FixedBucketsHistogramBufferAggregator(
        selector,
        0,
        50,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW
    );

    ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSizeWithNulls());
    int position = 0;

    agg.init(buf, position);
    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < values.length; i++) {
      aggregateBuffer(selector, agg, buf, position);
    }

    FixedBucketsHistogram h = ((FixedBucketsHistogram) agg.get(buf, position));

    Assert.assertArrayEquals(
        "final bin counts don't match expected counts",
        new long[]{2, 3, 1, 3, 1}, h.getHistogram()
    );

    Assert.assertEquals("getMin value doesn't match expected getMin", 2, h.getMin(), 0);
    Assert.assertEquals("getMax value doesn't match expected getMax", 45, h.getMax(), 0);

    Assert.assertEquals("count doesn't match expected count", 10, h.getCount());
  }

  @Test
  public void testFinalize() throws Exception
  {
    DefaultObjectMapper objectMapper = new DefaultObjectMapper();

    final float[] values = {23, 19, 10, 16, 36, 2, 9, 32, 30, 45};

    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    FixedBucketsHistogramAggregatorFactory humanReadableFactory = new FixedBucketsHistogramAggregatorFactory(
        "billy",
        "billy",
        5,
        0,
        50,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        false
    );

    FixedBucketsHistogramAggregatorFactory binaryFactory = new FixedBucketsHistogramAggregatorFactory(
        "billy",
        "billy",
        5,
        0,
        50,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        true
    );

    FixedBucketsHistogramAggregator agg = new FixedBucketsHistogramAggregator(
        selector,
        0,
        50,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW
    );
    agg.aggregate();

    Object finalizedObjectHumanReadable = humanReadableFactory.finalizeComputation(agg.get());
    String finalStringHumanReadable = objectMapper.writeValueAsString(finalizedObjectHumanReadable);
    Assert.assertEquals(
        "\"{lowerLimit=0.0, upperLimit=50.0, numBuckets=5, upperOutlierCount=0, lowerOutlierCount=0, missingValueCount=0, histogram=[0, 0, 1, 0, 0], outlierHandlingMode=overflow, count=1, max=23.0, min=23.0}\"",
        finalStringHumanReadable
    );

    Object finalizedObjectBinary = binaryFactory.finalizeComputation(agg.get());
    String finalStringBinary = objectMapper.writeValueAsString(finalizedObjectBinary);
    Assert.assertEquals(
        "\"AQIAAAAAAAAAAEBJAAAAAAAAAAAABQEAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEA3AAAAAAAAQDcAAAAAAAAAAAABAAAAAgAAAAAAAAAB\"",
        finalStringBinary
    );
  }
}

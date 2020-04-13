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
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class ApproximateHistogramAggregatorTest extends InitializedNullHandlingTest
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
    final int resolution = 5;
    final int numBuckets = 5;

    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    ApproximateHistogramAggregatorFactory factory = new ApproximateHistogramAggregatorFactory(
        "billy", "billy", resolution, numBuckets, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, false
    );
    ApproximateHistogramBufferAggregator agg = new ApproximateHistogramBufferAggregator(selector, resolution);

    ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSizeWithNulls());
    int position = 0;

    agg.init(buf, position);
    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < values.length; i++) {
      aggregateBuffer(selector, agg, buf, position);
    }

    ApproximateHistogram h = ((ApproximateHistogram) agg.get(buf, position));

    Assert.assertArrayEquals(
        "final bin positions don't match expected positions",
        new float[]{2, 9.5f, 19.33f, 32.67f, 45f}, h.positions, 0.01f
    );

    Assert.assertArrayEquals(
        "final bin counts don't match expected counts",
        new long[]{1, 2, 3, 3, 1}, h.bins()
    );

    Assert.assertEquals("getMin value doesn't match expected getMin", 2, h.min(), 0);
    Assert.assertEquals("getMax value doesn't match expected getMax", 45, h.max(), 0);

    Assert.assertEquals("bin count doesn't match expected bin count", 5, h.binCount());
  }

  @Test
  public void testFinalize() throws Exception
  {
    DefaultObjectMapper objectMapper = new DefaultObjectMapper();

    final float[] values = {23, 19, 10, 16, 36, 2, 9, 32, 30, 45};
    final int resolution = 5;
    final int numBuckets = 5;

    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    ApproximateHistogramAggregatorFactory humanReadableFactory = new ApproximateHistogramAggregatorFactory(
        "billy", "billy", resolution, numBuckets, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, false
    );

    ApproximateHistogramAggregatorFactory binaryFactory = new ApproximateHistogramAggregatorFactory(
        "billy", "billy", resolution, numBuckets, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, true
    );

    ApproximateHistogramAggregator agg = new ApproximateHistogramAggregator(selector, resolution, 0, 100);
    agg.aggregate();

    Object finalizedObjectHumanReadable = humanReadableFactory.finalizeComputation(agg.get());
    String finalStringHumanReadable = objectMapper.writeValueAsString(finalizedObjectHumanReadable);
    Assert.assertEquals(
        "{\"breaks\":[23.0,23.0,23.0,23.0,23.0,23.0],\"counts\":[0.0,0.0,0.0,0.0,0.0]}",
        finalStringHumanReadable
    );

    Object finalizedObjectBinary = binaryFactory.finalizeComputation(agg.get());
    String finalStringBinary = objectMapper.writeValueAsString(finalizedObjectBinary);
    Assert.assertEquals(
        "\"//sBQbgAAA==\"",
        finalStringBinary
    );
  }
}

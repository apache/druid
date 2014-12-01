/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation.histogram;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.TestFloatColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class ApproximateHistogramAggregatorTest
{
  private void aggregateBuffer(TestFloatColumnSelector selector, BufferAggregator agg, ByteBuffer buf, int position)
  {
    agg.aggregate(buf, position);
    selector.increment();
  }

  @Test
  public void testBufferAggregate() throws Exception
  {
    final float[] values = {23, 19, 10, 16, 36, 2, 9, 32, 30, 45};
    final int resolution = 5;
    final int numBuckets = 5;

    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    ApproximateHistogramAggregatorFactory factory = new ApproximateHistogramAggregatorFactory(
        "billy", "billy", resolution, numBuckets, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY
    );
    ApproximateHistogramBufferAggregator agg = new ApproximateHistogramBufferAggregator(selector, resolution, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY);

    ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize());
    int position = 0;

    agg.init(buf, position);
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
}

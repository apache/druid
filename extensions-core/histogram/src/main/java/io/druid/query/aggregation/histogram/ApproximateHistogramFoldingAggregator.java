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

package io.druid.query.aggregation.histogram;


import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

public class ApproximateHistogramFoldingAggregator implements Aggregator
{
  private final ObjectColumnSelector<ApproximateHistogram> selector;
  private final int resolution;
  private final float lowerLimit;
  private final float upperLimit;

  private ApproximateHistogram histogram;
  private float[] tmpBufferP;
  private long[] tmpBufferB;

  public ApproximateHistogramFoldingAggregator(
      ObjectColumnSelector<ApproximateHistogram> selector,
      int resolution,
      float lowerLimit,
      float upperLimit
  )
  {
    this.selector = selector;
    this.resolution = resolution;
    this.lowerLimit = lowerLimit;
    this.upperLimit = upperLimit;
    this.histogram = new ApproximateHistogram(resolution, lowerLimit, upperLimit);

    tmpBufferP = new float[resolution];
    tmpBufferB = new long[resolution];
  }

  @Override
  public void aggregate()
  {
    ApproximateHistogram h = selector.get();
    if (h == null) {
      return;
    }

    if (h.binCount() + histogram.binCount() <= tmpBufferB.length) {
      histogram.foldFast(h, tmpBufferP, tmpBufferB);
    } else {
      histogram.foldFast(h);
    }
  }

  @Override
  public void reset()
  {
    this.histogram = new ApproximateHistogram(resolution, lowerLimit, upperLimit);
  }

  @Override
  public Object get()
  {
    return histogram;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("ApproximateHistogramFoldingAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("ApproximateHistogramFoldingAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("ApproximateHistogramFoldingAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}

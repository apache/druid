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

import java.nio.ByteBuffer;

/**
 * A helper class used by {@link ApproximateHistogramBufferAggregator} and {@link ApproximateHistogramVectorAggregator}
 * for aggregation operations on byte buffers. Getting the object from value selectors is outside this class.
 */
final class ApproximateHistogramBufferAggregatorHelper
{
  private final int resolution;

  public ApproximateHistogramBufferAggregatorHelper(int resolution)
  {
    this.resolution = resolution;
  }

  public void init(final ByteBuffer buf, final int position)
  {
    ApproximateHistogram histogram = new ApproximateHistogram(resolution);
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    histogram.toBytesDense(mutationBuffer);
  }

  public ApproximateHistogram get(final ByteBuffer buf, final int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    return ApproximateHistogram.fromBytesDense(mutationBuffer);
  }

  public void put(final ByteBuffer buf, final int position, final ApproximateHistogram histogram)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    histogram.toBytesDense(mutationBuffer);
  }

  public void aggregate(final ByteBuffer buf, final int position, final float value)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    ApproximateHistogram h0 = ApproximateHistogram.fromBytesDense(mutationBuffer);
    h0.offer(value);

    mutationBuffer.position(position);
    h0.toBytesDense(mutationBuffer);
  }
}

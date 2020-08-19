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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class ApproximateHistogramFoldingBufferAggregatorInternal
{
  private final int resolution;
  private final float upperLimit;
  private final float lowerLimit;

  private float[] tmpBufferA;
  private long[] tmpBufferB;

  public ApproximateHistogramFoldingBufferAggregatorInternal(
      int resolution,
      float lowerLimit,
      float upperLimit
  )
  {
    this.resolution = resolution;
    this.lowerLimit = lowerLimit;
    this.upperLimit = upperLimit;

    tmpBufferA = new float[resolution];
    tmpBufferB = new long[resolution];
  }

  public void init(ByteBuffer buf, int position)
  {
    ApproximateHistogram h = new ApproximateHistogram(resolution, lowerLimit, upperLimit);

    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    // use dense storage for aggregation
    h.toBytesDense(mutationBuffer);
  }

  public void aggregate(ByteBuffer buf, int position, @Nullable ApproximateHistogram hNext)
  {
    if (hNext == null) {
      return;
    }
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    ApproximateHistogram h0 = ApproximateHistogram.fromBytesDense(mutationBuffer);
    foldFast(h0, hNext);

    mutationBuffer.position(position);
    h0.toBytesDense(mutationBuffer);
  }

  public void foldFast(ApproximateHistogram left, ApproximateHistogram right)
  {
    //TODO: do these have to set in every call
    left.setLowerLimit(lowerLimit);
    left.setUpperLimit(upperLimit);
    left.foldFast(right, tmpBufferA, tmpBufferB);
  }

  public ApproximateHistogram get(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.asReadOnlyBuffer();
    mutationBuffer.position(position);
    return ApproximateHistogram.fromBytesDense(mutationBuffer);
  }

  public void put(ByteBuffer buf, int position, ApproximateHistogram histogram)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    histogram.toBytesDense(mutationBuffer);
  }
}

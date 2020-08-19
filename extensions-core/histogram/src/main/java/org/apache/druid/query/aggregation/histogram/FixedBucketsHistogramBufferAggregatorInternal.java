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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class FixedBucketsHistogramBufferAggregatorInternal
{
  private final double lowerLimit;
  private final double upperLimit;
  private final int numBuckets;
  private final FixedBucketsHistogram.OutlierHandlingMode outlierHandlingMode;

  public FixedBucketsHistogramBufferAggregatorInternal(
      double lowerLimit,
      double upperLimit,
      int numBuckets,
      FixedBucketsHistogram.OutlierHandlingMode outlierHandlingMode
  )
  {

    this.lowerLimit = lowerLimit;
    this.upperLimit = upperLimit;
    this.numBuckets = numBuckets;
    this.outlierHandlingMode = outlierHandlingMode;
  }

  public void init(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    FixedBucketsHistogram histogram = new FixedBucketsHistogram(
        lowerLimit,
        upperLimit,
        numBuckets,
        outlierHandlingMode
    );
    mutationBuffer.put(histogram.toBytesFull(false));
  }

  public void aggregate(ByteBuffer buf, int position, @Nullable Object val)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    FixedBucketsHistogram h0 = FixedBucketsHistogram.fromByteBufferFullNoSerdeHeader(mutationBuffer);
    combine(h0, val);

    mutationBuffer.position(position);
    mutationBuffer.put(h0.toBytesFull(false));
  }

  public void combine(FixedBucketsHistogram histogram, @Nullable Object next)
  {
    if (next == null) {
      if (NullHandling.replaceWithDefault()) {
        histogram.incrementMissing();
      } else {
        histogram.add(NullHandling.defaultDoubleValue());
      }
    } else if (next instanceof String) {
      histogram.combineHistogram(FixedBucketsHistogram.fromBase64((String) next));
    } else if (next instanceof FixedBucketsHistogram) {
      histogram.combineHistogram((FixedBucketsHistogram) next);
    } else {
      Double x = ((Number) next).doubleValue();
      histogram.add(x);
    }
  }

  public FixedBucketsHistogram get(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    return FixedBucketsHistogram.fromByteBufferFullNoSerdeHeader(mutationBuffer);
  }

  public void put(ByteBuffer buf, int position, FixedBucketsHistogram histogram)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    mutationBuffer.put(histogram.toBytesFull(false));
  }
}

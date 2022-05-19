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
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class ApproximateHistogramFoldingVectorAggregator implements VectorAggregator
{
  private final ApproximateHistogramFoldingBufferAggregatorHelper innerAggregator;
  private final VectorObjectSelector selector;

  public ApproximateHistogramFoldingVectorAggregator(
      final VectorObjectSelector selector,
      final int resolution,
      final float lowerLimit,
      final float upperLimit
  )
  {
    this.selector = selector;
    this.innerAggregator = new ApproximateHistogramFoldingBufferAggregatorHelper(resolution, lowerLimit, upperLimit);
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    innerAggregator.init(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    Object[] vector = selector.getObjectVector();
    ApproximateHistogram histogram = innerAggregator.get(buf, position);
    for (int i = startRow; i < endRow; i++) {
      ApproximateHistogram other = (ApproximateHistogram) vector[i];
      if (null != other) {
        innerAggregator.foldFast(histogram, other);
      }
    }
    innerAggregator.put(buf, position, histogram);
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    Object[] vector = selector.getObjectVector();
    for (int i = 0; i < numRows; i++) {
      ApproximateHistogram other = (ApproximateHistogram) vector[null != rows ? rows[i] : i];
      if (null == other) {
        continue;
      }
      int position = positions[i] + positionOffset;
      innerAggregator.aggregate(buf, position, other);
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return innerAggregator.get(buf, position);
  }

  @Override
  public void close()
  {
    // Nothing to close
  }
}

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
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class ApproximateHistogramVectorAggregator implements VectorAggregator
{

  private final VectorValueSelector selector;
  private final ApproximateHistogramBufferAggregatorHelper innerAggregator;

  public ApproximateHistogramVectorAggregator(
      VectorValueSelector selector,
      int resolution
  )
  {
    this.selector = selector;
    this.innerAggregator = new ApproximateHistogramBufferAggregatorHelper(resolution);
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    innerAggregator.init(buf, position);
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position, final int startRow, final int endRow)
  {
    final boolean[] isValueNull = selector.getNullVector();
    final float[] vector = selector.getFloatVector();
    ApproximateHistogram histogram = innerAggregator.get(buf, position);

    for (int i = startRow; i < endRow; i++) {
      if (isValueNull != null && isValueNull[i]) {
        continue;
      }
      histogram.offer(vector[i]);
    }
    innerAggregator.put(buf, position, histogram);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return innerAggregator.get(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    final float[] vector = selector.getFloatVector();
    final boolean[] isValueNull = selector.getNullVector();

    for (int i = 0; i < numRows; i++) {
      if (isValueNull != null && isValueNull[i]) {
        continue;
      }
      final int position = positions[i] + positionOffset;
      innerAggregator.aggregate(buf, position, vector[rows != null ? rows[i] : i]);
    }
  }


  @Override
  public void close()
  {
    // no resources to cleanup
  }
}

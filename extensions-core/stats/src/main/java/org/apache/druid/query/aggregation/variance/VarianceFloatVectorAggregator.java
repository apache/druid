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

package org.apache.druid.query.aggregation.variance;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Vectorized implementation of {@link VarianceBufferAggregator}
 */
public class VarianceFloatVectorAggregator implements VectorAggregator
{
  private final VectorValueSelector selector;
  private final boolean replaceWithDefault = NullHandling.replaceWithDefault();

  public VarianceFloatVectorAggregator(VectorValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    VarianceBufferAggregator.doInit(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    float[] vector = selector.getFloatVector();
    boolean[] nulls = selector.getNullVector();
    doAggregate(buf, position, startRow, endRow, nulls, vector);
  }

  @Override
  public void aggregate(
      ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset
  )
  {
    float[] vector = selector.getFloatVector();
    boolean[] nulls = selector.getNullVector();
    for (int i = 0; i < numRows; i++) {
      int position = positions[i] + positionOffset;
      int row = rows != null ? rows[i] : i;
      doAggregate(buf, position, row, row + 1, nulls, vector);
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return VarianceBufferAggregator.doGet(buf, position);
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }

  private void doAggregate(ByteBuffer buf, int position, int startRow, int endRow, @Nullable boolean[] nulls, float[] vector)
  {
    long count = VarianceBufferAggregator.getCount(buf, position);
    double sum = VarianceBufferAggregator.getSum(buf, position);
    double nvariance = VarianceBufferAggregator.getVariance(buf, position);
    for (int i = startRow; i < endRow; i++) {
      if (!replaceWithDefault && nulls != null && !nulls[i]) {
        count++;
        sum += vector[i];
        double t = count * vector[i] - sum;
        nvariance = nvariance + (t * t) / ((double) count * (count - 1));
      }
    }
    VarianceBufferAggregator.writeNVariance(buf, position, count, sum, nvariance);
  }
}

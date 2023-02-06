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

import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Vectorized implementation of {@link VarianceBufferAggregator} for {@link VarianceAggregatorCollector}.
 */
public class VarianceObjectVectorAggregator implements VectorAggregator
{
  private final VectorObjectSelector selector;

  public VarianceObjectVectorAggregator(VectorObjectSelector selector)
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
    Object[] vector = selector.getObjectVector();
    VarianceAggregatorCollector previous = VarianceBufferAggregator.getVarianceCollector(buf, position);
    for (int i = startRow; i < endRow; i++) {
      VarianceAggregatorCollector other = (VarianceAggregatorCollector) vector[i];
      previous.fold(other);
    }
    VarianceBufferAggregator.writeNVariance(buf, position, previous.count, previous.sum, previous.nvariance);
  }

  @Override
  public void aggregate(
          ByteBuffer buf,
          int numRows,
          int[] positions,
          @Nullable int[] rows,
          int positionOffset
  )
  {
    Object[] vector = selector.getObjectVector();
    for (int i = 0; i < numRows; i++) {
      int position = positions[i] + positionOffset;
      int row = rows != null ? rows[i] : i;
      VarianceAggregatorCollector previous = VarianceBufferAggregator.getVarianceCollector(buf, position);
      VarianceAggregatorCollector other = (VarianceAggregatorCollector) vector[row];
      previous.fold(other);
      VarianceBufferAggregator.writeNVariance(buf, position, previous.count, previous.sum, previous.nvariance);
    }
  }

  @Nullable
  @Override
  public VarianceAggregatorCollector get(ByteBuffer buf, int position)
  {
    return VarianceBufferAggregator.getVarianceCollector(buf, position);
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }
}

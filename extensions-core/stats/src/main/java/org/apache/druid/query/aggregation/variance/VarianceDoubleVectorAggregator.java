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
 * Vectorized implementation of {@link VarianceBufferAggregator} for doubles.
 */
public class VarianceDoubleVectorAggregator implements VectorAggregator
{
  private final VectorValueSelector selector;
  private final boolean replaceWithDefault = NullHandling.replaceWithDefault();

  public VarianceDoubleVectorAggregator(VectorValueSelector selector)
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
    double[] vector = selector.getDoubleVector();
    long count = 0;
    double sum = 0, nvariance = 0;
    boolean[] nulls = replaceWithDefault ? null : selector.getNullVector();
    for (int i = startRow; i < endRow; i++) {
      if (nulls == null || !nulls[i]) {
        count++;
        sum += vector[i];
      }
    }
    double mean = sum / count;
    if (count > 1) {
      for (int i = startRow; i < endRow; i++) {
        if (nulls == null || !nulls[i]) {
          nvariance += (vector[i] - mean) * (vector[i] - mean);
        }
      }
    }

    VarianceAggregatorCollector previous = new VarianceAggregatorCollector(
        VarianceBufferAggregator.getCount(buf, position),
        VarianceBufferAggregator.getSum(buf, position),
        VarianceBufferAggregator.getVariance(buf, position)
    );
    previous.fold(new VarianceAggregatorCollector(count, sum, nvariance));
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
    double[] vector = selector.getDoubleVector();
    boolean[] nulls = replaceWithDefault ? null : selector.getNullVector();
    for (int i = 0; i < numRows; i++) {
      int position = positions[i] + positionOffset;
      int row = rows != null ? rows[i] : i;
      if (nulls == null || !nulls[row]) {
        VarianceAggregatorCollector previous = VarianceBufferAggregator.getVarianceCollector(buf, position);
        previous.add(vector[row]);
        VarianceBufferAggregator.writeNVariance(buf, position, previous.count, previous.sum, previous.nvariance);
      }
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

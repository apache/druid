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

public class FixedBucketsHistogramVectorAggregator implements VectorAggregator
{
  private final VectorValueSelector selector;
  private final FixedBucketsHistogramBufferAggregatorHelper innerAggregator;

  public FixedBucketsHistogramVectorAggregator(
      VectorValueSelector selector,
      double lowerLimit,
      double upperLimit,
      int numBuckets,
      FixedBucketsHistogram.OutlierHandlingMode outlierHandlingMode
  )
  {
    this.selector = selector;
    this.innerAggregator = new FixedBucketsHistogramBufferAggregatorHelper(
        lowerLimit,
        upperLimit,
        numBuckets,
        outlierHandlingMode
    );
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    innerAggregator.init(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    double[] vector = selector.getDoubleVector();
    boolean[] isNull = selector.getNullVector();
    FixedBucketsHistogram histogram = innerAggregator.get(buf, position);
    for (int i = startRow; i < endRow; i++) {
      histogram.combine(toObject(vector, isNull, i));
    }
    innerAggregator.put(buf, position, histogram);
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    double[] vector = selector.getDoubleVector();
    boolean[] isNull = selector.getNullVector();
    for (int i = 0; i < numRows; i++) {
      int position = positions[i] + positionOffset;
      int index = rows != null ? rows[i] : i;
      Double val = toObject(vector, isNull, index);
      innerAggregator.aggregate(buf, position, val);
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

  @Nullable
  private Double toObject(double[] vector, @Nullable boolean[] isNull, int index)
  {
    return (isNull != null && isNull[index]) ? null : vector[index];
  }
}

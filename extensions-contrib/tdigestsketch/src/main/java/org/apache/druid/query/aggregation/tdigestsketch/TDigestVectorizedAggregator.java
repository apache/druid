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

package org.apache.druid.query.aggregation.tdigestsketch;

import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class TDigestVectorizedAggregator implements VectorAggregator
{
  private final TDigestSketchAggregatorHelper innerAggregator;
  private final VectorObjectSelector selector;

  public TDigestVectorizedAggregator(Integer compression, VectorObjectSelector selector)
  {
    this.selector = selector;
    this.innerAggregator = new TDigestSketchAggregatorHelper(compression == null
                                                             ? TDigestSketchAggregatorFactory.DEFAULT_COMPRESSION
                                                             : compression);
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    // after this point the histogram is present in the cache
    innerAggregator.init(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    Object[] vector = selector.getObjectVector();
    for (int i = startRow; i < endRow; i++) {
      Object other = vector[i];
      innerAggregator.aggregate(other, buf, position);
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    Object[] vector = selector.getObjectVector();
    for (int i = 0; i < numRows; i++) {
      Object other = vector[i];
      int position = positions[i] + positionOffset;
      innerAggregator.aggregate(other, buf, position);
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
    innerAggregator.close();
  }
}

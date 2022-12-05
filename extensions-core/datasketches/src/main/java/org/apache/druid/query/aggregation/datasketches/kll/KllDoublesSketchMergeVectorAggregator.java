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

package org.apache.druid.query.aggregation.datasketches.kll;

import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class KllDoublesSketchMergeVectorAggregator implements VectorAggregator
{
  private final VectorObjectSelector selector;
  private final KllDoublesSketchMergeBufferAggregatorHelper helper;

  public KllDoublesSketchMergeVectorAggregator(
      final VectorObjectSelector selector,
      final int k,
      final int maxIntermediateSize
  )
  {
    this.selector = selector;
    this.helper = new KllDoublesSketchMergeBufferAggregatorHelper(k, maxIntermediateSize);
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    helper.init(buf, position);
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position, final int startRow, final int endRow)
  {
    final Object[] vector = selector.getObjectVector();

    final KllDoublesSketch union = helper.getSketchAtPosition(buf, position);

    for (int i = startRow; i < endRow; i++) {
      final KllDoublesSketch sketch = (KllDoublesSketch) vector[i];
      if (sketch != null) {
        union.merge(sketch);
      }
    }
  }

  @Override
  public void aggregate(
      final ByteBuffer buf,
      final int numRows,
      final int[] positions,
      @Nullable final int[] rows,
      final int positionOffset
  )
  {
    final Object[] vector = selector.getObjectVector();

    for (int i = 0; i < numRows; i++) {
      final KllDoublesSketch sketch = (KllDoublesSketch) vector[rows != null ? rows[i] : i];

      if (sketch != null) {
        final int position = positions[i] + positionOffset;
        final KllDoublesSketch union = helper.getSketchAtPosition(buf, position);
        union.merge(sketch);
      }
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return helper.get(buf, position);
  }

  @Override
  public void close()
  {
    helper.clear();
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    helper.relocate(oldPosition, newPosition, oldBuffer, newBuffer);
  }
}

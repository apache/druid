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

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class KllFloatsSketchBuildVectorAggregator implements VectorAggregator
{
  private final VectorValueSelector selector;
  private final KllFloatsSketchBuildBufferAggregatorHelper helper;

  KllFloatsSketchBuildVectorAggregator(
      final VectorValueSelector selector,
      final int size,
      final int maxIntermediateSize
  )
  {
    this.selector = selector;
    this.helper = new KllFloatsSketchBuildBufferAggregatorHelper(size, maxIntermediateSize);
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    helper.init(buf, position);
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position, final int startRow, final int endRow)
  {
    final float[] floats = selector.getFloatVector();
    final boolean[] nulls = selector.getNullVector();

    final KllFloatsSketch sketch = helper.getSketchAtPosition(buf, position);

    for (int i = startRow; i < endRow; i++) {
      if (nulls == null || !nulls[i]) {
        sketch.update(floats[i]);
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
    final float[] floats = selector.getFloatVector();
    final boolean[] nulls = selector.getNullVector();

    for (int i = 0; i < numRows; i++) {
      final int idx = rows != null ? rows[i] : i;

      if (nulls == null || !nulls[idx]) {
        final int position = positions[i] + positionOffset;
        helper.getSketchAtPosition(buf, position).update(floats[idx]);
      }
    }
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    return helper.get(buf, position);
  }

  @Override
  public void relocate(final int oldPosition, final int newPosition, final ByteBuffer oldBuf, final ByteBuffer newBuf)
  {
    helper.relocate(oldPosition, newPosition, oldBuf, newBuf);
  }

  @Override
  public void close()
  {
    helper.clear();
  }
}

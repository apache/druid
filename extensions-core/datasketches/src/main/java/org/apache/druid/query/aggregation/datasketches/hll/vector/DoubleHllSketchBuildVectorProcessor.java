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

package org.apache.druid.query.aggregation.datasketches.hll.vector;

import org.apache.datasketches.hll.HllSketch;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchBuildBufferAggregatorHelper;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class DoubleHllSketchBuildVectorProcessor implements HllSketchBuildVectorProcessor
{
  private final HllSketchBuildBufferAggregatorHelper helper;
  private final VectorValueSelector selector;

  public DoubleHllSketchBuildVectorProcessor(
      final HllSketchBuildBufferAggregatorHelper helper,
      final VectorValueSelector selector
  )
  {
    this.helper = helper;
    this.selector = selector;
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    final double[] vector = selector.getDoubleVector();
    final boolean[] nullVector = selector.getNullVector();

    final HllSketch sketch = helper.getSketchAtPosition(buf, position);

    for (int i = startRow; i < endRow; i++) {
      if (NullHandling.replaceWithDefault() || nullVector == null || !nullVector[i]) {
        sketch.update(vector[i]);
      }
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    final double[] vector = selector.getDoubleVector();
    final boolean[] nullVector = selector.getNullVector();

    for (int i = 0; i < numRows; i++) {
      final int idx = rows != null ? rows[i] : i;
      if (NullHandling.replaceWithDefault() || nullVector == null || !nullVector[idx]) {
        final int position = positions[i] + positionOffset;
        final HllSketch sketch = helper.getSketchAtPosition(buf, position);
        sketch.update(vector[idx]);
      }
    }
  }
}

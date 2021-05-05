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
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchBuildBufferAggregatorHelper;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchBuildUtil;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Processor that handles cases where string columns are presented as object selectors instead of dimension selectors.
 */
public class ObjectHllSketchBuildVectorProcessor implements HllSketchBuildVectorProcessor
{
  private final HllSketchBuildBufferAggregatorHelper helper;
  private final StringEncoding stringEncoding;
  private final VectorObjectSelector selector;

  public ObjectHllSketchBuildVectorProcessor(
      final HllSketchBuildBufferAggregatorHelper helper,
      final StringEncoding stringEncoding,
      final VectorObjectSelector selector
  )
  {
    this.helper = helper;
    this.stringEncoding = stringEncoding;
    this.selector = selector;
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    final Object[] vector = selector.getObjectVector();
    final HllSketch sketch = helper.getSketchAtPosition(buf, position);

    for (int i = startRow; i < endRow; i++) {
      if (vector[i] != null) {
        HllSketchBuildUtil.updateSketch(
            sketch,
            stringEncoding,
            vector[i]
        );
      }
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    final Object[] vector = selector.getObjectVector();

    for (int i = 0; i < numRows; i++) {
      final int position = positions[i] + positionOffset;
      final HllSketch sketch = helper.getSketchAtPosition(buf, position);

      if (vector[i] != null) {
        HllSketchBuildUtil.updateSketch(
            sketch,
            stringEncoding,
            vector[i]
        );
      }
    }
  }
}

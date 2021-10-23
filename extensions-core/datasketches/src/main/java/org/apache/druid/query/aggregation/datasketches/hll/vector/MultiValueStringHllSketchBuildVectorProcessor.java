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
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class MultiValueStringHllSketchBuildVectorProcessor implements HllSketchBuildVectorProcessor
{
  private final HllSketchBuildBufferAggregatorHelper helper;
  private final StringEncoding stringEncoding;
  private final MultiValueDimensionVectorSelector selector;

  public MultiValueStringHllSketchBuildVectorProcessor(
      final HllSketchBuildBufferAggregatorHelper helper,
      final StringEncoding stringEncoding,
      final MultiValueDimensionVectorSelector selector
  )
  {
    this.helper = helper;
    this.stringEncoding = stringEncoding;
    this.selector = selector;
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    final IndexedInts[] vector = selector.getRowVector();
    final HllSketch sketch = helper.getSketchAtPosition(buf, position);

    for (int i = startRow; i < endRow; i++) {
      final IndexedInts ids = vector[i];
      final int sz = ids.size();

      for (int j = 0; j < sz; j++) {
        HllSketchBuildUtil.updateSketchWithDictionarySelector(
            sketch,
            stringEncoding,
            selector,
            ids.get(j)
        );
      }
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    final IndexedInts[] vector = selector.getRowVector();

    for (int i = 0; i < numRows; i++) {
      final int idx = rows != null ? rows[i] : i;
      final int position = positions[i] + positionOffset;
      final HllSketch sketch = helper.getSketchAtPosition(buf, position);

      final IndexedInts ids = vector[idx];
      final int sz = ids.size();

      for (int j = 0; j < sz; j++) {
        HllSketchBuildUtil.updateSketchWithDictionarySelector(
            sketch,
            stringEncoding,
            selector,
            ids.get(j)
        );
      }
    }
  }
}

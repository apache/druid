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

package org.apache.druid.query.aggregation.datasketches.hll;

import org.apache.datasketches.hll.TgtHllType;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.aggregation.datasketches.util.ToObjectVectorColumnProcessorFactory;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

public class HllSketchBuildVectorAggregator implements VectorAggregator
{
  private final HllSketchBuildBufferAggregatorHelper helper;
  private final Supplier<Object[]> objectSupplier;

  HllSketchBuildVectorAggregator(
      final VectorColumnSelectorFactory columnSelectorFactory,
      final String column,
      final int lgK,
      final TgtHllType tgtHllType,
      final int size
  )
  {
    this.helper = new HllSketchBuildBufferAggregatorHelper(lgK, tgtHllType, size);
    this.objectSupplier =
        ColumnProcessors.makeVectorProcessor(
            column,
            ToObjectVectorColumnProcessorFactory.INSTANCE,
            columnSelectorFactory
        );
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    helper.init(buf, position);
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position, final int startRow, final int endRow)
  {
    final Object[] vector = objectSupplier.get();
    for (int i = startRow; i < endRow; i++) {
      final Object value = vector[i];
      if (value != null) {
        HllSketchBuildAggregator.updateSketch(helper.getSketchAtPosition(buf, position), value);
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
    final Object[] vector = objectSupplier.get();

    for (int i = 0; i < numRows; i++) {
      final Object o = vector[rows != null ? rows[i] : i];

      if (o != null) {
        final int position = positions[i] + positionOffset;
        HllSketchBuildAggregator.updateSketch(helper.getSketchAtPosition(buf, position), o);
      }
    }
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    return helper.get(buf, position);
  }

  /**
   * In very rare cases sketches can exceed given memory, request on-heap memory and move there.
   * We need to identify such sketches and reuse the same objects as opposed to wrapping new memory regions.
   */
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

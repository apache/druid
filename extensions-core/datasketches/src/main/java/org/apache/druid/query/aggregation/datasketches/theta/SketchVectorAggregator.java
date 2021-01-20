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

package org.apache.druid.query.aggregation.datasketches.theta;

import org.apache.datasketches.theta.Union;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.aggregation.datasketches.util.ToObjectVectorColumnProcessorFactory;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

public class SketchVectorAggregator implements VectorAggregator
{
  private final Supplier<Object[]> toObjectProcessor;
  private final SketchBufferAggregatorHelper helper;

  public SketchVectorAggregator(
      VectorColumnSelectorFactory columnSelectorFactory,
      String column,
      int size,
      int maxIntermediateSize
  )
  {
    this.helper = new SketchBufferAggregatorHelper(size, maxIntermediateSize);
    this.toObjectProcessor =
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
    final Union union = helper.getOrCreateUnion(buf, position);
    final Object[] vector = toObjectProcessor.get();

    for (int i = startRow; i < endRow; i++) {
      final Object o = vector[i];
      if (o != null) {
        SketchAggregator.updateUnion(union, o);
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
    final Object[] vector = toObjectProcessor.get();

    for (int i = 0; i < numRows; i++) {
      final Object o = vector[rows != null ? rows[i] : i];

      if (o != null) {
        final int position = positions[i] + positionOffset;
        final Union union = helper.getOrCreateUnion(buf, position);
        SketchAggregator.updateUnion(union, o);
      }
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return helper.get(buf, position);
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    helper.relocate(oldPosition, newPosition, oldBuffer, newBuffer);
  }

  @Override
  public void close()
  {
    helper.close();
  }
}

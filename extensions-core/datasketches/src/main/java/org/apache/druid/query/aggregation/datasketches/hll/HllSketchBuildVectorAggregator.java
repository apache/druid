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
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.vector.HllSketchBuildVectorProcessor;
import org.apache.druid.query.aggregation.datasketches.hll.vector.HllSketchBuildVectorProcessorFactory;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class HllSketchBuildVectorAggregator implements VectorAggregator
{
  private final HllSketchBuildVectorProcessor processor;
  private final HllSketchBuildBufferAggregatorHelper helper;

  private HllSketchBuildVectorAggregator(
      final HllSketchBuildVectorProcessor processor,
      final HllSketchBuildBufferAggregatorHelper helper
  )
  {
    this.processor = processor;
    this.helper = helper;
  }

  public static HllSketchBuildVectorAggregator create(
      final VectorColumnSelectorFactory columnSelectorFactory,
      final String column,
      final int lgK,
      final TgtHllType tgtHllType,
      final StringEncoding stringEncoding,
      final int size
  )
  {
    final HllSketchBuildBufferAggregatorHelper helper = new HllSketchBuildBufferAggregatorHelper(
        lgK,
        tgtHllType,
        size
    );

    final HllSketchBuildVectorProcessor processor = ColumnProcessors.makeVectorProcessor(
        column,
        new HllSketchBuildVectorProcessorFactory(helper, stringEncoding),
        columnSelectorFactory
    );

    return new HllSketchBuildVectorAggregator(processor, helper);
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    helper.init(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    processor.aggregate(buf, position, startRow, endRow);
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    processor.aggregate(buf, numRows, positions, rows, positionOffset);
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

/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.datasketches.tuple;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSetOperationBuilder;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;
import com.yahoo.sketches.tuple.ArrayOfDoublesUnion;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;

public class ArrayOfDoublesSketchMergeBufferAggregator implements BufferAggregator
{

  private final BaseObjectColumnValueSelector<ArrayOfDoublesSketch> selector;
  private final int nominalEntries;
  private final int numberOfValues;
  private final int maxIntermediateSize;

  public ArrayOfDoublesSketchMergeBufferAggregator(
      final BaseObjectColumnValueSelector<ArrayOfDoublesSketch> selector,
      final int nominalEntries,
      final int numberOfValues,
      final int maxIntermediateSize
  )
  {
    this.selector = selector;
    this.nominalEntries = nominalEntries;
    this.numberOfValues = numberOfValues;
    this.maxIntermediateSize = maxIntermediateSize;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    final WritableMemory mem = WritableMemory.wrap(buf);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    new ArrayOfDoublesSetOperationBuilder().setNominalEntries(nominalEntries)
        .setNumberOfValues(numberOfValues).buildUnion(region);
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
    final ArrayOfDoublesSketch update = selector.getObject();
    if (update == null) {
      return;
    }
    final WritableMemory mem = WritableMemory.wrap(buf);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    final ArrayOfDoublesUnion union = ArrayOfDoublesSketches.wrapUnion(region);
    union.update(update);
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    final WritableMemory mem = WritableMemory.wrap(buf);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    final ArrayOfDoublesUnion union = ArrayOfDoublesSketches.wrapUnion(region);
    return union.getResult();
  }

  @Override
  public float getFloat(final ByteBuffer buf, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(final ByteBuffer buf, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }

}

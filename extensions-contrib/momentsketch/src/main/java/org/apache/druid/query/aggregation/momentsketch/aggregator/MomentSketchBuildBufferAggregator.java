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

package org.apache.druid.query.aggregation.momentsketch.aggregator;

import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.momentsketch.MomentSketchWrapper;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;

import java.nio.ByteBuffer;

public class MomentSketchBuildBufferAggregator implements BufferAggregator
{
  private final BaseDoubleColumnValueSelector selector;
  private final int k;
  private final boolean compress;

  public MomentSketchBuildBufferAggregator(
      final BaseDoubleColumnValueSelector valueSelector,
      final int k,
      final boolean compress
  )
  {
    this.selector = valueSelector;
    this.k = k;
    this.compress = compress;
  }

  @Override
  public synchronized void init(final ByteBuffer buffer, final int position)
  {
    ByteBuffer mutationBuffer = buffer.duplicate();
    mutationBuffer.position(position);

    MomentSketchWrapper emptyStruct = new MomentSketchWrapper(k);
    emptyStruct.setCompressed(compress);
    emptyStruct.toBytes(mutationBuffer);
  }

  @Override
  public synchronized void aggregate(final ByteBuffer buffer, final int position)
  {
    if (selector.isNull()) {
      return;
    }
    ByteBuffer mutationBuffer = buffer.duplicate();
    mutationBuffer.position(position);

    MomentSketchWrapper ms0 = MomentSketchWrapper.fromBytes(mutationBuffer);
    double x = selector.getDouble();
    ms0.add(x);

    mutationBuffer.position(position);
    ms0.toBytes(mutationBuffer);
  }

  @Override
  public synchronized Object get(final ByteBuffer buffer, final int position)
  {
    ByteBuffer mutationBuffer = buffer.duplicate();
    mutationBuffer.position(position);
    return MomentSketchWrapper.fromBytes(mutationBuffer);
  }

  @Override
  public float getFloat(final ByteBuffer buffer, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(final ByteBuffer buffer, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
  }
}

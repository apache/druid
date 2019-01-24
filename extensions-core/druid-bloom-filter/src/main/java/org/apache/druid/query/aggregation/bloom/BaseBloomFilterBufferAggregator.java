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

package org.apache.druid.query.aggregation.bloom;

import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseNullableColumnValueSelector;

import java.nio.ByteBuffer;

public abstract class BaseBloomFilterBufferAggregator<TSelector extends BaseNullableColumnValueSelector> implements BufferAggregator
{
  protected final int maxNumEntries;
  protected final TSelector selector;

  BaseBloomFilterBufferAggregator(TSelector selector, int maxNumEntries)
  {
    this.selector = selector;
    this.maxNumEntries = maxNumEntries;
  }

  abstract void bufferAdd(ByteBuffer buf);

  @Override
  public void init(ByteBuffer buf, int position)
  {
    final ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    BloomKFilter filter = new BloomKFilter(maxNumEntries);
    BloomKFilter.serialize(mutationBuffer, filter);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    final int oldPosition = buf.position();
    buf.position(position);
    bufferAdd(buf);
    buf.position(oldPosition);
  }


  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    // | k (byte) | numLongs (int) | bitset (long[numLongs]) |
    int sizeBytes = 1 + Integer.BYTES + (buf.getInt(position + 1) * Long.BYTES);
    mutationBuffer.limit(position + sizeBytes);
    return mutationBuffer.slice();
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("BloomFilterBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("BloomFilterBufferAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("BloomFilterBufferAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // nothing to close
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}

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

package org.apache.druid.query.aggregation.hyperloglog;

import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;

/**
 */
public class HyperUniquesBufferAggregator implements BufferAggregator
{
  private static final byte[] EMPTY_BYTES = HyperLogLogCollector.makeEmptyVersionedByteArray();
  private final BaseObjectColumnValueSelector selector;

  public HyperUniquesBufferAggregator(BaseObjectColumnValueSelector selector)
  {
    this.selector = selector;
  }

  public static void doInit(ByteBuffer buf, int position)
  {
    final ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    mutationBuffer.put(EMPTY_BYTES);
  }

  public static HyperLogLogCollector doGet(ByteBuffer buf, int position)
  {
    final int size = HyperLogLogCollector.getLatestNumBytesForDenseStorage();
    ByteBuffer dataCopyBuffer = ByteBuffer.allocate(size);
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    mutationBuffer.limit(position + size);
    dataCopyBuffer.put(mutationBuffer);
    dataCopyBuffer.rewind();
    return HyperLogLogCollector.makeCollector(dataCopyBuffer);
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    doInit(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    HyperLogLogCollector collector = (HyperLogLogCollector) selector.getObject();

    if (collector == null) {
      return;
    }

    // Save position, limit and restore later instead of allocating a new ByteBuffer object
    final int oldPosition = buf.position();
    final int oldLimit = buf.limit();
    buf.limit(position + HyperLogLogCollector.getLatestNumBytesForDenseStorage());
    buf.position(position);

    try {
      HyperLogLogCollector.makeCollector(buf).fold(collector);
    }
    finally {
      buf.limit(oldLimit);
      buf.position(oldPosition);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return doGet(buf, position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("HyperUniquesBufferAggregator does not support getFloat()");
  }


  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("HyperUniquesBufferAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("HyperUniquesBufferAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}

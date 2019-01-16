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

import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.aggregation.bloom.types.BloomFilterAggregatorColumnSelectorStrategy;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;


public class BloomFilterBufferAggregator extends BaseBloomFilterBufferAggregator
{
  private final ColumnSelectorPlus<BloomFilterAggregatorColumnSelectorStrategy> selectorPlus;

  public BloomFilterBufferAggregator(
      ColumnSelectorPlus<BloomFilterAggregatorColumnSelectorStrategy> selectorPlus,
      int maxNumEntries
  )
  {
    super(maxNumEntries);
    this.selectorPlus = selectorPlus;
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    int index = (System.identityHashCode(buf) + 31 * position) & 63;
    Lock lock = striped.getAt(index).writeLock();
    lock.lock();
    try {
      final int oldPosition = buf.position();
      buf.position(position);
      selectorPlus.getColumnSelectorStrategy().bufferAdd(selectorPlus.getSelector(), buf);
      buf.position(oldPosition);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selectorPlus", selectorPlus);
  }
}

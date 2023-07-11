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

package org.apache.druid.query.aggregation.datasketches.kll;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

import java.nio.ByteBuffer;

public class KllFloatsSketchMergeBufferAggregatorHelper extends KllSketchMergeBufferAggregatorHelper<KllFloatsSketch>
{
  public KllFloatsSketchMergeBufferAggregatorHelper(
      final int k,
      final int maxIntermediateSize
  )
  {
    super(k, maxIntermediateSize);
  }

  @Override
  KllFloatsSketch newDirectInstance(final int k, final WritableMemory mem, final MemoryRequestServer reqServer)
  {
    return KllFloatsSketch.newDirectInstance(mem, reqServer);
  }

  @Override
  KllFloatsSketch writableWrap(final WritableMemory mem, final MemoryRequestServer reqServer)
  {
    return KllFloatsSketch.writableWrap(mem, reqServer);
  }

  @Override
  KllFloatsSketch get(ByteBuffer buffer, int position)
  {
    final KllFloatsSketch union = getSketchAtPosition(buffer, position);
    return KllFloatsSketch.wrap(Memory.wrap(union.toByteArray()));
  }
}

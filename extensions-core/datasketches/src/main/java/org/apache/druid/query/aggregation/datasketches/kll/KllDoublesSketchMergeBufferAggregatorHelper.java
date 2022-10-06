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

import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

import java.nio.ByteBuffer;

public class KllDoublesSketchMergeBufferAggregatorHelper extends KllSketchMergeBufferAggregatorHelper<KllDoublesSketch>
{
  public KllDoublesSketchMergeBufferAggregatorHelper(
      final int k,
      final int maxIntermediateSize
  )
  {
    super(k, maxIntermediateSize);
  }

  @Override
  KllDoublesSketch newDirectInstance(int k, WritableMemory mem, MemoryRequestServer reqServer)
  {
    return KllDoublesSketch.newDirectInstance(mem, reqServer);
  }

  @Override
  KllDoublesSketch writableWrap(WritableMemory mem, MemoryRequestServer reqServer)
  {
    return KllDoublesSketch.writableWrap(mem, reqServer);
  }

  @Override
  KllDoublesSketch get(ByteBuffer buffer, int position)
  {
    final KllDoublesSketch union = getSketchAtPosition(buffer, position);
    return KllDoublesSketch.wrap(Memory.wrap(union.toByteArray()));
  }
}

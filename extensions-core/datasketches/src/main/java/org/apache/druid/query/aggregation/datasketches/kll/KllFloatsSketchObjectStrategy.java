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

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class KllFloatsSketchObjectStrategy implements ObjectStrategy<KllFloatsSketch>
{

  @Override
  public int compare(final KllFloatsSketch s1, final KllFloatsSketch s2)
  {
    return KllFloatsSketchAggregatorFactory.COMPARATOR.compare(s1, s2);
  }

  @Override
  public KllFloatsSketch fromByteBuffer(final ByteBuffer buffer, final int numBytes)
  {
    if (numBytes == 0) {
      return KllFloatsSketchOperations.EMPTY_SKETCH;
    }
    return KllFloatsSketch.wrap(Memory.wrap(buffer, ByteOrder.LITTLE_ENDIAN).region(buffer.position(), numBytes));
  }

  @Override
  public Class<KllFloatsSketch> getClazz()
  {
    return KllFloatsSketch.class;
  }

  @Override
  public byte[] toBytes(final KllFloatsSketch sketch)
  {
    if (sketch == null || sketch.isEmpty()) {
      return ByteArrays.EMPTY_ARRAY;
    }
    return sketch.toByteArray();
  }

}

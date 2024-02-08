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

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.data.SafeWritableMemory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class HllSketchHolderObjectStrategy implements ObjectStrategy<HllSketchHolder>
{

  static final HllSketchHolderObjectStrategy STRATEGY = new HllSketchHolderObjectStrategy();

  @Override
  public Class<HllSketchHolder> getClazz()
  {
    return HllSketchHolder.class;
  }

  @Override
  public int compare(final HllSketchHolder sketch1, final HllSketchHolder sketch2)
  {
    return HllSketchAggregatorFactory.COMPARATOR.compare(sketch1, sketch2);
  }

  @Nullable
  @Override
  public HllSketchHolder fromByteBuffer(final ByteBuffer buf, final int size)
  {
    if (size == 0 || isSafeToConvertToNullSketch(buf, size)) {
      return null;
    }
    return HllSketchHolder.of(HllSketch.wrap(Memory.wrap(buf, ByteOrder.LITTLE_ENDIAN).region(buf.position(), size)));
  }

  @Override
  public byte[] toBytes(final HllSketchHolder holder)
  {
    if (holder == null) {
      return new byte[] {};
    }
    HllSketch sketch = holder.getSketch();
    if (sketch == null || sketch.isEmpty()) {
      return new byte[] {};
    }
    return sketch.toCompactByteArray();
  }

  @Nullable
  @Override
  public HllSketchHolder fromByteBufferSafe(ByteBuffer buffer, int numBytes)
  {
    return HllSketchHolder.of(
        HllSketch.wrap(
            SafeWritableMemory.wrap(buffer, ByteOrder.LITTLE_ENDIAN).region(buffer.position(), numBytes)
        )
    );
  }

  /**
   * Checks if a sketch is empty and can be converted to null. Returns true if it is and false if it is not, or if is
   * not possible to say for sure.
   * Checks the initial 8 byte header to find the type of internal sketch implementation, then uses the logic the
   * corresponding implementation uses to tell if a sketch is empty while deserializing it.
   */
  static boolean isSafeToConvertToNullSketch(ByteBuffer buf, int size)
  {
    if (size < 8) {
      // Sanity check.
      // HllSketches as bytes should be at least 8 bytes even with an empty sketch. If this is not the case, return
      // false since we can't be sure.
      return false;
    }
    final int position = buf.position();

    // Get preamble int. These should correspond to the type of internal implementaion as a sanity check.
    final int preInts = buf.get(position) & 0x3F;   // get(PREAMBLE_INTS_BYTE) & PREAMBLE_MASK

    // Get org.apache.datasketches.hll.CurMode. This indicates the type of internal data structure.
    final int curMode = buf.get(position + 7) & 3;  // get(MODE_BYTE) & CUR_MODE_MASK
    switch (curMode) {
      case 0: // LIST
        if (preInts != 2) {
          // preInts should be LIST_PREINTS, Sanity check.
          return false;
        }
        // Based on org.apache.datasketches.hll.PreambleUtil.extractListCount
        int listCount = buf.get(position + 6) & 0xFF; // get(LIST_COUNT_BYTE) & 0xFF
        return listCount == 0;
      case 1: // SET
        if (preInts != 3 || size < 12) {
          // preInts should be HASH_SET_PREINTS, Sanity check.
          // We also need to read an additional int for Set implementations.
          return false;
        }
        // Based on org.apache.datasketches.hll.PreambleUtil.extractHashSetCount
        // Endianness of buf doesn't matter, since we're checking for equality with zero.
        int setCount = buf.getInt(position + 8);  // getInt(HASH_SET_COUNT_INT)
        return setCount == 0;
      case 2: // HLL
        if (preInts != 10) {
          // preInts should be HLL_PREINTS, Sanity check.
          return false;
        }
        // Based on org.apache.datasketches.hll.DirectHllArray.isEmpty
        final int flags = buf.get(position + 5);      // get(FLAGS_BYTE)
        return (flags & 4) > 0;                       // (flags & EMPTY_FLAG_MASK) > 0
      default: // Unknown implementation
        // Can't say for sure, so return false.
        return false;
    }
  }
}

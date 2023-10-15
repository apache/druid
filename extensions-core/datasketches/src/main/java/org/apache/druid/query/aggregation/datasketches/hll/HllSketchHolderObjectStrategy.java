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

import com.google.common.base.Preconditions;
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

  @Override
  public HllSketchHolder fromByteBuffer(final ByteBuffer buf, final int size)
  {
    if (size == 0 || isSafeToConvertToNullSketch(buf)) {
      return HllSketchHolder.of((HllSketch) null);
    }
    return HllSketchHolder.of(HllSketch.wrap(Memory.wrap(buf, ByteOrder.LITTLE_ENDIAN).region(buf.position(), size)));
  }

  @Override
  public byte[] toBytes(final HllSketchHolder sketch)
  {
    if (sketch.getSketch() == null || sketch.getSketch().isEmpty()) {
      return new byte[] {};
    }
    return sketch.getSketch().toCompactByteArray();
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

  private boolean isSafeToConvertToNullSketch(ByteBuffer buf)
  {
    // TODO: Might need a sanity check here, to ensure that position and offset makes sense.

    // Get org.apache.datasketches.hll.CurMode. This indicates the type of data structure.
    final int position = buf.position();
    final int preInts = buf.get(position) & 0X3F; // get(PREAMBLE_INTS_BYTE) & 0X3F
    final int curMode = buf.get(position + 7) & 3;    // get(MODE_BYTE) & CUR_MODE_MASK

    switch (curMode) {
      case 0:          // LIST
        Preconditions.checkArgument(preInts == 2);  // preInts == LIST_PREINTS, Sanity
        int listCount = buf.get(position + 6) & 0XFF; // get(LIST_COUNT_BYTE) & 0XFF
        return listCount == 0;
      case 1:          // SET
        Preconditions.checkArgument(preInts == 3);  // preInts == HASH_SET_PREINTS, Sanity
        int setCount = buf.get(position + 8);  // get(HASH_SET_COUNT_INT)
        return setCount == 0;
      case 2:          // HLL
        Preconditions.checkArgument(preInts == 10);  // preInts == HLL_PREINTS, Sanity
        final int flags = buf.get(position + 5);      // get(FLAGS_BYTE)
        return (flags & 4) > 0;                       // (flags & EMPTY_FLAG_MASK) > 0
      default:         // UNKNOWN
        // Can't say for sure, so return "false".
        return false;
    }
  }
}

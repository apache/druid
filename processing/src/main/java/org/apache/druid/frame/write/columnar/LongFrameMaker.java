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

package org.apache.druid.frame.write.columnar;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.allocation.AppendableMemory;
import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.frame.allocation.MemoryRange;
import org.apache.druid.java.util.common.ISE;

import java.io.Closeable;

public class LongFrameMaker implements Closeable
{
  public static final long DATA_OFFSET = 1 /* type code */ + 1 /* has nulls? */;

  private final AppendableMemory appendableMemory;
  private final boolean hasNulls;
  private final int sz;

  public LongFrameMaker(
      MemoryAllocator allocator,
      boolean hasNulls
  )
  {
    this.appendableMemory = AppendableMemory.create(allocator);
    this.hasNulls = hasNulls;
    this.sz = valueSize(hasNulls);
  }

  public static int valueSize(final boolean hasNulls)
  {
    return hasNulls ? Long.BYTES + 1 : Long.BYTES;
  }

  public boolean add(long value)
  {
    if (!(appendableMemory.reserveAdditional(sz))) {
      return false;
    }

    final MemoryRange<WritableMemory> cursor = appendableMemory.cursor();
    final WritableMemory memory = cursor.memory();
    final long position = cursor.start();

    if (hasNulls) {
      memory.putByte(position, (byte) 0);
      memory.putLong(position + 1, value);
    } else {
      memory.putLong(position, value);
    }

    appendableMemory.advanceCursor(sz);
    return true;
  }

  public boolean addNull()
  {
    if (!hasNulls) {
      throw new ISE("Was told that null doesn't exist, cannot add null");
    }

    if (!(appendableMemory.reserveAdditional(sz))) {
      return false;
    }

    final MemoryRange<WritableMemory> cursor = appendableMemory.cursor();
    final WritableMemory memory = cursor.memory();
    final long position = cursor.start();

    memory.putByte(position, (byte) 1);
    memory.putLong(position + 1, 0);

    appendableMemory.advanceCursor(sz);
    return true;
  }

  public void undo()
  {
    appendableMemory.rewindCursor(sz);
  }

  public long size()
  {
    return DATA_OFFSET + appendableMemory.size();
  }

  public long writeTo(final WritableMemory memory, final long startPosition)
  {
    long currentPosition = startPosition;

    memory.putByte(currentPosition, FrameColumnWriters.TYPE_LONG);
    memory.putByte(currentPosition + 1, hasNulls ? (byte) 1 : (byte) 0);
    currentPosition += 2;

    currentPosition += appendableMemory.writeTo(memory, currentPosition);
    return currentPosition - startPosition;
  }

  @Override
  public void close()
  {
    appendableMemory.close();
  }
}

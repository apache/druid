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

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.allocation.AppendableMemory;
import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.frame.allocation.MemoryRange;
import org.apache.druid.frame.read.columnar.ComplexFrameColumnReader;
import org.apache.druid.java.util.common.ISE;

/**
 * Column writer for complex columns.
 *
 * Dual to {@link ComplexFrameColumnReader}.
 */
public class ComplexFrameMaker
{
  // Less than half of AppendableMemory.DEFAULT_INITIAL_ALLOCATION_SIZE.
  // This guarantees we can fit a WorkerMemoryParmeters.MAX_FRAME_COLUMNS number of columns into a frame.
  private static final int INITIAL_ALLOCATION_SIZE = 128;

  public static final byte NOT_NULL_MARKER = 0x00;
  public static final byte NULL_MARKER = 0x01;
  public static final int TYPE_NAME_LENGTH_POSITION = Byte.BYTES;
  public static final int TYPE_NAME_POSITION = Byte.BYTES + Integer.BYTES;

  private final AppendableMemory offsetMemory;
  private final AppendableMemory dataMemory;
  private final byte[] typeName;

  private int lastDataLength = -1;

  ComplexFrameMaker(
      final MemoryAllocator allocator,
      final byte[] typeName
  )
  {
    this.offsetMemory = AppendableMemory.create(allocator, INITIAL_ALLOCATION_SIZE);
    this.dataMemory = AppendableMemory.create(allocator, INITIAL_ALLOCATION_SIZE);
    this.typeName = typeName;
  }

  public boolean add(byte[] bytes)
  {
    if (!offsetMemory.reserveAdditional(Integer.BYTES)) {
      return false;
    }

    boolean isNull = false;
    if (bytes == null) {
      isNull = true;
      bytes = ByteArrays.EMPTY_ARRAY;
    }

    if (bytes.length == Integer.MAX_VALUE) {
      // Cannot handle objects this large.
      return false;
    }

    final int dataLength = bytes.length + 1;

    if (dataMemory.size() + dataLength > Integer.MAX_VALUE || !(dataMemory.reserveAdditional(dataLength))) {
      return false;
    }

    // All space is reserved. Start writing.
    final MemoryRange<WritableMemory> offsetCursor = offsetMemory.cursor();
    offsetCursor.memory().putInt(offsetCursor.start(), Ints.checkedCast(dataMemory.size() + dataLength));
    offsetMemory.advanceCursor(Integer.BYTES);

    final MemoryRange<WritableMemory> dataCursor = dataMemory.cursor();
    dataCursor.memory().putByte(dataCursor.start(), isNull ? NULL_MARKER : NOT_NULL_MARKER);
    dataCursor.memory().putByteArray(dataCursor.start() + 1, bytes, 0, bytes.length);
    dataMemory.advanceCursor(dataLength);

    lastDataLength = dataLength;
    return true;
  }

  public void undo()
  {
    if (lastDataLength == -1) {
      throw new ISE("Nothing to undo");
    }

    offsetMemory.rewindCursor(Integer.BYTES);
    dataMemory.rewindCursor(lastDataLength);
    lastDataLength = -1;
  }

  public long size()
  {
    return headerSize() + offsetMemory.size() + dataMemory.size();
  }

  public long writeTo(final WritableMemory memory, final long startPosition)
  {
    long currentPosition = startPosition;

    memory.putByte(currentPosition, FrameColumnWriters.TYPE_COMPLEX);
    currentPosition += 1;

    memory.putInt(currentPosition, typeName.length);
    currentPosition += Integer.BYTES;

    memory.putByteArray(currentPosition, typeName, 0, typeName.length);
    currentPosition += typeName.length;

    currentPosition += offsetMemory.writeTo(memory, currentPosition);
    currentPosition += dataMemory.writeTo(memory, currentPosition);

    return currentPosition - startPosition;
  }

  public void close()
  {
    offsetMemory.close();
    dataMemory.close();
  }

  private int headerSize()
  {
    return 1 /* type code */
           + Integer.BYTES /* type name length */
           + typeName.length;
  }
}

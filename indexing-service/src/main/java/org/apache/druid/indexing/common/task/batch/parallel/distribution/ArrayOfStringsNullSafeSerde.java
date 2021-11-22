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

package org.apache.druid.indexing.common.task.batch.parallel.distribution;

import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.data.input.StringTuple;

import java.nio.charset.StandardCharsets;

/**
 * Serde for {@link StringTuple}.
 * <p>
 * The implementation is the same as {@link ArrayOfStringsSerDe}, except this
 * class handles null String values as well.
 */
public class ArrayOfStringsNullSafeSerde extends ArrayOfItemsSerDe<String>
{

  @Override
  public byte[] serializeToByteArray(final String[] items)
  {
    int length = 0;
    final byte[][] itemsBytes = new byte[items.length][];
    for (int i = 0; i < items.length; i++) {
      // If the String is null, make the byte array also null
      itemsBytes[i] = items[i] == null ? null : items[i].getBytes(StandardCharsets.UTF_8);
      length += (itemsBytes[i] == null ? 0 : itemsBytes[i].length) + Integer.BYTES;
    }
    final byte[] bytes = new byte[length];
    final WritableMemory mem = WritableMemory.writableWrap(bytes);
    long offsetBytes = 0;
    for (int i = 0; i < items.length; i++) {
      if (itemsBytes[i] != null) {
        // Write the length of the array and the array itself
        mem.putInt(offsetBytes, itemsBytes[i].length);
        offsetBytes += Integer.BYTES;
        mem.putByteArray(offsetBytes, itemsBytes[i], 0, itemsBytes[i].length);
        offsetBytes += itemsBytes[i].length;
      } else {
        // If the byte array is null, write the length as -1
        mem.putInt(offsetBytes, -1);
        offsetBytes += Integer.BYTES;
      }
    }
    return bytes;
  }

  @Override
  public String[] deserializeFromMemory(final Memory mem, final int numItems)
  {
    final String[] array = new String[numItems];
    long offsetBytes = 0;
    for (int i = 0; i < numItems; i++) {
      // Read the length of the byte array
      Util.checkBounds(offsetBytes, Integer.BYTES, mem.getCapacity());
      final int arrayLength = mem.getInt(offsetBytes);
      offsetBytes += Integer.BYTES;

      // Negative strLength represents a null byte array and a null String
      if (arrayLength >= 0) {
        final byte[] bytes = new byte[arrayLength];
        Util.checkBounds(offsetBytes, arrayLength, mem.getCapacity());
        mem.getByteArray(offsetBytes, bytes, 0, arrayLength);
        offsetBytes += arrayLength;
        array[i] = new String(bytes, StandardCharsets.UTF_8);
      }
    }
    return array;
  }

}


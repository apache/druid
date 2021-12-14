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
import org.apache.druid.java.util.common.IAE;

import java.nio.charset.StandardCharsets;

/**
 * Serde for {@link StringTuple}.
 * <p>
 * The implementation is the same as {@link ArrayOfStringsSerDe}, except this
 * class handles null String values as well.
 */
public class ArrayOfStringsNullSafeSerde extends ArrayOfItemsSerDe<String>
{

  private static final int NULL_STRING_LENGTH = -1;

  @Override
  public byte[] serializeToByteArray(final String[] items)
  {
    // Determine the bytes for each String
    int length = 0;
    final byte[][] itemsBytes = new byte[items.length][];
    for (int i = 0; i < items.length; i++) {
      length += Integer.BYTES;

      // Do not initialize the byte array for a null String
      if (items[i] != null) {
        itemsBytes[i] = items[i].getBytes(StandardCharsets.UTF_8);
        length += itemsBytes[i].length;
      }
    }

    // Create a single byte array for all the Strings
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
        mem.putInt(offsetBytes, NULL_STRING_LENGTH);
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
      // Read the length of the ith String
      Util.checkBounds(offsetBytes, Integer.BYTES, mem.getCapacity());
      final int strLength = mem.getInt(offsetBytes);
      offsetBytes += Integer.BYTES;

      if (strLength >= 0) {
        // Read the bytes for the String
        final byte[] bytes = new byte[strLength];
        Util.checkBounds(offsetBytes, strLength, mem.getCapacity());
        mem.getByteArray(offsetBytes, bytes, 0, strLength);
        offsetBytes += strLength;
        array[i] = new String(bytes, StandardCharsets.UTF_8);
      } else if (strLength != NULL_STRING_LENGTH) {
        throw new IAE(
            "Illegal strLength [%s] at offset [%s]. Must be %s, 0 or a positive integer.",
            strLength,
            offsetBytes,
            NULL_STRING_LENGTH
        );
      }
    }
    return array;
  }

}


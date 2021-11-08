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
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.memory.internal.UnsafeUtil;
import org.apache.druid.data.input.StringTuple;

/**
 * Serde for {@link StringTuple}.
 * <p>
 * Implementation similar to {@link ArrayOfStringsSerDe}.
 */
public class ArrayOfStringTuplesSerDe extends ArrayOfItemsSerDe<StringTuple>
{
  private static final ArrayOfStringsSerDe STRINGS_SERDE = new ArrayOfStringsSerDe();

  @Override
  public byte[] serializeToByteArray(StringTuple[] items)
  {
    int length = 0;
    final byte[][] itemsBytes = new byte[items.length][];
    for (int i = 0; i < items.length; i++) {
      // Get the byte contents of the StringTuple
      itemsBytes[i] = STRINGS_SERDE.serializeToByteArray(items[i].toArray());

      // Overall byte representation contains number of items, size of content, byte contents
      length += Integer.BYTES + Integer.BYTES + itemsBytes[i].length;
    }

    final byte[] bytes = new byte[length];
    final WritableMemory mem = WritableMemory.writableWrap(bytes);
    long offsetBytes = 0;
    for (int i = 0; i < items.length; i++) {
      // Add the number of items in the StringTuple
      mem.putInt(offsetBytes, items[i].size());
      offsetBytes += Integer.BYTES;

      // Add the size of byte content for the StringTuple
      mem.putInt(offsetBytes, itemsBytes[i].length);
      offsetBytes += Integer.BYTES;

      // Add the byte contents of the StringTuple
      mem.putByteArray(offsetBytes, itemsBytes[i], 0, itemsBytes[i].length);
      offsetBytes += itemsBytes[i].length;
    }
    return bytes;
  }

  @Override
  public StringTuple[] deserializeFromMemory(Memory mem, int numItems)
  {
    final StringTuple[] array = new StringTuple[numItems];
    long offsetBytes = 0;
    for (int i = 0; i < numItems; i++) {
      // Read the number of items in the StringTuple
      UnsafeUtil.checkBounds(offsetBytes, Integer.BYTES, mem.getCapacity());
      final int numItemsInTuple = mem.getInt(offsetBytes);
      offsetBytes += Integer.BYTES;

      // Read the size of byte content
      UnsafeUtil.checkBounds(offsetBytes, Integer.BYTES, mem.getCapacity());
      final int byteContentSize = mem.getInt(offsetBytes);
      offsetBytes += Integer.BYTES;

      // Read the byte content
      final byte[] byteContent = new byte[byteContentSize];
      UnsafeUtil.checkBounds(offsetBytes, byteContentSize, mem.getCapacity());
      mem.getByteArray(offsetBytes, byteContent, 0, byteContentSize);
      offsetBytes += byteContentSize;

      // Deserialize the byte content as a StringTuple
      array[i] = StringTuple.create(
          STRINGS_SERDE.deserializeFromMemory(Memory.wrap(byteContent), numItemsInTuple)
      );
    }
    return array;
  }
}

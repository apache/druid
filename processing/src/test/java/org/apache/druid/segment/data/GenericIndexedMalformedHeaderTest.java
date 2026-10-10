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

package org.apache.druid.segment.data;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class GenericIndexedMalformedHeaderTest
{
  private static ByteBuffer buildV1(int numElements, int[] offsets, byte[] values, int numBytesUsedOverride)
  {
    final int valuesBytes = values.length;
    final int numBytesUsed = numBytesUsedOverride >= 0 ? numBytesUsedOverride : 8 + numElements * 4 + valuesBytes;
    final ByteBuffer buffer = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
    buffer.put((byte) 0x1); // VERSION_ONE
    buffer.put((byte) 0x0); // allowReverseLookup
    buffer.putInt(numBytesUsed);
    buffer.putInt(numElements);
    for (int offset : offsets) {
      buffer.putInt(offset);
    }
    buffer.put(values);
    buffer.flip();
    buffer.limit(buffer.capacity());
    return buffer;
  }

  @Test
  public void testNegativeCountRejected()
  {
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GenericIndexed.read(buildV1(-5, new int[]{}, new byte[]{1, 2, 3}, 8), GenericIndexed.STRING_STRATEGY, null)
    );
    Assertions.assertTrue(e.getMessage().contains("must be non-negative"), e.getMessage());
  }

  @Test
  public void testOverflowCountRejected()
  {
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GenericIndexed.read(buildV1(0x40000001, new int[]{}, new byte[]{}, 8), GenericIndexed.STRING_STRATEGY, null)
    );
    Assertions.assertTrue(e.getMessage().contains("exceeds the available buffer"), e.getMessage());
  }

  @Test
  public void testHugeOffsetRejected()
  {
    GenericIndexed<String> indexed = GenericIndexed.read(
        buildV1(2, new int[]{6, 1000}, new byte[]{0, 0, 0, 2, 'a', 'b', 0, 0, 0, 2, 'c', 'd'}, -1),
        GenericIndexed.STRING_STRATEGY,
        null
    );
    Assertions.assertEquals("ab", indexed.get(0));
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> indexed.get(1)
    );
    Assertions.assertTrue(e.getMessage().contains("out of bounds"), e.getMessage());
  }

  @Test
  public void testNegativeOffsetRejected()
  {
    GenericIndexed<String> indexed = GenericIndexed.read(
        buildV1(2, new int[]{-5, 10}, new byte[]{0, 0, 0, 2, 'x', 'y', 0, 0, 0, 2, 'p', 'q'}, -1),
        GenericIndexed.STRING_STRATEGY,
        null
    );
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> indexed.get(0)
    );
    Assertions.assertTrue(e.getMessage().contains("out of bounds"), e.getMessage());
  }

  @Test
  public void testValidHeaderReads()
  {
    GenericIndexed<String> indexed = GenericIndexed.read(
        buildV1(1, new int[]{6}, new byte[]{0, 0, 0, 2, 'h', 'i'}, -1),
        GenericIndexed.STRING_STRATEGY,
        null
    );
    Assertions.assertEquals(1, indexed.size());
    Assertions.assertEquals("hi", indexed.get(0));
  }

  @Test
  public void testSingleThreadedRightwardProbes()
  {
    GenericIndexed<String> indexed = GenericIndexed.fromIterable(
        java.util.Arrays.asList("a", "b", "c"),
        GenericIndexed.STRING_STRATEGY
    );
    GenericIndexed<String>.BufferIndexed bi = indexed.singleThreaded();

    Assertions.assertDoesNotThrow(() -> bi.getByteBuffer(0));
    // A rightward binary-search probe must not be rejected by the bounds check
    // after an earlier probe narrowed the reused buffer's limit.
    Assertions.assertDoesNotThrow(() -> bi.getByteBuffer(2));
    Assertions.assertDoesNotThrow(() -> bi.getByteBuffer(1));
  }

  @Test
  public void testSingleThreadedSuccessiveLookups()
  {
    GenericIndexed<String> indexed = GenericIndexed.fromIterable(
        java.util.Arrays.asList("a", "b", "c"),
        GenericIndexed.STRING_STRATEGY
    );
    GenericIndexed<String>.BufferIndexed bi = indexed.singleThreaded();

    Assertions.assertDoesNotThrow(() -> bi.getByteBuffer(1));
    final ByteBuffer second = bi.getByteBuffer(2);
    Assertions.assertEquals(1, second.remaining());
    Assertions.assertEquals((byte) 'c', second.get());
    final ByteBuffer first = bi.getByteBuffer(0);
    Assertions.assertEquals(1, first.remaining());
    Assertions.assertEquals((byte) 'a', first.get());
  }

  @Test
  public void testIntermediateOffsetGapRejected()
  {
    GenericIndexed<String> indexed = GenericIndexed.read(
        buildV1(2, new int[]{0, 10}, new byte[]{0, 0, 0, 2, 'x', 'y', 0, 0, 0, 2, 'p', 'q'}, -1),
        GenericIndexed.STRING_STRATEGY,
        null
    );
    // offsets[0] == 0 leaves the first value without its four-byte size marker,
    // so a direct get(1) must reject the header instead of decoding outside the element.
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> indexed.get(1)
    );
    Assertions.assertTrue(e.getMessage().contains("out of bounds"), e.getMessage());
  }

  @Test
  public void testSingleThreadedIntermediateOffsetGapRejected()
  {
    GenericIndexed<String> indexed = GenericIndexed.read(
        buildV1(2, new int[]{0, 10}, new byte[]{0, 0, 0, 2, 'x', 'y', 0, 0, 0, 2, 'p', 'q'}, -1),
        GenericIndexed.STRING_STRATEGY,
        null
    );
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> indexed.singleThreaded().getByteBuffer(1)
    );
    Assertions.assertTrue(e.getMessage().contains("out of bounds"), e.getMessage());
  }
}

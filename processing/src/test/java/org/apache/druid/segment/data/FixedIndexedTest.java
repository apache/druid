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

import com.google.common.collect.ImmutableList;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Iterator;


@ParameterizedClass


@MethodSource("constructorFeeder")
public class FixedIndexedTest extends InitializedNullHandlingTest
{
  private static final Long[] LONGS = new Long[64];

  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{ByteOrder.LITTLE_ENDIAN}, new Object[]{ByteOrder.BIG_ENDIAN});
  }

  @BeforeAll
  public static void setup()
  {
    for (int i = 0; i < LONGS.length; i++) {
      LONGS[i] = i * 10L;
    }
  }

  private final ByteOrder order;

  public FixedIndexedTest(ByteOrder byteOrder)
  {
    this.order = byteOrder;
  }

  @Test
  public void testGet() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
    fillBuffer(buffer, order, false);
    FixedIndexed<Long> fixedIndexed =
        FixedIndexed.<Long>read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES).get();
    Assertions.assertEquals(64, fixedIndexed.size());
    for (int i = 0; i < LONGS.length; i++) {
      Assertions.assertEquals(LONGS[i], fixedIndexed.get(i));
      Assertions.assertEquals(i, fixedIndexed.indexOf(LONGS[i]));
    }

    Assertions.assertThrows(IllegalArgumentException.class, () -> fixedIndexed.get(-1));
    Assertions.assertThrows(IllegalArgumentException.class, () -> fixedIndexed.get(LONGS.length));
  }

  @Test
  public void testIterator() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
    fillBuffer(buffer, order, false);
    FixedIndexed<Long> fixedIndexed =
        FixedIndexed.<Long>read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES).get();
    Iterator<Long> iterator = fixedIndexed.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      Assertions.assertEquals(LONGS[i++], iterator.next());
    }
  }

  @Test
  public void testGetWithNull() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
    fillBuffer(buffer, order, true);
    FixedIndexed<Long> fixedIndexed =
        FixedIndexed.<Long>read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES).get();
    Assertions.assertEquals(65, fixedIndexed.size());
    Assertions.assertNull(fixedIndexed.get(0));
    for (int i = 0; i < LONGS.length; i++) {
      Assertions.assertEquals(LONGS[i], fixedIndexed.get(i + 1));
      Assertions.assertEquals(i + 1, fixedIndexed.indexOf(LONGS[i]));
    }
  }

  @Test
  public void testIteratorWithNull() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
    fillBuffer(buffer, order, true);
    FixedIndexed<Long> fixedIndexed =
        FixedIndexed.<Long>read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES).get();
    Iterator<Long> iterator = fixedIndexed.iterator();
    Assertions.assertNull(iterator.next());
    int i = 0;
    while (iterator.hasNext()) {
      Assertions.assertEquals(LONGS[i++], iterator.next());
    }
  }

  private static void fillBuffer(ByteBuffer buffer, ByteOrder order, boolean withNull) throws IOException
  {
    buffer.position(0);
    FixedIndexedWriter<Long> writer = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        ColumnType.LONG.getStrategy(),
        order,
        Long.BYTES,
        true
    );
    writer.open();
    if (withNull) {
      writer.write(null);
    }
    for (Long aLong : LONGS) {
      writer.write(aLong);
    }
    Iterator<Long> longIterator = writer.getIterator();
    int ctr = 0;
    if (withNull) {
      Assertions.assertNull(writer.get(0));
      for (int i = 1; i <= LONGS.length; i++) {
        Assertions.assertEquals(LONGS[i - 1], writer.get(i), " index: " + i);
      }
    } else {
      for (int i = 0; i < LONGS.length; i++) {
        Assertions.assertEquals(LONGS[i], writer.get(i), " index: " + i);
      }
    }
    while (longIterator.hasNext()) {
      if (withNull) {
        if (ctr == 0) {
          Assertions.assertNull(longIterator.next());
          Assertions.assertNull(writer.get(ctr));
        } else {
          Assertions.assertEquals(LONGS[ctr - 1], longIterator.next());
          Assertions.assertEquals(LONGS[ctr - 1], writer.get(ctr));
        }
      } else {
        Assertions.assertEquals(LONGS[ctr], longIterator.next());
        Assertions.assertEquals(LONGS[ctr], writer.get(ctr));
      }
      ctr++;
    }
    WritableByteChannel channel = new WritableByteChannel()
    {
      @Override
      public int write(ByteBuffer src)
      {
        int size = src.remaining();
        buffer.put(src);
        return size;
      }

      @Override
      public boolean isOpen()
      {
        return true;
      }

      @Override
      public void close()
      {
      }
    };
    long size = writer.getSerializedSize();
    buffer.position(0);
    writer.writeTo(channel, null);
    Assertions.assertEquals(size, buffer.position());
    buffer.position(0);
  }

  @Test
  public void testNegativeSizeRejected()
  {
    final ByteBuffer buffer = ByteBuffer.allocate(16).order(order);
    buffer.put((byte) 0);
    buffer.put((byte) 0);
    buffer.putInt(-5);
    buffer.flip();

    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> FixedIndexed.read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES)
    );
    Assertions.assertTrue(e.getMessage().contains("must be non-negative"), e.getMessage());
  }

  @Test
  public void testSizeExceedingBufferRejected()
  {
    final ByteBuffer buffer = ByteBuffer.allocate(20).order(order);
    buffer.put((byte) 0);
    buffer.put((byte) 0);
    buffer.putInt(4); // 4 * Long.BYTES = 32 bytes claimed, but the buffer only holds 8 value bytes
    buffer.flip();

    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> FixedIndexed.read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES)
    );
    Assertions.assertTrue(e.getMessage().contains("exceeds the available buffer"), e.getMessage());
  }

  @Test
  public void testNegativeCountWithNullFlagRejected()
  {
    // With the null flag set the serialized count is incremented before validation: a raw
    // count of -1 yields size == 0 but valuesCount == -1, which must still be rejected.
    final ByteBuffer buffer = ByteBuffer.allocate(16).order(order);
    buffer.put((byte) 0);
    buffer.put((byte) (TypeStrategies.IS_NULL_BYTE | FixedIndexed.IS_SORTED_MASK));
    buffer.putInt(-1);
    buffer.flip();

    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> FixedIndexed.read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES)
    );
    Assertions.assertTrue(e.getMessage().contains("must be non-negative"), e.getMessage());
  }
}

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
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

@ParameterizedClass

@MethodSource("constructorFeeder")
public class FrontCodedIndexedTest extends InitializedNullHandlingTest
{
  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{ByteOrder.LITTLE_ENDIAN, FrontCodedIndexed.V1},
        new Object[]{ByteOrder.LITTLE_ENDIAN, FrontCodedIndexed.V0},
        new Object[]{ByteOrder.BIG_ENDIAN, FrontCodedIndexed.V1},
        new Object[]{ByteOrder.BIG_ENDIAN, FrontCodedIndexed.V0}
    );
  }

  private final ByteOrder order;
  private final byte version;

  public FrontCodedIndexedTest(ByteOrder byteOrder, byte version)
  {
    this.order = byteOrder;
    this.version = version;
  }

  @Test
  public void testFrontCodedIndexed() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12).order(order);
    List<String> theList = ImmutableList.of("hello", "helloo", "hellooo", "hellooz", "helloozy");
    persistToBuffer(buffer, theList, 4, version);

    buffer.position(0);
    FrontCodedIndexed codedUtf8Indexed = FrontCodedIndexed.read(
        buffer,
        buffer.order()
    ).get();
    Assertions.assertEquals("helloo", StringUtils.fromUtf8(codedUtf8Indexed.get(1)));
    Assertions.assertEquals("helloozy", StringUtils.fromUtf8(codedUtf8Indexed.get(4)));

    Iterator<ByteBuffer> utf8Iterator = codedUtf8Indexed.iterator();
    Iterator<String> newListIterator = theList.iterator();
    int ctr = 0;
    while (newListIterator.hasNext() && utf8Iterator.hasNext()) {
      final String next = newListIterator.next();
      final ByteBuffer nextUtf8 = utf8Iterator.next();
      Assertions.assertEquals(next, StringUtils.fromUtf8(nextUtf8));
      nextUtf8.position(0);
      Assertions.assertEquals(next, StringUtils.fromUtf8(codedUtf8Indexed.get(ctr)));
      Assertions.assertEquals(ctr, codedUtf8Indexed.indexOf(nextUtf8));
      ctr++;
    }
    Assertions.assertEquals(newListIterator.hasNext(), utf8Iterator.hasNext());
  }


  @Test
  public void testFrontCodedIndexedSingleBucket() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12).order(order);
    List<String> theList = ImmutableList.of("hello", "helloo", "hellooo", "hellooz", "helloozy");
    persistToBuffer(buffer, theList, 16, version);

    FrontCodedIndexed codedUtf8Indexed = FrontCodedIndexed.read(
        buffer,
        buffer.order()
    ).get();
    Assertions.assertEquals("hello", StringUtils.fromUtf8(codedUtf8Indexed.get(0)));
    Assertions.assertEquals("helloo", StringUtils.fromUtf8(codedUtf8Indexed.get(1)));
    Assertions.assertEquals("hellooo", StringUtils.fromUtf8(codedUtf8Indexed.get(2)));
    Assertions.assertEquals("hellooz", StringUtils.fromUtf8(codedUtf8Indexed.get(3)));
    Assertions.assertEquals("helloozy", StringUtils.fromUtf8(codedUtf8Indexed.get(4)));

    Iterator<String> newListIterator = theList.iterator();
    Iterator<ByteBuffer> utf8Iterator = codedUtf8Indexed.iterator();
    int ctr = 0;
    while (utf8Iterator.hasNext() && newListIterator.hasNext()) {
      final String next = newListIterator.next();
      final ByteBuffer nextUtf8 = utf8Iterator.next();
      Assertions.assertEquals(next, StringUtils.fromUtf8(nextUtf8));
      nextUtf8.position(0);
      Assertions.assertEquals(next, StringUtils.fromUtf8(codedUtf8Indexed.get(ctr)));
      Assertions.assertEquals(ctr, codedUtf8Indexed.indexOf(nextUtf8));
      ctr++;
    }
    Assertions.assertEquals(newListIterator.hasNext(), utf8Iterator.hasNext());
  }

  @Test
  public void testFrontCodedIndexedBigger() throws IOException
  {
    final int sizeBase = 10000;
    final int bucketSize = 16;
    final ByteBuffer buffer = ByteBuffer.allocate(1 << 24).order(order);
    for (int sizeAdjust = 0; sizeAdjust < bucketSize; sizeAdjust++) {
      final TreeSet<String> values = new TreeSet<>(GenericIndexed.STRING_STRATEGY);
      for (int i = 0; i < sizeBase + sizeAdjust; i++) {
        values.add(IdUtils.getRandomId() + IdUtils.getRandomId() + IdUtils.getRandomId() + IdUtils.getRandomId());
      }
      persistToBuffer(buffer, values, bucketSize, version);

      FrontCodedIndexed codedUtf8Indexed = FrontCodedIndexed.read(
          buffer,
          buffer.order()
      ).get();

      Iterator<String> newListIterator = values.iterator();
      Iterator<ByteBuffer> utf8Iterator = codedUtf8Indexed.iterator();
      int ctr = 0;
      while (utf8Iterator.hasNext() && newListIterator.hasNext()) {
        final String next = newListIterator.next();
        final ByteBuffer nextUtf8 = utf8Iterator.next();
        Assertions.assertEquals(next, StringUtils.fromUtf8(nextUtf8));
        nextUtf8.position(0);
        Assertions.assertEquals(next, StringUtils.fromUtf8(codedUtf8Indexed.get(ctr)));
        Assertions.assertEquals(ctr, codedUtf8Indexed.indexOf(nextUtf8));
        ctr++;
      }
      Assertions.assertEquals(newListIterator.hasNext(), utf8Iterator.hasNext());
      Assertions.assertEquals(ctr, sizeBase + sizeAdjust);
    }
  }

  @Test
  public void testFrontCodedIndexedBiggerWithNulls() throws IOException
  {
    final int sizeBase = 10000;
    final int bucketSize = 16;
    final ByteBuffer buffer = ByteBuffer.allocate(1 << 24).order(order);
    for (int sizeAdjust = 0; sizeAdjust < bucketSize; sizeAdjust++) {
      TreeSet<String> values = new TreeSet<>(GenericIndexed.STRING_STRATEGY);
      values.add(null);
      for (int i = 0; i < sizeBase + sizeAdjust; i++) {
        values.add(IdUtils.getRandomId() + IdUtils.getRandomId() + IdUtils.getRandomId() + IdUtils.getRandomId());
      }
      persistToBuffer(buffer, values, bucketSize, version);

      FrontCodedIndexed codedUtf8Indexed = FrontCodedIndexed.read(
          buffer,
          buffer.order()
      ).get();

      Iterator<String> newListIterator = values.iterator();
      Iterator<ByteBuffer> utf8Iterator = codedUtf8Indexed.iterator();
      int ctr = 0;
      while (utf8Iterator.hasNext() && newListIterator.hasNext()) {
        final String next = newListIterator.next();
        final ByteBuffer nextUtf8 = utf8Iterator.next();
        if (next == null) {
          Assertions.assertNull(nextUtf8);
        } else {
          Assertions.assertEquals(next, StringUtils.fromUtf8(nextUtf8));
          nextUtf8.position(0);
          Assertions.assertEquals(next, StringUtils.fromUtf8(codedUtf8Indexed.get(ctr)));
        }
        Assertions.assertEquals(ctr, codedUtf8Indexed.indexOf(nextUtf8));
        ctr++;
      }
      Assertions.assertEquals(newListIterator.hasNext(), utf8Iterator.hasNext());
      Assertions.assertEquals(ctr, sizeBase + sizeAdjust + 1);
    }
  }

  @Test
  public void testFrontCodedIndexedIndexOf() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12).order(order);
    List<String> theList = ImmutableList.of("hello", "helloo", "hellooo", "hellooz", "helloozy");

    persistToBuffer(buffer, theList, 4, version);

    FrontCodedIndexed codedUtf8Indexed = FrontCodedIndexed.read(
        buffer,
        buffer.order()
    ).get();
    Assertions.assertEquals(-1, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("a")));
    Assertions.assertEquals(0, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("hello")));
    Assertions.assertEquals(1, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloo")));
    Assertions.assertEquals(-3, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloob")));
    Assertions.assertEquals(4, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloozy")));
    Assertions.assertEquals(-6, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloozz")));
    Assertions.assertEquals(-6, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("wat")));
  }


  @Test
  public void testFrontCodedIndexedIndexOfWithNull() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12).order(order);
    List<String> theList = ImmutableList.of("hello", "helloo", "hellooo", "hellooz", "helloozy");
    TreeSet<String> values = new TreeSet<>(GenericIndexed.STRING_STRATEGY);
    values.add(null);
    values.addAll(theList);
    persistToBuffer(buffer, values, 4, version);

    FrontCodedIndexed codedUtf8Indexed = FrontCodedIndexed.read(
        buffer,
        buffer.order()
    ).get();
    Assertions.assertEquals(0, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer(null)));
    Assertions.assertEquals(-2, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("a")));
    Assertions.assertEquals(1, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("hello")));
    Assertions.assertEquals(2, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloo")));
    Assertions.assertEquals(-4, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloob")));
    Assertions.assertEquals(5, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloozy")));
    Assertions.assertEquals(-7, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloozz")));
    Assertions.assertEquals(-7, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("wat")));
  }

  @Test
  public void testFrontCodedIndexedUnicodes() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12).order(order);

    // "\uD83D\uDCA9" and "（請參見已被刪除版本）" are a regression test for https://github.com/apache/druid/pull/13364
    List<String> theList = ImmutableList.of("Győ-Moson-Sopron", "Győr", "\uD83D\uDCA9", "（請參見已被刪除版本）");
    persistToBuffer(buffer, theList, 4, version);

    buffer.position(0);
    FrontCodedIndexed codedUtf8Indexed = FrontCodedIndexed.read(
        buffer,
        buffer.order()
    ).get();

    Iterator<ByteBuffer> utf8Iterator = codedUtf8Indexed.iterator();
    Iterator<String> newListIterator = theList.iterator();
    int ctr = 0;
    while (newListIterator.hasNext() && utf8Iterator.hasNext()) {
      final String next = newListIterator.next();
      final ByteBuffer nextUtf8 = utf8Iterator.next();
      Assertions.assertEquals(next, StringUtils.fromUtf8(nextUtf8));
      nextUtf8.position(0);
      Assertions.assertEquals(next, StringUtils.fromUtf8(codedUtf8Indexed.get(ctr)), "mismatch row " + ctr);
      Assertions.assertEquals(ctr, codedUtf8Indexed.indexOf(nextUtf8));
      ctr++;
    }
    Assertions.assertEquals(newListIterator.hasNext(), utf8Iterator.hasNext());
  }

  @Test
  public void testFrontCodedOnlyNull() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12).order(order);
    List<String> theList = Collections.singletonList(null);
    persistToBuffer(buffer, theList, 4, version);

    buffer.position(0);
    FrontCodedIndexed codedUtf8Indexed = FrontCodedIndexed.read(
        buffer,
        buffer.order()
    ).get();

    Assertions.assertNull(codedUtf8Indexed.get(0));
    Assertions.assertThrows(IllegalArgumentException.class, () -> codedUtf8Indexed.get(-1));
    Assertions.assertThrows(IllegalArgumentException.class, () -> codedUtf8Indexed.get(theList.size()));

    Assertions.assertEquals(0, codedUtf8Indexed.indexOf(null));
    Assertions.assertEquals(-2, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("hello")));

    Iterator<ByteBuffer> utf8Iterator = codedUtf8Indexed.iterator();
    Assertions.assertTrue(utf8Iterator.hasNext());
    Assertions.assertNull(utf8Iterator.next());
    Assertions.assertFalse(utf8Iterator.hasNext());
  }

  @Test
  public void testFrontCodedEmpty() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 6).order(order);
    List<String> theList = Collections.emptyList();
    persistToBuffer(buffer, theList, 4, version);

    buffer.position(0);
    FrontCodedIndexed codedUtf8Indexed = FrontCodedIndexed.read(
        buffer,
        buffer.order()
    ).get();

    Assertions.assertEquals(0, codedUtf8Indexed.size());
    Throwable t = Assertions.assertThrows(IAE.class, () -> codedUtf8Indexed.get(0));
    Assertions.assertEquals("Index[0] >= size[0]", t.getMessage());
    Assertions.assertThrows(IllegalArgumentException.class, () -> codedUtf8Indexed.get(-1));
    Assertions.assertThrows(IllegalArgumentException.class, () -> codedUtf8Indexed.get(theList.size()));

    Assertions.assertEquals(-1, codedUtf8Indexed.indexOf(null));
    Assertions.assertEquals(-1, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("hello")));

    Iterator<ByteBuffer> utf8Iterator = codedUtf8Indexed.iterator();
    Assertions.assertFalse(utf8Iterator.hasNext());
  }

  @Test
  public void testBucketSizes() throws IOException
  {
    final int numValues = 10000;
    final ByteBuffer buffer = ByteBuffer.allocate(1 << 24).order(order);
    final int[] bucketSizes = new int[] {
        1,
        1 << 1,
        1 << 2,
        1 << 3,
        1 << 4,
        1 << 5,
        1 << 6,
        1 << 7
    };

    TreeSet<String> values = new TreeSet<>(GenericIndexed.STRING_STRATEGY);
    values.add(null);
    for (int i = 0; i < numValues; i++) {
      values.add(IdUtils.getRandomId() + IdUtils.getRandomId() + IdUtils.getRandomId() + IdUtils.getRandomId());
    }
    for (int bucketSize : bucketSizes) {
      persistToBuffer(buffer, values, bucketSize, version);
      FrontCodedIndexed codedUtf8Indexed = FrontCodedIndexed.read(
          buffer,
          buffer.order()
      ).get();

      Iterator<String> newListIterator = values.iterator();
      Iterator<ByteBuffer> utf8Iterator = codedUtf8Indexed.iterator();
      int ctr = 0;
      while (utf8Iterator.hasNext() && newListIterator.hasNext()) {
        final String next = newListIterator.next();
        final ByteBuffer nextUtf8 = utf8Iterator.next();
        if (next == null) {
          Assertions.assertNull(nextUtf8);
        } else {
          Assertions.assertEquals(next, StringUtils.fromUtf8(nextUtf8));
          nextUtf8.position(0);
          Assertions.assertEquals(next, StringUtils.fromUtf8(codedUtf8Indexed.get(ctr)));
        }
        Assertions.assertEquals(ctr, codedUtf8Indexed.indexOf(nextUtf8));
        ctr++;
      }
      Assertions.assertEquals(newListIterator.hasNext(), utf8Iterator.hasNext());
      Assertions.assertEquals(ctr, numValues + 1);
    }
  }

  @Test
  public void testBadBucketSize()
  {
    OnHeapMemorySegmentWriteOutMedium medium = new OnHeapMemorySegmentWriteOutMedium();

    Assertions.assertThrows(
        IAE.class,
        () -> new FrontCodedIndexedWriter(
            medium,
            ByteOrder.nativeOrder(),
            0,
            version
        )
    );

    Assertions.assertThrows(
        IAE.class,
        () -> new FrontCodedIndexedWriter(
            medium,
            ByteOrder.nativeOrder(),
            15,
            version
        )
    );

    Assertions.assertThrows(
        IAE.class,
        () -> new FrontCodedIndexedWriter(
            medium,
            ByteOrder.nativeOrder(),
            256,
            version
        )
    );
  }

  private static long persistToBuffer(
      ByteBuffer buffer,
      Iterable<String> sortedIterable,
      int bucketSize,
      byte version
  ) throws IOException
  {
    Iterator<String> sortedStrings = sortedIterable.iterator();
    buffer.position(0);
    OnHeapMemorySegmentWriteOutMedium medium = new OnHeapMemorySegmentWriteOutMedium();
    DictionaryWriter<byte[]> writer;
    writer = new FrontCodedIndexedWriter(
        medium,
        buffer.order(),
        bucketSize,
        version
    );
    writer.open();
    int index = 0;
    while (sortedStrings.hasNext()) {
      final String next = sortedStrings.next();
      final byte[] nextBytes = StringUtils.toUtf8Nullable(next);
      Assertions.assertEquals(index, writer.write(nextBytes));
      if (nextBytes == null) {
        Assertions.assertNull(writer.get(index));
      } else {
        Assertions.assertArrayEquals(nextBytes, writer.get(index));
      }
      index++;
    }
    Assertions.assertEquals(index, writer.getCardinality());

    // check 'get' again so that we aren't always reading from current page
    index = 0;
    sortedStrings = sortedIterable.iterator();
    while (sortedStrings.hasNext()) {
      final String next = sortedStrings.next();
      final byte[] nextBytes = StringUtils.toUtf8Nullable(next);
      if (nextBytes == null) {
        Assertions.assertNull(writer.get(index), "row " + index);
      } else {
        Assertions.assertArrayEquals(nextBytes, writer.get(index), "row " + index);
      }
      index++;
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
    return size;
  }
}

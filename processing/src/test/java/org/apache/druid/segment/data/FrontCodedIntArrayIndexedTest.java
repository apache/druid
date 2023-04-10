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
import junitparams.converters.Nullable;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

@RunWith(Parameterized.class)
public class FrontCodedIntArrayIndexedTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{ByteOrder.LITTLE_ENDIAN}, new Object[]{ByteOrder.BIG_ENDIAN});
  }

  private final ByteOrder order;

  public FrontCodedIntArrayIndexedTest(ByteOrder byteOrder)
  {
    this.order = byteOrder;
  }

  @Test
  public void testFrontCodedIntArrayIndexed() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12).order(order);
    TreeSet<int[]> values = new TreeSet<>(FrontCodedIntArrayIndexedWriter.ARRAY_COMPARATOR);
    values.add(new int[]{1, 2, 3});
    values.add(new int[]{1, 2});
    values.add(new int[]{1, 3});
    values.add(new int[]{1, 2, 4});
    values.add(new int[]{1, 3, 4});
    values.add(new int[]{1, 2, 1});
    values.add(new int[]{2, 1});
    values.add(new int[]{2, 2, 1});

    persistToBuffer(buffer, values, 4);

    buffer.position(0);
    FrontCodedIntArrayIndexed codedIndexed = FrontCodedIntArrayIndexed.read(
        buffer,
        buffer.order()
    ).get();

    Iterator<int[]> indexedIterator = codedIndexed.iterator();
    Iterator<int[]> expectedIterator = values.iterator();
    int ctr = 0;
    while (expectedIterator.hasNext() && indexedIterator.hasNext()) {
      final int[] expectedNext = expectedIterator.next();
      final int[] next = indexedIterator.next();
      assertSame(ctr, expectedNext, next);
      assertSame(ctr, expectedNext, codedIndexed.get(ctr));
      Assert.assertEquals("row " + ctr, ctr, codedIndexed.indexOf(next));
      ctr++;
    }
    Assert.assertEquals(expectedIterator.hasNext(), indexedIterator.hasNext());
  }


  @Test
  public void testFrontCodedIntArrayIndexedSingleBucket() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12).order(order);
    TreeSet<int[]> values = new TreeSet<>(FrontCodedIntArrayIndexedWriter.ARRAY_COMPARATOR);
    values.add(new int[]{1, 2, 3});
    values.add(new int[]{1, 2});
    values.add(new int[]{1, 3});
    values.add(new int[]{1, 2, 4});
    values.add(new int[]{1, 3, 4});
    values.add(new int[]{1, 2, 1});
    persistToBuffer(buffer, values, 16);

    FrontCodedIntArrayIndexed codedIndexed = FrontCodedIntArrayIndexed.read(
        buffer,
        buffer.order()
    ).get();

    Iterator<int[]> expectedIterator = values.iterator();
    Iterator<int[]> indexedIterator = codedIndexed.iterator();
    int ctr = 0;
    while (indexedIterator.hasNext() && expectedIterator.hasNext()) {
      final int[] expectedNext = expectedIterator.next();
      final int[] next = indexedIterator.next();
      assertSame(ctr, expectedNext, next);
      assertSame(ctr, expectedNext, codedIndexed.get(ctr));
      Assert.assertEquals(ctr, codedIndexed.indexOf(next));
      ctr++;
    }
    Assert.assertEquals(expectedIterator.hasNext(), indexedIterator.hasNext());
  }

  @Test
  public void testFrontCodedIntArrayIndexedBigger() throws IOException
  {
    final int sizeBase = 10000;
    final int bucketSize = 16;
    final ByteBuffer buffer = ByteBuffer.allocate(1 << 24).order(order);
    for (int sizeAdjust = 0; sizeAdjust < bucketSize; sizeAdjust++) {
      final TreeSet<int[]> values = new TreeSet<>(FrontCodedIntArrayIndexedWriter.ARRAY_COMPARATOR);
      while (values.size() < sizeBase + sizeAdjust) {
        int length = ThreadLocalRandom.current().nextInt(10);
        final int[] val = new int[length];
        for (int j = 0; j < length; j++) {
          val[j] = ThreadLocalRandom.current().nextInt(0, 10_000);
        }
        values.add(val);
      }
      persistToBuffer(buffer, values, bucketSize);

      FrontCodedIntArrayIndexed codedIndexed = FrontCodedIntArrayIndexed.read(
          buffer,
          buffer.order()
      ).get();

      Iterator<int[]> expectedIterator = values.iterator();
      Iterator<int[]> indexedIterator = codedIndexed.iterator();
      int ctr = 0;
      while (indexedIterator.hasNext() && expectedIterator.hasNext()) {
        final int[] expectedNext = expectedIterator.next();
        final int[] next = indexedIterator.next();
        assertSame(ctr, expectedNext, next);
        assertSame(ctr, expectedNext, codedIndexed.get(ctr));
        Assert.assertEquals(ctr, codedIndexed.indexOf(next));
        ctr++;
      }
      Assert.assertEquals(expectedIterator.hasNext(), indexedIterator.hasNext());
      Assert.assertEquals(ctr, sizeBase + sizeAdjust);
    }
  }

  @Test
  public void testFrontCodedIntArrayIndexedBiggerWithNulls() throws IOException
  {
    final int sizeBase = 10000;
    final int bucketSize = 16;
    final ByteBuffer buffer = ByteBuffer.allocate(1 << 25).order(order);
    for (int sizeAdjust = 0; sizeAdjust < bucketSize; sizeAdjust++) {
      TreeSet<int[]> values = new TreeSet<>(FrontCodedIntArrayIndexedWriter.ARRAY_COMPARATOR);
      values.add(null);
      while (values.size() < sizeBase + sizeAdjust + 1) {
        int length = ThreadLocalRandom.current().nextInt(10);
        final int[] val = new int[length];
        for (int j = 0; j < length; j++) {
          val[j] = ThreadLocalRandom.current().nextInt(0, 10_000);
        }
        values.add(val);
      }
      persistToBuffer(buffer, values, bucketSize);

      FrontCodedIntArrayIndexed codedIndexed = FrontCodedIntArrayIndexed.read(
          buffer,
          buffer.order()
      ).get();

      Iterator<int[]> expectedIterator = values.iterator();
      Iterator<int[]> indexedIterator = codedIndexed.iterator();
      int ctr = 0;
      while (indexedIterator.hasNext() && expectedIterator.hasNext()) {
        final int[] expectedNext = expectedIterator.next();
        final int[] next = indexedIterator.next();
        assertSame(ctr, expectedNext, next);
        assertSame(ctr, expectedNext, codedIndexed.get(ctr));
        Assert.assertEquals(ctr, codedIndexed.indexOf(next));
        ctr++;
      }
      Assert.assertEquals(expectedIterator.hasNext(), indexedIterator.hasNext());
      Assert.assertEquals(ctr, sizeBase + sizeAdjust + 1);
    }
  }

  @Test
  public void testFrontCodedIntArrayIndexedIndexOf() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12).order(order);
    TreeSet<int[]> values = new TreeSet<>(FrontCodedIntArrayIndexedWriter.ARRAY_COMPARATOR);
    values.add(new int[]{1, 2});
    values.add(new int[]{1, 2, 1});
    values.add(new int[]{1, 2, 3});
    values.add(new int[]{1, 2, 4});
    values.add(new int[]{1, 3});
    values.add(new int[]{1, 3, 4});

    persistToBuffer(buffer, values, 4);

    FrontCodedIntArrayIndexed codedIndexed = FrontCodedIntArrayIndexed.read(
        buffer,
        buffer.order()
    ).get();
    Assert.assertEquals(-1, codedIndexed.indexOf(new int[]{1}));
    Assert.assertEquals(0, codedIndexed.indexOf(new int[]{1, 2}));
    Assert.assertEquals(1, codedIndexed.indexOf(new int[]{1, 2, 1}));
    Assert.assertEquals(-3, codedIndexed.indexOf(new int[]{1, 2, 2}));
    Assert.assertEquals(4, codedIndexed.indexOf(new int[]{1, 3}));
    Assert.assertEquals(-7, codedIndexed.indexOf(new int[]{1, 4, 4}));
    Assert.assertEquals(-7, codedIndexed.indexOf(new int[]{9, 1, 1}));
  }


  @Test
  public void testFrontCodedIntArrayIndexedIndexOfWithNull() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12).order(order);
    TreeSet<int[]> values = new TreeSet<>(FrontCodedIntArrayIndexedWriter.ARRAY_COMPARATOR);
    values.add(null);
    values.add(new int[]{1, 2});
    values.add(new int[]{1, 2, 1});
    values.add(new int[]{1, 2, 3});
    values.add(new int[]{1, 2, 4});
    values.add(new int[]{1, 3});
    values.add(new int[]{1, 3, 4});
    persistToBuffer(buffer, values, 4);

    FrontCodedIntArrayIndexed codedIndexed = FrontCodedIntArrayIndexed.read(
        buffer,
        buffer.order()
    ).get();
    Assert.assertEquals(0, codedIndexed.indexOf(null));
    Assert.assertEquals(-2, codedIndexed.indexOf(new int[]{1}));
    Assert.assertEquals(1, codedIndexed.indexOf(new int[]{1, 2}));
    Assert.assertEquals(2, codedIndexed.indexOf(new int[]{1, 2, 1}));
    Assert.assertEquals(-4, codedIndexed.indexOf(new int[]{1, 2, 2}));
    Assert.assertEquals(5, codedIndexed.indexOf(new int[]{1, 3}));
    Assert.assertEquals(-8, codedIndexed.indexOf(new int[]{1, 4, 4}));
    Assert.assertEquals(-8, codedIndexed.indexOf(new int[]{9, 1, 1}));
  }


  @Test
  public void testFrontCodedOnlyNull() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12).order(order);
    List<int[]> theList = Collections.singletonList(null);
    persistToBuffer(buffer, theList, 4);

    buffer.position(0);
    FrontCodedIntArrayIndexed codedIndexed = FrontCodedIntArrayIndexed.read(
        buffer,
        buffer.order()
    ).get();

    Assert.assertNull(codedIndexed.get(0));
    Assert.assertThrows(IllegalArgumentException.class, () -> codedIndexed.get(-1));
    Assert.assertThrows(IllegalArgumentException.class, () -> codedIndexed.get(theList.size()));

    Assert.assertEquals(0, codedIndexed.indexOf(null));
    Assert.assertEquals(-2, codedIndexed.indexOf(new int[]{1, 2, 3, 4}));

    Iterator<int[]> iterator = codedIndexed.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testFrontCodedEmpty() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 6).order(order);
    List<int[]> theList = Collections.emptyList();
    persistToBuffer(buffer, theList, 4);

    buffer.position(0);
    FrontCodedIndexed codedUtf8Indexed = FrontCodedIndexed.read(
        buffer,
        buffer.order()
    ).get();

    Assert.assertEquals(0, codedUtf8Indexed.size());
    Throwable t = Assert.assertThrows(IAE.class, () -> codedUtf8Indexed.get(0));
    Assert.assertEquals("Index[0] >= size[0]", t.getMessage());
    Assert.assertThrows(IllegalArgumentException.class, () -> codedUtf8Indexed.get(-1));
    Assert.assertThrows(IllegalArgumentException.class, () -> codedUtf8Indexed.get(theList.size()));

    Assert.assertEquals(-1, codedUtf8Indexed.indexOf(null));
    Assert.assertEquals(-1, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("hello")));

    Iterator<ByteBuffer> utf8Iterator = codedUtf8Indexed.iterator();
    Assert.assertFalse(utf8Iterator.hasNext());
  }

  @Test
  public void testBucketSizes() throws IOException
  {
    final int numValues = 10000;
    final ByteBuffer buffer = ByteBuffer.allocate(1 << 25).order(order);
    final int[] bucketSizes = new int[]{
        1,
        1 << 1,
        1 << 2,
        1 << 3,
        1 << 4,
        1 << 5,
        1 << 6,
        1 << 7
    };

    TreeSet<int[]> values = new TreeSet<>(FrontCodedIntArrayIndexedWriter.ARRAY_COMPARATOR);
    values.add(null);
    while (values.size() < numValues + 1) {
      int length = ThreadLocalRandom.current().nextInt(10);
      final int[] val = new int[length];
      for (int j = 0; j < length; j++) {
        val[j] = ThreadLocalRandom.current().nextInt(0, 10_000);
      }
      values.add(val);
    }
    for (int bucketSize : bucketSizes) {
      persistToBuffer(buffer, values, bucketSize);
      FrontCodedIntArrayIndexed codedIndexed = FrontCodedIntArrayIndexed.read(
          buffer,
          buffer.order()
      ).get();

      Iterator<int[]> expectedIterator = values.iterator();
      Iterator<int[]> iterator = codedIndexed.iterator();
      int ctr = 0;
      while (iterator.hasNext() && expectedIterator.hasNext()) {
        final int[] expectedNext = expectedIterator.next();
        final int[] next = iterator.next();
        assertSame(ctr, expectedNext, next);
        assertSame(ctr, expectedNext, codedIndexed.get(ctr));
        Assert.assertEquals(ctr, codedIndexed.indexOf(next));
        ctr++;
      }
      Assert.assertEquals(expectedIterator.hasNext(), iterator.hasNext());
      Assert.assertEquals(ctr, numValues + 1);
    }
  }

  @Test
  public void testBadBucketSize()
  {
    OnHeapMemorySegmentWriteOutMedium medium = new OnHeapMemorySegmentWriteOutMedium();

    Assert.assertThrows(
        IAE.class,
        () -> new FrontCodedIntArrayIndexedWriter(
            medium,
            ByteOrder.nativeOrder(),
            0
        )
    );

    Assert.assertThrows(
        IAE.class,
        () -> new FrontCodedIntArrayIndexedWriter(
            medium,
            ByteOrder.nativeOrder(),
            15
        )
    );

    Assert.assertThrows(
        IAE.class,
        () -> new FrontCodedIntArrayIndexedWriter(
            medium,
            ByteOrder.nativeOrder(),
            256
        )
    );
  }

  private static long persistToBuffer(ByteBuffer buffer, Iterable<int[]> sortedIterable, int bucketSize) throws IOException
  {
    Iterator<int[]> sortedInts = sortedIterable.iterator();
    buffer.position(0);
    OnHeapMemorySegmentWriteOutMedium medium = new OnHeapMemorySegmentWriteOutMedium();
    FrontCodedIntArrayIndexedWriter writer = new FrontCodedIntArrayIndexedWriter(
        medium,
        buffer.order(),
        bucketSize
    );
    writer.open();
    int index = 0;
    while (sortedInts.hasNext()) {
      final int[] next = sortedInts.next();
      writer.write(next);
      assertSame(index, next, writer.get(index));
      index++;
    }
    Assert.assertEquals(index, writer.getCardinality());

    // check 'get' again so that we aren't always reading from current page
    index = 0;
    sortedInts = sortedIterable.iterator();
    while (sortedInts.hasNext()) {
      assertSame(index, sortedInts.next(), writer.get(index));
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
    Assert.assertEquals(size, buffer.position());
    buffer.position(0);
    return size;
  }

  private static void assertSame(int index, @Nullable int[] expected, @Nullable int[] actual)
  {
    if (expected == null) {
      Assert.assertNull("row " + index, actual);
    } else {
      Assert.assertArrayEquals(
          "row " + index + " expected: " + Arrays.toString(expected) + " actual: " + Arrays.toString(actual),
          expected,
          actual
      );
    }
  }
}

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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

@RunWith(Parameterized.class)
public class FrontCodedIndexedTest extends InitializedNullHandlingTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{ByteOrder.LITTLE_ENDIAN}, new Object[]{ByteOrder.BIG_ENDIAN});
  }

  private final ByteOrder order;

  public FrontCodedIndexedTest(ByteOrder byteOrder)
  {
    this.order = byteOrder;
  }

  @Test
  public void testFrontCodedIndexed() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12).order(order);
    List<String> theList = ImmutableList.of("hello", "helloo", "hellooo", "hellooz", "helloozy");
    fillBuffer(buffer, theList.iterator(), 4);

    FrontCodedIndexed codedIndexed = FrontCodedIndexed.read(buffer, buffer.order());
    buffer.position(0);
    FrontCodedIndexedUtf8 codedUtf8Indexed = FrontCodedIndexedUtf8.read(buffer, buffer.order());
    Assert.assertEquals("helloo", codedIndexed.get(1));
    Assert.assertEquals("helloo", StringUtils.fromUtf8(codedUtf8Indexed.get(1)));
    Assert.assertEquals("helloozy", codedIndexed.get(4));
    Assert.assertEquals("helloozy", StringUtils.fromUtf8(codedUtf8Indexed.get(4)));

    Iterator<String> codedIterator = codedIndexed.iterator();
    Iterator<ByteBuffer> utf8Iterator = codedUtf8Indexed.iterator();
    Iterator<String> newListIterator = theList.iterator();
    int ctr = 0;
    while (codedIterator.hasNext() && newListIterator.hasNext() && utf8Iterator.hasNext()) {
      final String next = codedIterator.next();
      Assert.assertEquals(newListIterator.next(), next);
      Assert.assertEquals(next, codedIndexed.get(ctr));
      Assert.assertEquals(ctr, codedIndexed.indexOf(next));
      final ByteBuffer nextUtf8 = utf8Iterator.next();
      Assert.assertEquals(next, StringUtils.fromUtf8(nextUtf8));
      nextUtf8.position(0);
      Assert.assertEquals(next, StringUtils.fromUtf8(codedUtf8Indexed.get(ctr)));
      Assert.assertEquals(ctr, codedUtf8Indexed.indexOf(nextUtf8));
      ctr++;
    }
    Assert.assertEquals(newListIterator.hasNext(), codedIterator.hasNext());
  }


  @Test
  public void testFrontCodedIndexedSingleBucket() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12).order(order);
    List<String> theList = ImmutableList.of("hello", "helloo", "hellooo", "hellooz", "helloozy");
    fillBuffer(buffer, theList.iterator(), 16);

    FrontCodedIndexed codedIndexed = FrontCodedIndexed.read(buffer, buffer.order());
    buffer.position(0);
    FrontCodedIndexedUtf8 codedUtf8Indexed = FrontCodedIndexedUtf8.read(buffer, buffer.order());
    Assert.assertEquals("helloo", codedIndexed.get(1));
    Assert.assertEquals("helloozy", codedIndexed.get(4));

    Iterator<String> codedIterator = codedIndexed.iterator();
    Iterator<String> newListIterator = theList.iterator();
    Iterator<ByteBuffer> utf8Iterator = codedUtf8Indexed.iterator();
    int ctr = 0;
    while (codedIterator.hasNext() && newListIterator.hasNext()) {
      final String next = codedIterator.next();
      Assert.assertEquals(newListIterator.next(), next);
      Assert.assertEquals(next, codedIndexed.get(ctr));
      Assert.assertEquals(ctr, codedIndexed.indexOf(next));
      final ByteBuffer nextUtf8 = utf8Iterator.next();
      Assert.assertEquals(next, StringUtils.fromUtf8(nextUtf8));
      nextUtf8.position(0);
      Assert.assertEquals(next, StringUtils.fromUtf8(codedUtf8Indexed.get(ctr)));
      Assert.assertEquals(ctr, codedUtf8Indexed.indexOf(nextUtf8));
      ctr++;
    }
    Assert.assertEquals(newListIterator.hasNext(), codedIterator.hasNext());
  }

  @Test
  public void testFrontCodedIndexedBigger() throws IOException
  {
    final int sizeBase = 10000;
    final int bucketSize = 16;
    final ByteBuffer buffer = ByteBuffer.allocate(1 << 24).order(order);
    for (int sizeAdjust = 0; sizeAdjust < bucketSize; sizeAdjust++) {
      final TreeSet<String> values = new TreeSet<>(ColumnType.STRING.getNullableStrategy());
      for (int i = 0; i < sizeBase + sizeAdjust; i++) {
        values.add(IdUtils.getRandomId() + IdUtils.getRandomId() + IdUtils.getRandomId() + IdUtils.getRandomId());
      }
      fillBuffer(buffer, values.iterator(), bucketSize);

      FrontCodedIndexed codedIndexed = FrontCodedIndexed.read(buffer, buffer.order());
      buffer.position(0);
      FrontCodedIndexedUtf8 codedUtf8Indexed = FrontCodedIndexedUtf8.read(buffer, buffer.order());

      Iterator<String> codedIterator = codedIndexed.iterator();
      Iterator<String> newListIterator = values.iterator();
      Iterator<ByteBuffer> utf8Iterator = codedUtf8Indexed.iterator();
      int ctr = 0;
      while (codedIterator.hasNext() && newListIterator.hasNext()) {
        final String next = codedIterator.next();
        Assert.assertEquals(newListIterator.next(), next);
        Assert.assertEquals(next, codedIndexed.get(ctr));
        Assert.assertEquals(ctr, codedIndexed.indexOf(next));
        final ByteBuffer nextUtf8 = utf8Iterator.next();
        Assert.assertEquals(next, StringUtils.fromUtf8(nextUtf8));
        nextUtf8.position(0);
        Assert.assertEquals(next, StringUtils.fromUtf8(codedUtf8Indexed.get(ctr)));
        Assert.assertEquals(ctr, codedUtf8Indexed.indexOf(nextUtf8));
        ctr++;
      }
      Assert.assertEquals(newListIterator.hasNext(), codedIterator.hasNext());
      Assert.assertEquals(ctr, sizeBase + sizeAdjust);
    }
  }

  @Test
  public void testFrontCodedIndexedBiggerWithNulls() throws IOException
  {
    final int sizeBase = 10000;
    final int bucketSize = 16;
    final ByteBuffer buffer = ByteBuffer.allocate(1 << 24).order(order);
    for (int sizeAdjust = 0; sizeAdjust < bucketSize; sizeAdjust++) {
      TreeSet<String> values = new TreeSet<>(ColumnType.STRING.getNullableStrategy());
      values.add(null);
      for (int i = 0; i < sizeBase + sizeAdjust; i++) {
        values.add(IdUtils.getRandomId() + IdUtils.getRandomId() + IdUtils.getRandomId() + IdUtils.getRandomId());
      }
      fillBuffer(buffer, values.iterator(), 4);

      FrontCodedIndexed codedIndexed = FrontCodedIndexed.read(buffer, buffer.order());
      buffer.position(0);
      FrontCodedIndexedUtf8 codedUtf8Indexed = FrontCodedIndexedUtf8.read(buffer, buffer.order());

      Iterator<String> codedIterator = codedIndexed.iterator();
      Iterator<String> newListIterator = values.iterator();
      Iterator<ByteBuffer> utf8Iterator = codedUtf8Indexed.iterator();
      int ctr = 0;
      while (codedIterator.hasNext() && newListIterator.hasNext()) {
        final String next = codedIterator.next();
        Assert.assertEquals(newListIterator.next(), next);
        Assert.assertEquals(next, codedIndexed.get(ctr));
        Assert.assertEquals(ctr, codedIndexed.indexOf(next));
        final ByteBuffer nextUtf8 = utf8Iterator.next();
        if (next == null) {
          Assert.assertNull(nextUtf8);
        } else {
          Assert.assertEquals(next, StringUtils.fromUtf8(nextUtf8));
          nextUtf8.position(0);
          Assert.assertEquals(next, StringUtils.fromUtf8(codedUtf8Indexed.get(ctr)));
        }
        Assert.assertEquals(ctr, codedUtf8Indexed.indexOf(nextUtf8));
        ctr++;
      }
      Assert.assertEquals(newListIterator.hasNext(), codedIterator.hasNext());
      Assert.assertEquals(ctr, sizeBase + sizeAdjust + 1);
    }
  }

  @Test
  public void testFrontCodedIndexedIndexOf() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12).order(order);
    List<String> theList = ImmutableList.of("hello", "helloo", "hellooo", "hellooz", "helloozy");

    fillBuffer(buffer, theList.iterator(), 4);

    FrontCodedIndexed codedIndexed = FrontCodedIndexed.read(buffer, buffer.order());
    buffer.position(0);
    FrontCodedIndexedUtf8 codedUtf8Indexed = FrontCodedIndexedUtf8.read(buffer, buffer.order());
    Assert.assertEquals(-1, codedIndexed.indexOf("a"));
    Assert.assertEquals(-1, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("a")));
    Assert.assertEquals(0, codedIndexed.indexOf("hello"));
    Assert.assertEquals(0, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("hello")));
    Assert.assertEquals(1, codedIndexed.indexOf("helloo"));
    Assert.assertEquals(1, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloo")));
    Assert.assertEquals(-3, codedIndexed.indexOf("helloob"));
    Assert.assertEquals(-3, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloob")));
    Assert.assertEquals(4, codedIndexed.indexOf("helloozy"));
    Assert.assertEquals(4, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloozy")));
    Assert.assertEquals(-6, codedIndexed.indexOf("helloozz"));
    Assert.assertEquals(-6, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloozz")));
    Assert.assertEquals(-6, codedIndexed.indexOf("wat"));
    Assert.assertEquals(-6, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("wat")));
  }


  @Test
  public void testFrontCodedIndexedIndexOfWithNull() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12).order(order);
    List<String> theList = ImmutableList.of("hello", "helloo", "hellooo", "hellooz", "helloozy");
    TreeSet<String> values = new TreeSet<>(ColumnType.STRING.getNullableStrategy());
    values.add(null);
    values.addAll(theList);
    fillBuffer(buffer, values.iterator(), 4);

    FrontCodedIndexed codedIndexed = FrontCodedIndexed.read(buffer, buffer.order());
    buffer.position(0);
    FrontCodedIndexedUtf8 codedUtf8Indexed = FrontCodedIndexedUtf8.read(buffer, buffer.order());
    Assert.assertEquals(0, codedIndexed.indexOf(null));
    Assert.assertEquals(0, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer(null)));
    Assert.assertEquals(-2, codedIndexed.indexOf("a"));
    Assert.assertEquals(-2, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("a")));
    Assert.assertEquals(1, codedIndexed.indexOf("hello"));
    Assert.assertEquals(1, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("hello")));
    Assert.assertEquals(2, codedIndexed.indexOf("helloo"));
    Assert.assertEquals(2, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloo")));
    Assert.assertEquals(-4, codedIndexed.indexOf("helloob"));
    Assert.assertEquals(-4, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloob")));
    Assert.assertEquals(5, codedIndexed.indexOf("helloozy"));
    Assert.assertEquals(5, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloozy")));
    Assert.assertEquals(-7, codedIndexed.indexOf("helloozz"));
    Assert.assertEquals(-7, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("helloozz")));
    Assert.assertEquals(-7, codedIndexed.indexOf("wat"));
    Assert.assertEquals(-7, codedUtf8Indexed.indexOf(StringUtils.toUtf8ByteBuffer("wat")));
  }

  private static long fillBuffer(ByteBuffer buffer, Iterator<String> sortedStrings, int bucketSize) throws IOException
  {
    buffer.position(0);
    OnHeapMemorySegmentWriteOutMedium medium = new OnHeapMemorySegmentWriteOutMedium();
    FrontCodedIndexedWriter writer = new FrontCodedIndexedWriter(medium, buffer.order(), bucketSize);
    writer.open();
    while (sortedStrings.hasNext()) {
      writer.write(sortedStrings.next());
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
}

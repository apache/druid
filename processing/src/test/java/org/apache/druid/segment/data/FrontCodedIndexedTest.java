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
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

public class FrontCodedIndexedTest extends InitializedNullHandlingTest
{
  @Test
  public void testVbyte()
  {
    ByteBuffer buffer = ByteBuffer.allocate(24).order(ByteOrder.BIG_ENDIAN);
    roundTrip(buffer, 0, 0, 1);
    roundTrip(buffer, 0, 4, 1);
    roundTrip(buffer, 0, 224, 2);
    roundTrip(buffer, 0, 1024, 2);
    roundTrip(buffer, 0, 1 << 14 - 1, 2);
    roundTrip(buffer, 0, 1 << 14, 3);
    roundTrip(buffer, 0, 1 << 16, 3);
    roundTrip(buffer, 0, 1 << 25, 4);
    roundTrip(buffer, 0, 1 << 28 - 1, 4);
    roundTrip(buffer, 0, 1 << 28, 5);
    roundTrip(buffer, 0, Integer.MAX_VALUE, 5);
  }

  @Test
  public void testVbyteNative()
  {
    ByteBuffer buffer = ByteBuffer.allocate(24).order(ByteOrder.nativeOrder());
    roundTrip(buffer, 0, 0, 1);
    roundTrip(buffer, 0, 4, 1);
    roundTrip(buffer, 0, 224, 2);
    roundTrip(buffer, 0, 1024, 2);
    roundTrip(buffer, 0, 1 << 14 - 1, 2);
    roundTrip(buffer, 0, 1 << 14, 3);
    roundTrip(buffer, 0, 1 << 16, 3);
    roundTrip(buffer, 0, 1 << 25, 4);
    roundTrip(buffer, 0, 1 << 28 - 1, 4);
    roundTrip(buffer, 0, 1 << 28, 5);
    roundTrip(buffer, 0, Integer.MAX_VALUE, 5);
  }

  @Test
  public void testFrontCodedIndexed() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12);
    List<String> theList = ImmutableList.of("hello", "helloo", "hellooo", "hellooz", "helloozy");
    fillBuffer(buffer, theList.iterator(), 4);

    FrontCodedIndexed codedIndexed = new FrontCodedIndexed(buffer, buffer.order());
    Assert.assertEquals("helloo", codedIndexed.get(1));
    Assert.assertEquals("helloozy", codedIndexed.get(4));

    Iterator<String> codedIterator = codedIndexed.iterator();
    Iterator<String> newListIterator = theList.iterator();
    while (codedIterator.hasNext() && newListIterator.hasNext()) {
      Assert.assertEquals(newListIterator.next(), codedIterator.next());
    }
    Assert.assertEquals(newListIterator.hasNext(), codedIterator.hasNext());
  }

  @Test
  public void testFrontCodedIndexedBigger() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 24);
    TreeSet<String> values = new TreeSet<>(ColumnType.STRING.getNullableStrategy());
    for (int i = 0; i < 10000; i++) {
      values.add(IdUtils.getRandomId());
    }
    fillBuffer(buffer, values.iterator(), 4);

    FrontCodedIndexed codedIndexed = new FrontCodedIndexed(buffer, buffer.order());

    Iterator<String> codedIterator = codedIndexed.iterator();
    Iterator<String> newListIterator = values.iterator();
    int ctr = 0;
    while (codedIterator.hasNext() && newListIterator.hasNext()) {
      Assert.assertEquals(newListIterator.next(), codedIterator.next());
    }
    Assert.assertEquals(newListIterator.hasNext(), codedIterator.hasNext());
  }

  @Test
  public void testFrontCodedIndexedBiggerWithNulls() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 24);
    TreeSet<String> values = new TreeSet<>(ColumnType.STRING.getNullableStrategy());
    values.add(null);
    for (int i = 0; i < 10000; i++) {
      values.add(IdUtils.getRandomId());
    }
    fillBuffer(buffer, values.iterator(), 4);

    FrontCodedIndexed codedIndexed = new FrontCodedIndexed(buffer, buffer.order());

    Iterator<String> codedIterator = codedIndexed.iterator();
    Iterator<String> newListIterator = values.iterator();
    int ctr = 0;
    while (codedIterator.hasNext() && newListIterator.hasNext()) {
      Assert.assertEquals(newListIterator.next(), codedIterator.next());
    }
    Assert.assertEquals(newListIterator.hasNext(), codedIterator.hasNext());
  }

  @Test
  public void testFrontCodedIndexedIndexOf() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12);
    List<String> theList = ImmutableList.of("hello", "helloo", "hellooo", "hellooz", "helloozy");

    fillBuffer(buffer, theList.iterator(), 4);

    FrontCodedIndexed codedIndexed = new FrontCodedIndexed(buffer, buffer.order());
    Assert.assertEquals(-1, codedIndexed.indexOf("a"));
    Assert.assertEquals(0, codedIndexed.indexOf("hello"));
    Assert.assertEquals(1, codedIndexed.indexOf("helloo"));
    Assert.assertEquals(-1, codedIndexed.indexOf("helloob"));
    Assert.assertEquals(4, codedIndexed.indexOf("helloozy"));
    Assert.assertEquals(-1, codedIndexed.indexOf("helloozz"));
    Assert.assertEquals(-1, codedIndexed.indexOf("wat"));
  }


  @Test
  public void testFrontCodedIndexedIndexOfWithNull() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12);
    List<String> theList = ImmutableList.of("hello", "helloo", "hellooo", "hellooz", "helloozy");
    TreeSet<String> values = new TreeSet<>(ColumnType.STRING.getNullableStrategy());
    values.add(null);
    values.addAll(theList);
    fillBuffer(buffer, values.iterator(), 4);

    FrontCodedIndexed codedIndexed = new FrontCodedIndexed(buffer, buffer.order());
    Assert.assertEquals(0, codedIndexed.indexOf(null));
    Assert.assertEquals(-1, codedIndexed.indexOf("a"));
    Assert.assertEquals(1, codedIndexed.indexOf("hello"));
    Assert.assertEquals(2, codedIndexed.indexOf("helloo"));
    Assert.assertEquals(-1, codedIndexed.indexOf("helloob"));
    Assert.assertEquals(5, codedIndexed.indexOf("helloozy"));
    Assert.assertEquals(-1, codedIndexed.indexOf("helloozz"));
    Assert.assertEquals(-1, codedIndexed.indexOf("wat"));
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
    buffer.position(0);
    return size;
  }

  private static void roundTrip(ByteBuffer buffer, int position, int value, int expectedSize)
  {
    buffer.position(position);
    FrontCodedIndexed.writeVbyteInt(buffer, value);
    Assert.assertEquals(expectedSize, buffer.position() - position);
    buffer.position(position);
    Assert.assertEquals(value, FrontCodedIndexed.readVbyteInt(buffer));
    Assert.assertEquals(expectedSize, buffer.position() - position);
  }
}

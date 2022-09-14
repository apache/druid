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
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.longs.LongIterator;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.nested.NestedDataColumnSerializer;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Iterator;


@RunWith(Parameterized.class)
public class FixedIndexedTest extends InitializedNullHandlingTest
{
  private static final long[] LONGS = new long[1 << 16];
  private static final int[] INTS = new int[1 << 16];
  private static final double[] DOUBLES = new double[1 << 16];

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{ByteOrder.LITTLE_ENDIAN}, new Object[]{ByteOrder.BIG_ENDIAN});
  }

  @BeforeClass
  public static void setup()
  {
    for (int i = 0; i < LONGS.length; i++) {
      LONGS[i] = i * 2L;
      INTS[i] = i + 1;
      DOUBLES[i] = i * 1.3;
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
    ByteBuffer buffer = ByteBuffer.allocate(1 << 20);
    fillBuffer(buffer, order, false);
    FixedIndexed<Long> fixedIndexed = FixedIndexed.read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES);
    Assert.assertEquals(LONGS.length, fixedIndexed.size());
    for (int i = 0; i < LONGS.length; i++) {
      Assert.assertEquals(LONGS[i], (long) fixedIndexed.get(i));
      Assert.assertEquals(i, fixedIndexed.indexOf(LONGS[i]));
    }
  }

  @Test
  public void testIterator() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 20);
    fillBuffer(buffer, order, false);
    FixedIndexed<Long> fixedIndexed = FixedIndexed.read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES);
    Iterator<Long> iterator = fixedIndexed.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      Assert.assertEquals(LONGS[i++], (long) iterator.next());
    }
  }

  @Test
  public void testGetWithNull() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 20);
    fillBuffer(buffer, order, true);
    FixedIndexed<Long> fixedIndexed = FixedIndexed.read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES);
    Assert.assertEquals(LONGS.length + 1, fixedIndexed.size());
    Assert.assertNull(fixedIndexed.get(0));
    for (int i = 0; i < LONGS.length; i++) {
      Assert.assertEquals(LONGS[i], (long) fixedIndexed.get(i + 1));
      Assert.assertEquals(i + 1, fixedIndexed.indexOf(LONGS[i]));
    }
  }

  @Test
  public void testIteratorWithNull() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 20);
    fillBuffer(buffer, order, true);
    FixedIndexed<Long> fixedIndexed = FixedIndexed.read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES);
    Iterator<Long> iterator = fixedIndexed.iterator();
    Assert.assertNull(iterator.next());
    int i = 0;
    while (iterator.hasNext()) {
      Assert.assertEquals(LONGS[i++], (long) iterator.next());
    }
  }

  @Test
  public void testSpecializedInts() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 20);
    ByteBuffer buffer2 = ByteBuffer.allocate(1 << 20);
    fillIntBuffers(buffer, buffer2, order);
    FixedIndexed<Integer> fixedIndexed = FixedIndexed.read(buffer, NestedDataColumnSerializer.INT_TYPE_STRATEGY, order, Integer.BYTES);
    FixedIndexedInts specializedIndexed = FixedIndexedInts.read(buffer2, order);
    Iterator<Integer> iterator = fixedIndexed.iterator();
    IntIterator intIterator = specializedIndexed.intIterator();
    int i = 0;
    while (iterator.hasNext()) {
      int next = iterator.next();
      int nextInt = intIterator.nextInt();
      final String msg = "row : " + i;
      Assert.assertEquals(msg, INTS[i], next);
      Assert.assertEquals(msg, INTS[i], nextInt);
      Assert.assertEquals(msg, next, (int) fixedIndexed.get(i));
      Assert.assertEquals(msg, nextInt, specializedIndexed.getInt(i));
      Assert.assertEquals(msg, i, fixedIndexed.indexOf(next));
      Assert.assertEquals(msg, i, specializedIndexed.indexOf(nextInt));
      i++;
    }
    Assert.assertFalse(intIterator.hasNext());
  }

  @Test
  public void testSpecializedLongs() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 20);
    ByteBuffer buffer2 = ByteBuffer.allocate(1 << 20);
    fillLongBuffers(buffer, buffer2, order);
    FixedIndexed<Long> fixedIndexed = FixedIndexed.read(buffer, TypeStrategies.LONG, order, Long.BYTES);
    FixedIndexedLongs specializedIndexed = FixedIndexedLongs.read(buffer2, order);
    Iterator<Long> iterator = fixedIndexed.iterator();
    LongIterator intIterator = specializedIndexed.longIterator();
    int i = 0;
    while (iterator.hasNext()) {
      long next = iterator.next();
      long nextLong = intIterator.nextLong();
      final String msg = "row : " + i;
      Assert.assertEquals(msg, LONGS[i], next);
      Assert.assertEquals(msg, LONGS[i], nextLong);
      Assert.assertEquals(msg, next, (long) fixedIndexed.get(i));
      Assert.assertEquals(msg, nextLong, specializedIndexed.getLong(i));
      Assert.assertEquals(msg, i, fixedIndexed.indexOf(next));
      Assert.assertEquals(msg, i, specializedIndexed.indexOf(nextLong));
      i++;
    }
    Assert.assertFalse(intIterator.hasNext());
  }

  @Test
  public void testSpecializedDoubles() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 20);
    ByteBuffer buffer2 = ByteBuffer.allocate(1 << 20);
    fillDoubleBuffers(buffer, buffer2, order);
    FixedIndexed<Double> fixedIndexed = FixedIndexed.read(buffer, TypeStrategies.DOUBLE, order, Double.BYTES);
    FixedIndexedDoubles specializedIndexed = FixedIndexedDoubles.read(buffer2, order);
    Iterator<Double> iterator = fixedIndexed.iterator();
    DoubleIterator intIterator = specializedIndexed.doubleIterator();
    int i = 0;
    while (iterator.hasNext()) {
      double next = iterator.next();
      double nextDouble = intIterator.nextDouble();
      final String msg = "row : " + i;
      Assert.assertEquals(msg, DOUBLES[i], next, 0.0);
      Assert.assertEquals(msg, DOUBLES[i], nextDouble, 0.0);
      Assert.assertEquals(msg, next, fixedIndexed.get(i), 0.0);
      Assert.assertEquals(msg, nextDouble, specializedIndexed.getDouble(i), 0.0);
      Assert.assertEquals(msg, i, fixedIndexed.indexOf(next));
      Assert.assertEquals(msg, i, specializedIndexed.indexOf(nextDouble));
      i++;
    }
    Assert.assertFalse(intIterator.hasNext());
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
    WritableByteChannel channel = makeChannelForBuffer(buffer);
    long size = writer.getSerializedSize();
    Iterator<Long> validationIterator = writer.getIterator();
    int i = 0;
    boolean processFirstNull = withNull;
    while (validationIterator.hasNext()) {
      if (processFirstNull) {
        Assert.assertNull(validationIterator.next());
        processFirstNull = false;
      } else {
        Assert.assertEquals(LONGS[i++], (long) validationIterator.next());
      }
    }
    buffer.position(0);
    writer.writeTo(channel, null);
    Assert.assertEquals(size, buffer.position());
    buffer.position(0);
  }

  private static void fillIntBuffers(ByteBuffer buffer, ByteBuffer specialized, ByteOrder order) throws IOException
  {
    buffer.position(0);
    Serializer genericWriter, specializedWriter;
    FixedIndexedWriter<Integer> writer = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        NestedDataColumnSerializer.INT_TYPE_STRATEGY,
        order,
        Integer.BYTES,
        true
    );
    writer.open();
    genericWriter = writer;

    if (order == ByteOrder.nativeOrder()) {
      FixedIndexedIntWriter intWriter = new FixedIndexedIntWriter(
          new OnHeapMemorySegmentWriteOutMedium(),
          true
      );
      specializedWriter = intWriter;
      intWriter.open();
      for (Integer val : INTS) {
        writer.write(val);
        intWriter.write(val);
      }

      IntIterator validationIterator = intWriter.getIterator();
      int i = 0;
      while (validationIterator.hasNext()) {
        Assert.assertEquals("row : " + i, INTS[i++], validationIterator.nextInt());
      }
    } else {
      FixedIndexedWriter<Integer> fallbackWriter = new FixedIndexedWriter<>(
          new OnHeapMemorySegmentWriteOutMedium(),
          NestedDataColumnSerializer.INT_TYPE_STRATEGY,
          order,
          Integer.BYTES,
          true
      );
      fallbackWriter.open();
      specializedWriter = fallbackWriter;
      for (Integer val : INTS) {
        writer.write(val);
        fallbackWriter.write(val);
      }
    }
    WritableByteChannel channel = makeChannelForBuffer(buffer);
    long size = genericWriter.getSerializedSize();
    buffer.position(0);
    writer.writeTo(channel, null);
    Assert.assertEquals(size, buffer.position());
    buffer.position(0);

    WritableByteChannel specializedChannel = makeChannelForBuffer(specialized);
    long sizeSpecialized = specializedWriter.getSerializedSize();
    Assert.assertEquals(size, sizeSpecialized);
    specialized.position(0);
    specializedWriter.writeTo(specializedChannel, null);
    Assert.assertEquals(sizeSpecialized, specialized.position());
    specialized.position(0);
  }

  private static void fillLongBuffers(ByteBuffer buffer, ByteBuffer specialized, ByteOrder order) throws IOException
  {
    buffer.position(0);
    Serializer genericWriter, specializedWriter;
    FixedIndexedWriter<Long> writer = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        TypeStrategies.LONG,
        order,
        Long.BYTES,
        true
    );
    writer.open();
    genericWriter = writer;

    if (order == ByteOrder.nativeOrder()) {
      FixedIndexedLongWriter longWriter = new FixedIndexedLongWriter(
          new OnHeapMemorySegmentWriteOutMedium(),
          true
      );
      specializedWriter = longWriter;
      longWriter.open();
      for (Long val : LONGS) {
        writer.write(val);
        longWriter.write(val);
      }
      LongIterator validationIterator = longWriter.getIterator();
      int i = 0;
      while (validationIterator.hasNext()) {
        Assert.assertEquals("row : " + i, LONGS[i++], validationIterator.nextLong());
      }
    } else {
      FixedIndexedWriter<Long> fallbackWriter = new FixedIndexedWriter<>(
          new OnHeapMemorySegmentWriteOutMedium(),
          TypeStrategies.LONG,
          order,
          Long.BYTES,
          true
      );
      fallbackWriter.open();
      specializedWriter = fallbackWriter;
      for (Long val : LONGS) {
        writer.write(val);
        fallbackWriter.write(val);
      }
    }
    WritableByteChannel channel = makeChannelForBuffer(buffer);
    long size = genericWriter.getSerializedSize();
    buffer.position(0);
    writer.writeTo(channel, null);
    Assert.assertEquals(size, buffer.position());
    buffer.position(0);

    WritableByteChannel specializedChannel = makeChannelForBuffer(specialized);
    long sizeSpecialized = specializedWriter.getSerializedSize();
    Assert.assertEquals(size, sizeSpecialized);
    specialized.position(0);
    specializedWriter.writeTo(specializedChannel, null);
    Assert.assertEquals(sizeSpecialized, specialized.position());
    specialized.position(0);
  }

  private static void fillDoubleBuffers(ByteBuffer buffer, ByteBuffer specialized, ByteOrder order) throws IOException
  {
    buffer.position(0);
    Serializer genericWriter, specializedWriter;
    FixedIndexedWriter<Double> writer = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        TypeStrategies.DOUBLE,
        order,
        Double.BYTES,
        true
    );
    writer.open();
    genericWriter = writer;

    if (order == ByteOrder.nativeOrder()) {
      FixedIndexedDoubleWriter doubleWriter = new FixedIndexedDoubleWriter(
          new OnHeapMemorySegmentWriteOutMedium(),
          true
      );
      specializedWriter = doubleWriter;
      doubleWriter.open();
      for (Double val : DOUBLES) {
        writer.write(val);
        doubleWriter.write(val);
      }
      DoubleIterator validationIterator = doubleWriter.getIterator();
      int i = 0;
      while (validationIterator.hasNext()) {
        Assert.assertEquals("row : " + i, DOUBLES[i++], validationIterator.nextDouble(), 0.0);
      }
    } else {
      FixedIndexedWriter<Double> fallbackWriter = new FixedIndexedWriter<>(
          new OnHeapMemorySegmentWriteOutMedium(),
          TypeStrategies.DOUBLE,
          order,
          Double.BYTES,
          true
      );
      fallbackWriter.open();
      specializedWriter = fallbackWriter;
      for (Double val : DOUBLES) {
        writer.write(val);
        fallbackWriter.write(val);
      }
    }
    WritableByteChannel channel = makeChannelForBuffer(buffer);
    long size = genericWriter.getSerializedSize();
    buffer.position(0);
    writer.writeTo(channel, null);
    Assert.assertEquals(size, buffer.position());
    buffer.position(0);

    WritableByteChannel specializedChannel = makeChannelForBuffer(specialized);
    long sizeSpecialized = specializedWriter.getSerializedSize();
    Assert.assertEquals(size, sizeSpecialized);
    specialized.position(0);
    specializedWriter.writeTo(specializedChannel, null);
    Assert.assertEquals(sizeSpecialized, specialized.position());
    specialized.position(0);
  }

  @Nonnull
  private static WritableByteChannel makeChannelForBuffer(ByteBuffer buffer)
  {
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
    return channel;
  }
}

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

package org.apache.druid.query.aggregation;

import com.google.common.primitives.Ints;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.serde.cell.IOIterator;
import org.apache.druid.segment.serde.cell.NativeClearedByteBufferProvider;
import org.apache.druid.segment.writeout.HeapByteBufferWriteOutBytes;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

public class SerializablePairLongLongBufferStoreTest
{
  private final Random random = new Random(0);
  private static final int MIN_INTEGER = 100;
  private static final long MIN_LONG = 0L;
  private final SerializablePairLongLong[] integerRangeArr = new SerializablePairLongLong[]{
      new SerializablePairLongLong((long) MIN_INTEGER, 10L),
      new SerializablePairLongLong(101L, 20L),
      new SerializablePairLongLong(102L, 30L),
      };

  private final SerializablePairLongLong[] longRangeArr = new SerializablePairLongLong[]{
      new SerializablePairLongLong((long) MIN_LONG, 10L),
      new SerializablePairLongLong(101L, 20L),
      new SerializablePairLongLong(102L, 30L),
      new SerializablePairLongLong((long) Integer.MAX_VALUE, 40L),
      new SerializablePairLongLong(Long.MAX_VALUE, 50L),
      };

  private final SegmentWriteOutMedium writeOutMedium = new OnHeapMemorySegmentWriteOutMedium();
  private SerializablePairLongLongBufferStore bufferStore;

  @Before
  public void setup() throws Exception
  {
    bufferStore = new SerializablePairLongLongBufferStore(
        new SerializedStorage<>(
            writeOutMedium.makeWriteOutBytes(),
            new SerializablePairLongLongSimpleStagedSerde()
        )
    );
  }

  @Test
  public void testIteratorSimple() throws Exception
  {
    for (SerializablePairLongLong value : integerRangeArr) {
      bufferStore.store(value);
    }

    IOIterator<SerializablePairLongLong> iterator = bufferStore.iterator();

    int i = 0;
    while (iterator.hasNext()) {
      Assert.assertEquals(integerRangeArr[i], iterator.next());
      i++;
    }
  }

  @Test
  public void testIteratorNull() throws Exception
  {
    bufferStore.store(null);
    IOIterator<SerializablePairLongLong> iterator = bufferStore.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertNull(iterator.next());
  }

  @Test
  public void testIteratorIdempotentHasNext() throws Exception
  {
    bufferStore.store(integerRangeArr[0]);

    IOIterator<SerializablePairLongLong> iterator = bufferStore.iterator();

    Assert.assertTrue(iterator.hasNext());
    // expect hasNext() to not modify state
    Assert.assertTrue(iterator.hasNext());
  }

  @Test(expected = NoSuchElementException.class)
  public void testIteratorEmptyThrows() throws Exception
  {
    IOIterator<SerializablePairLongLong> iterator = bufferStore.iterator();
    iterator.next();
  }

  @Test
  public void testIteratorEmptyHasNext() throws Exception
  {
    IOIterator<SerializablePairLongLong> iterator = bufferStore.iterator();
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testMinValueUsesInteger() throws Exception
  {
    for (SerializablePairLongLong value : integerRangeArr) {
      bufferStore.store(value);
    }

    SerializablePairLongLongColumnHeader columnHeader = (SerializablePairLongLongColumnHeader) bufferStore.createColumnHeader();
    Assert.assertEquals(integerRangeArr[0].lhs.longValue(), columnHeader.getMinValue());
    Assert.assertTrue(columnHeader.isUseIntegerDeltas());
  }

  @Test
  public void testMinValueUsesLong() throws Exception
  {
    for (SerializablePairLongLong value : longRangeArr) {
      bufferStore.store(value);
    }

    SerializablePairLongLongColumnHeader columnHeader = (SerializablePairLongLongColumnHeader) bufferStore.createColumnHeader();
    Assert.assertEquals(MIN_LONG, columnHeader.getMinValue());
    Assert.assertFalse(columnHeader.isUseIntegerDeltas());
  }

  @Test
  public void testMinValueUsesIntegerSerialization() throws Exception
  {
    for (SerializablePairLongLong value : integerRangeArr) {
      bufferStore.store(value);
    }

    SerializablePairLongLongColumnHeader columnHeader = (SerializablePairLongLongColumnHeader) bufferStore.createColumnHeader();

    HeapByteBufferWriteOutBytes channel = new HeapByteBufferWriteOutBytes();
    try (ResourceHolder<ByteBuffer> resourceHolder = NativeClearedByteBufferProvider.INSTANCE.get()) {
      columnHeader.transferTo(channel);

      ByteBuffer byteBuffer = resourceHolder.get();
      channel.writeTo(byteBuffer);
      byteBuffer.flip();

      SerializablePairLongLongColumnHeader deserializedColumnhHeader =
          (SerializablePairLongLongColumnHeader) AbstractSerializablePairLongObjectColumnHeader.fromBuffer(byteBuffer, SerializablePairLongLong.class);
      Assert.assertEquals(MIN_INTEGER, deserializedColumnhHeader.getMinValue());
      Assert.assertTrue(deserializedColumnhHeader.isUseIntegerDeltas());
    }
  }

  @Test
  public void testMinValueSerialization() throws Exception

  {
    for (SerializablePairLongLong value : longRangeArr) {
      bufferStore.store(value);
    }

    SerializablePairLongLongColumnHeader columnHeader = (SerializablePairLongLongColumnHeader) bufferStore.createColumnHeader();

    HeapByteBufferWriteOutBytes channel = new HeapByteBufferWriteOutBytes();
    try (ResourceHolder<ByteBuffer> resourceHolder = NativeClearedByteBufferProvider.INSTANCE.get()) {
      columnHeader.transferTo(channel);

      ByteBuffer byteBuffer = resourceHolder.get();

      channel.writeTo(byteBuffer);
      byteBuffer.flip();

      SerializablePairLongLongColumnHeader deserializedColumnhHeader =
          (SerializablePairLongLongColumnHeader) AbstractSerializablePairLongObjectColumnHeader.fromBuffer(byteBuffer, SerializablePairLongLong.class);
      Assert.assertEquals(MIN_LONG, deserializedColumnhHeader.getMinValue());
      Assert.assertFalse(deserializedColumnhHeader.isUseIntegerDeltas());
    }
  }

  @Test
  public void testLargeBuffer() throws Exception
  {
    SerializablePairLongLong value =
        new SerializablePairLongLong(Long.MAX_VALUE, Long.MAX_VALUE);

    bufferStore.store(value);

    IOIterator<SerializablePairLongLong> iterator = bufferStore.iterator();

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(value, iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testLargeValueCount() throws Exception
  {
    List<SerializablePairLongLong> valueList = new ArrayList<>();

    for (int i = 0; i < 10000; i++) {
      valueList.add(new SerializablePairLongLong(Integer.MAX_VALUE + (long) i, random.nextLong()));
    }

    assertBufferedValuesEqual(valueList);
  }

  @Test
  public void testOverflowTransfer() throws Exception
  {
    bufferStore.store(new SerializablePairLongLong(Long.MIN_VALUE, 10L));
    bufferStore.store(new SerializablePairLongLong(Long.MAX_VALUE, 10L));

    SerializablePairLongLongColumnHeader columnHeader = (SerializablePairLongLongColumnHeader) bufferStore.createColumnHeader();

    Assert.assertEquals(0, columnHeader.getMinValue());

    AbstractSerializablePairLongObjectBufferStore.TransferredBuffer transferredBuffer = bufferStore.transferToRowWriter(
        NativeClearedByteBufferProvider.INSTANCE,
        writeOutMedium
    );

    Assert.assertEquals(94, transferredBuffer.getSerializedSize());
  }

  @Test
  public void testNullOnlyTransfer() throws Exception
  {
    bufferStore.store(null);

    bufferStore.store(null);

    bufferStore.store(null);

    SerializablePairLongLongColumnHeader columnHeader = (SerializablePairLongLongColumnHeader) bufferStore.createColumnHeader();

    Assert.assertEquals(0, columnHeader.getMinValue());

    AbstractSerializablePairLongObjectBufferStore.TransferredBuffer transferredBuffer = bufferStore.transferToRowWriter(
        NativeClearedByteBufferProvider.INSTANCE,
        writeOutMedium
    );

    Assert.assertEquals(59, transferredBuffer.getSerializedSize());
  }

  @Test
  public void testTransferIntegerRange() throws Exception
  {
    for (SerializablePairLongLong value : integerRangeArr) {
      bufferStore.store(value);
    }

    Assert.assertTrue(bufferStore.createColumnHeader().isUseIntegerDeltas());

    assertTransferredValuesEqual(integerRangeArr);
  }

  @Test
  public void testTransferLongRange() throws Exception
  {
    for (SerializablePairLongLong value : longRangeArr) {
      bufferStore.store(value);
    }

    Assert.assertFalse(bufferStore.createColumnHeader().isUseIntegerDeltas());

    assertTransferredValuesEqual(longRangeArr);
  }

  private void assertBufferedValuesEqual(List<SerializablePairLongLong> input) throws IOException
  {
    for (SerializablePairLongLong pairLongLong : input) {
      bufferStore.store(pairLongLong);
    }

    IOIterator<SerializablePairLongLong> iterator = bufferStore.iterator();
    int i = 0;

    while (iterator.hasNext()) {
      Assert.assertEquals(input.get(i), iterator.next());
      i++;
    }

    Assert.assertEquals(
        StringUtils.format("element count mismatch: expected %s, got %s", input.size(), i),
        input.size(),
        i
    );
  }

  private void assertTransferredValuesEqual(SerializablePairLongLong[] input) throws IOException
  {
    AbstractSerializablePairLongObjectBufferStore.TransferredBuffer transferredBuffer =
        bufferStore.transferToRowWriter(NativeClearedByteBufferProvider.INSTANCE, writeOutMedium);
    HeapByteBufferWriteOutBytes resultChannel = new HeapByteBufferWriteOutBytes();

    transferredBuffer.writeTo(resultChannel, null);

    try (SerializablePairLongLongComplexColumn column = createComplexColumn(transferredBuffer, resultChannel)) {
      for (int i = 0; i < input.length; i++) {
        Assert.assertEquals(input[i], column.getRowValue(i));
      }
    }
  }

  private static SerializablePairLongLongComplexColumn createComplexColumn(
      AbstractSerializablePairLongObjectBufferStore.TransferredBuffer transferredBuffer,
      HeapByteBufferWriteOutBytes resultChannel
  )
  {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Ints.checkedCast(transferredBuffer.getSerializedSize()));

    resultChannel.readFully(0, byteBuffer);
    byteBuffer.flip();

    SerializablePairLongLongComplexMetricSerde complexMetricSerde = new SerializablePairLongLongComplexMetricSerde();
    ColumnBuilder builder = new ColumnBuilder();

    complexMetricSerde.deserializeColumn(byteBuffer, builder);
    builder.setType(ValueType.COMPLEX);

    ColumnHolder columnHolder = builder.build();
    SerializablePairLongLongComplexColumn column = (SerializablePairLongLongComplexColumn) columnHolder.getColumn();

    return column;
  }

}

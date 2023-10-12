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

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.writeout.HeapByteBufferWriteOutBytes;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

public class SerializablePairLongLongComplexMetricSerdeTest
{
  static {
    NullHandling.initializeForTests();
  }

  private static final SerializablePairLongLongComplexMetricSerde COMPRESSED_SERDE =
      new SerializablePairLongLongComplexMetricSerde();

  // want deterministic test input
  private final Random random = new Random(0);

  @Test
  public void testSingle() throws Exception
  {
    assertExpected(ImmutableList.of(new SerializablePairLongLong(100L, 10L)), 78);
  }

  @Test
  public void testLargeRHS() throws Exception
  {
    // single entry spans more than one block in underlying storage
    assertExpected(ImmutableList.of(new SerializablePairLongLong(
        100L,
        random.nextLong()
    )), 78);
  }

  @Test
  public void testCompressable() throws Exception
  {
    int numLongs = 10;
    List<SerializablePairLongLong> valueList = new ArrayList<>();
    List<Long> longList = new ArrayList<>();

    for (int i = 0; i < numLongs; i++) {
      longList.add(random.nextLong());
    }
    for (int i = 0; i < 10000; i++) {
      valueList.add(new SerializablePairLongLong(Integer.MAX_VALUE + (long) i, longList.get(i % numLongs)));
    }

    assertExpected(valueList, 80509);
  }

  @Test
  public void testHighlyCompressable() throws Exception
  {
    List<SerializablePairLongLong> valueList = new ArrayList<>();

    Long longValue = random.nextLong();
    for (int i = 0; i < 10000; i++) {
      valueList.add(new SerializablePairLongLong(Integer.MAX_VALUE + (long) i, longValue));
    }

    assertExpected(valueList, 80274);
  }

  @Test
  public void testRandom() throws Exception
  {
    List<SerializablePairLongLong> valueList = new ArrayList<>();

    for (int i = 0; i < 10000; i++) {
      valueList.add(new SerializablePairLongLong(random.nextLong(), random.nextLong()));
    }

    assertExpected(valueList, 210967);
  }

  @Test
  public void testNullRHS() throws Exception
  {
    assertExpected(ImmutableList.of(new SerializablePairLongLong(100L, null)), 71);
  }

  @Test
  public void testEmpty() throws Exception
  {
    // minimum size for empty data
    assertExpected(Collections.emptyList(), 57);
  }

  @Test
  public void testSingleNull() throws Exception
  {
    assertExpected(Arrays.asList(new SerializablePairLongLong[]{null}), 58);
  }

  @Test
  public void testMultipleNull() throws Exception
  {
    assertExpected(Arrays.asList(null, null, null, null), 59);
  }

  private ByteBuffer assertExpected(
      List<SerializablePairLongLong> expected,
      int expectedCompressedSize
  ) throws IOException
  {
    SegmentWriteOutMedium writeOutMedium = new OnHeapMemorySegmentWriteOutMedium();
    ByteBuffer compressedBuffer = serializeAllValuesToByteBuffer(
        expected,
        COMPRESSED_SERDE.getSerializer(writeOutMedium, "not-used"),
        expectedCompressedSize
    ).asReadOnlyBuffer();

    try (ComplexColumn compressedCol = createComplexColumn(compressedBuffer)
    ) {
      for (int i = 0; i < expected.size(); i++) {
        Assert.assertEquals(expected.get(i), compressedCol.getRowValue(i));
      }
    }
    return compressedBuffer;
  }

  private ComplexColumn createComplexColumn(ByteBuffer byteBuffer)
  {
    ColumnBuilder builder = new ColumnBuilder();
    int serializedSize = byteBuffer.remaining();

    COMPRESSED_SERDE.deserializeColumn(byteBuffer, builder);
    builder.setType(ValueType.COMPLEX);


    ColumnHolder columnHolder = builder.build();

    final ComplexColumn col = (ComplexColumn) columnHolder.getColumn();
    if (col instanceof SerializablePairLongLongComplexColumn) {
      Assert.assertEquals(serializedSize, col.getLength());
    }
    Assert.assertEquals("serializablePairLongLong", col.getTypeName());
    Assert.assertEquals(SerializablePairLongLong.class, col.getClazz());

    return col;
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  private static ByteBuffer serializeAllValuesToByteBuffer(
      List<SerializablePairLongLong> values,
      GenericColumnSerializer serializer,
      int expectedSize
  ) throws IOException
  {
    serializer.open();

    final AtomicReference<SerializablePairLongLong> reference = new AtomicReference<>(null);
    ColumnValueSelector<SerializablePairLongLong> valueSelector =
        new SingleObjectColumnValueSelector<SerializablePairLongLong>(
            SerializablePairLongLong.class
        )
        {
          @Nullable
          @Override
          public SerializablePairLongLong getObject()
          {
            return reference.get();
          }
        };

    for (SerializablePairLongLong selector : values) {
      reference.set(selector);
      serializer.serialize(valueSelector);
    }

    return serializeToByteBuffer(serializer, expectedSize);
  }

  private static ByteBuffer serializeToByteBuffer(
      GenericColumnSerializer<SerializablePairLongLong> serializer,
      int expectedSize
  ) throws IOException
  {
    HeapByteBufferWriteOutBytes channel = new HeapByteBufferWriteOutBytes();

    serializer.writeTo(channel, null);

    ByteBuffer byteBuffer = ByteBuffer.allocate((int) channel.size()).order(ByteOrder.nativeOrder());

    channel.readFully(0, byteBuffer);
    byteBuffer.flip();

    if (expectedSize > -1) {
      Assert.assertEquals(expectedSize, serializer.getSerializedSize());
    }

    Assert.assertEquals(serializer.getSerializedSize(), byteBuffer.limit());

    return byteBuffer;
  }
}

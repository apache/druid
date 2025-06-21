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
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.serde.cell.RandomStringUtils;
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

public class SerializablePairLongStringComplexMetricSerdeTest
{
  private static final SerializablePairLongStringComplexMetricSerde LEGACY_SERDE =
      new SerializablePairLongStringComplexMetricSerde();
  private static final SerializablePairLongStringComplexMetricSerde COMPRESSED_SERDE =
      new SerializablePairLongStringComplexMetricSerde(true);

  // want deterministic test input
  private final Random random = new Random(0);
  private final RandomStringUtils randomStringUtils = new RandomStringUtils(random);

  @Test
  public void testSingle() throws Exception
  {
    assertExpected(ImmutableList.of(new SerializablePairLongString(100L, "fuu")), 33, 77);
  }

  @Test
  public void testLargeString() throws Exception
  {
    // single entry spans more than one block in underlying storage
    assertExpected(ImmutableList.of(new SerializablePairLongString(
        100L,
        randomStringUtils.randomAlphanumeric(2 * 1024 * 1024)
    )), 2097182, 2103139);
  }

  @Test
  public void testCompressable() throws Exception
  {
    int numStrings = 10;
    List<SerializablePairLongString> valueList = new ArrayList<>();
    List<String> stringList = new ArrayList<>();

    for (int i = 0; i < numStrings; i++) {
      stringList.add(randomStringUtils.randomAlphanumeric(1024));
    }
    for (int i = 0; i < 10000; i++) {
      valueList.add(new SerializablePairLongString(Integer.MAX_VALUE + (long) i, stringList.get(i % numStrings)));
    }

    assertExpected(valueList, 10440010, 1746198);
  }

  @Test
  public void testHighlyCompressable() throws Exception
  {
    List<SerializablePairLongString> valueList = new ArrayList<>();

    String stringValue = randomStringUtils.randomAlphanumeric(1024);
    for (int i = 0; i < 10000; i++) {
      valueList.add(new SerializablePairLongString(Integer.MAX_VALUE + (long) i, stringValue));
    }

    assertExpected(valueList, 10440010, 289645);
  }

  @Test
  public void testRandom() throws Exception
  {
    List<SerializablePairLongString> valueList = new ArrayList<>();

    for (int i = 0; i < 10000; i++) {
      valueList.add(new SerializablePairLongString(random.nextLong(), randomStringUtils.randomAlphanumeric(1024)));
    }

    assertExpected(valueList, 10440010, 10429009);
  }

  @Test
  public void testNullString() throws Exception
  {
    assertExpected(ImmutableList.of(new SerializablePairLongString(100L, null)), 30, 74);
  }

  @Test
  public void testEmpty() throws Exception
  {
    // minimum size for empty data
    assertExpected(Collections.emptyList(), 10, 57);
  }

  @Test
  public void testSingleNull() throws Exception
  {
    assertExpected(Arrays.asList(new SerializablePairLongString[]{null}), 18, 58);
  }

  @Test
  public void testMultipleNull() throws Exception
  {
    assertExpected(Arrays.asList(null, null, null, null), 42, 59);
  }

  private ByteBuffer assertExpected(
      List<SerializablePairLongString> expected,
      int expectedLegacySize,
      int expectedCompressedSize
  ) throws IOException
  {
    SegmentWriteOutMedium writeOutMedium = new OnHeapMemorySegmentWriteOutMedium();
    ByteBuffer legacyBuffer = serializeAllValuesToByteBuffer(
        expected,
        LEGACY_SERDE.getSerializer(writeOutMedium, "not-used", IndexSpec.DEFAULT),
        expectedLegacySize
    ).asReadOnlyBuffer();
    ByteBuffer compressedBuffer = serializeAllValuesToByteBuffer(
        expected,
        COMPRESSED_SERDE.getSerializer(writeOutMedium, "not-used", IndexSpec.DEFAULT),
        expectedCompressedSize
    ).asReadOnlyBuffer();

    try (ComplexColumn legacyCol = createComplexColumn(legacyBuffer);
         ComplexColumn compressedCol = createComplexColumn(compressedBuffer)
    ) {
      for (int i = 0; i < expected.size(); i++) {
        Assert.assertEquals(expected.get(i), legacyCol.getRowValue(i));
        Assert.assertEquals(expected.get(i), compressedCol.getRowValue(i));
      }
    }
    return compressedBuffer;
  }

  private ComplexColumn createComplexColumn(ByteBuffer byteBuffer)
  {
    ColumnBuilder builder = new ColumnBuilder();
    int serializedSize = byteBuffer.remaining();

    LEGACY_SERDE.deserializeColumn(byteBuffer, builder);
    builder.setType(ValueType.COMPLEX);

    ColumnHolder columnHolder = builder.build();

    final ComplexColumn col = (ComplexColumn) columnHolder.getColumn();
    if (col instanceof SerializablePairLongStringComplexColumn) {
      Assert.assertEquals(serializedSize, col.getLength());
    }
    Assert.assertEquals("serializablePairLongString", col.getTypeName());
    Assert.assertEquals(SerializablePairLongString.class, col.getClazz());

    return col;
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  private static ByteBuffer serializeAllValuesToByteBuffer(
      List<SerializablePairLongString> values,
      GenericColumnSerializer serializer,
      int expectedSize
  ) throws IOException
  {
    serializer.open();

    final AtomicReference<SerializablePairLongString> reference = new AtomicReference<>(null);
    ColumnValueSelector<SerializablePairLongString> valueSelector =
        new SingleObjectColumnValueSelector<>(
            SerializablePairLongString.class
        )
        {
          @Nullable
          @Override
          public SerializablePairLongString getObject()
          {
            return reference.get();
          }
        };

    for (SerializablePairLongString selector : values) {
      reference.set(selector);
      serializer.serialize(valueSelector);
    }

    return serializeToByteBuffer(serializer, expectedSize);
  }

  private static ByteBuffer serializeToByteBuffer(
      GenericColumnSerializer<SerializablePairLongString> serializer,
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

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
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.serde.cell.RandomStringUtils;
import org.apache.druid.segment.writeout.HeapByteBufferWriteOutBytes;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class SerializablePairLongStringComplexMetricSerdeTest
{
  private static final SerializablePairLongStringComplexMetricSerde COMPLEX_METRIC_SERDE =
      new SerializablePairLongStringComplexMetricSerde();

  // want deterministic test input
  private final Random random = new Random(0);
  private final RandomStringUtils randomStringUtils = new RandomStringUtils(random);

  private GenericColumnSerializer<SerializablePairLongString> serializer;

  @SuppressWarnings("unchecked")
  @Before
  public void setup()
  {
    SegmentWriteOutMedium writeOutMedium = new OnHeapMemorySegmentWriteOutMedium();
    serializer = (GenericColumnSerializer<SerializablePairLongString>) COMPLEX_METRIC_SERDE.getSerializer(
        writeOutMedium,
        "not-used"
    );
  }

  @Test
  public void testSingle() throws Exception
  {
    assertExpected(ImmutableList.of(new SerializablePairLongString(100L, "fuu")), 77);
  }

  @Test
  public void testLargeString() throws Exception
  {
    // single entry spans more than one block in underlying storage
    assertExpected(ImmutableList.of(new SerializablePairLongString(
        100L,
        randomStringUtils.randomAlphanumeric(2 * 1024 * 1024)
    )), 2103140);
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

    //actual input bytes in naive encoding is ~10mb
    assertExpected(valueList, 1746026);
  }

  @Test
  public void testHighlyCompressable() throws Exception
  {
    List<SerializablePairLongString> valueList = new ArrayList<>();

    String stringValue = randomStringUtils.randomAlphanumeric(1024);
    for (int i = 0; i < 10000; i++) {
      valueList.add(new SerializablePairLongString(Integer.MAX_VALUE + (long) i, stringValue));
    }

    //actual input bytes in naive encoding is ~10mb
    assertExpected(valueList, 289645);
  }

  @Test
  public void testRandom() throws Exception
  {
    List<SerializablePairLongString> valueList = new ArrayList<>();

    for (int i = 0; i < 10000; i++) {
      valueList.add(new SerializablePairLongString(random.nextLong(), randomStringUtils.randomAlphanumeric(1024)));
    }

    assertExpected(valueList, 10428975);
  }

  @Test
  public void testNullString() throws Exception
  {
    assertExpected(ImmutableList.of(new SerializablePairLongString(100L, null)), 74);
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
    assertExpected(Arrays.asList(new SerializablePairLongString[]{null}), 58);
  }

  @Test
  public void testMultipleNull() throws Exception
  {
    assertExpected(Arrays.asList(null, null, null, null), 59);
  }

  private void assertExpected(List<SerializablePairLongString> expected) throws IOException
  {
    assertExpected(expected, -1);
  }

  private void assertExpected(List<SerializablePairLongString> expected, int expectedSize) throws IOException
  {
    List<SerializablePairLongStringValueSelector> valueSelectors =
        expected.stream().map(SerializablePairLongStringValueSelector::new).collect(Collectors.toList());
    ByteBuffer byteBuffer = serializeAllValuesToByteBuffer(valueSelectors, serializer, expectedSize);

    try (SerializablePairLongStringComplexColumn complexColumn = createComplexColumn(byteBuffer)) {
      for (int i = 0; i < valueSelectors.size(); i++) {
        Assert.assertEquals(expected.get(i), complexColumn.getRowValue(i));
      }
    }
  }

  private SerializablePairLongStringComplexColumn createComplexColumn(ByteBuffer byteBuffer)
  {
    ColumnBuilder builder = new ColumnBuilder();
    int serializedSize = byteBuffer.remaining();

    COMPLEX_METRIC_SERDE.deserializeColumn(byteBuffer, builder);
    builder.setType(ValueType.COMPLEX);

    ColumnHolder columnHolder = builder.build();

    SerializablePairLongStringComplexColumn column = (SerializablePairLongStringComplexColumn) columnHolder.getColumn();

    Assert.assertEquals(serializedSize, column.getLength());
    Assert.assertEquals("serializablePairLongString", column.getTypeName());
    Assert.assertEquals(SerializablePairLongString.class, column.getClazz());

    return column;
  }


  private static ByteBuffer serializeAllValuesToByteBuffer(
      Collection<SerializablePairLongStringValueSelector> valueSelectors,
      GenericColumnSerializer<SerializablePairLongString> serializer,
      int expectedSize
  ) throws IOException
  {
    serializer.open();

    for (SerializablePairLongStringValueSelector valueSelector : valueSelectors) {
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

  private static class SerializablePairLongStringValueSelector
      extends SingleValueColumnValueSelector<SerializablePairLongString>
  {
    public SerializablePairLongStringValueSelector(SerializablePairLongString value)
    {
      super(SerializablePairLongString.class, value);
    }
  }
}

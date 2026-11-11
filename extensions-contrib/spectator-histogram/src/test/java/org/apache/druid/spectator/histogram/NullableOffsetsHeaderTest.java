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

package org.apache.druid.spectator.histogram;

import com.google.common.collect.ImmutableList;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;

public class NullableOffsetsHeaderTest
{
  @Test
  public void testShouldAcceptNullWrites() throws IOException
  {
    NullableOffsetsHeader header = NullableOffsetsHeader.create(new OnHeapMemorySegmentWriteOutMedium());
    header.writeNull();
    header.writeNull();
    header.writeNull();

    Assertions.assertEquals(3, header.size(), "Size should be count of entries");

    header = serde(header);
    Assertions.assertEquals(3, header.size(), "Size should be count of entries");

    Assertions.assertNull(header.get(0), "Should return null for null entries by index");
    Assertions.assertNull(header.get(1), "Should return null for null entries by index");
    Assertions.assertNull(header.get(2), "Should return null for null entries by index");
  }

  @Test
  public void testShouldAcceptOffsetWrites() throws IOException
  {
    NullableOffsetsHeader header = NullableOffsetsHeader.create(new OnHeapMemorySegmentWriteOutMedium());
    header.writeOffset(123);
    header.writeOffset(456);

    Assertions.assertEquals(2, header.size(), "Size should be count of entries");

    header = serde(header);
    Assertions.assertEquals(2, header.size(), "Size should be count of entries");

    Assertions.assertNotNull(header.get(0), "Should flag nulls by index");
    Assertions.assertNotNull(header.get(1), "Should flag nulls by index");

    Assertions.assertEquals(0, header.get(0).getStart(), "Should return value for entries by index");
    Assertions.assertEquals(123, header.get(0).getEnd(), "Should return value for entries by index");
    Assertions.assertEquals(123, header.get(1).getStart(), "Should return value for entries by index");
    Assertions.assertEquals(456, header.get(1).getEnd(), "Should return value for entries by index");
  }

  @Test
  public void testShouldAcceptMixedWrites() throws IOException
  {
    NullableOffsetsHeader header = NullableOffsetsHeader.create(new OnHeapMemorySegmentWriteOutMedium());
    header.writeOffset(123);
    header.writeNull();
    header.writeNull();
    header.writeOffset(456);
    header.writeOffset(789);
    header.writeNull();

    Assertions.assertEquals(6, header.size(), "Size should be count of entries");

    header = serde(header);
    Assertions.assertEquals(6, header.size(), "Size should be count of entries");

    Assertions.assertNotNull(header.get(0), "Should flag nulls by index");
    Assertions.assertNull(header.get(1), "Should flag nulls by index");
    Assertions.assertNull(header.get(2), "Should flag nulls by index");
    Assertions.assertNotNull(header.get(3), "Should flag nulls by index");
    Assertions.assertNotNull(header.get(4), "Should flag nulls by index");
    Assertions.assertNull(header.get(5), "Should flag nulls by index");

    Assertions.assertEquals(0, header.get(0).getStart(), "Should return value for entries by index");
    Assertions.assertEquals(123, header.get(0).getEnd(), "Should return value for entries by index");
    Assertions.assertEquals(123, header.get(3).getStart(), "Should return value for entries by index");
    Assertions.assertEquals(456, header.get(3).getEnd(), "Should return value for entries by index");
    Assertions.assertEquals(456, header.get(4).getStart(), "Should return value for entries by index");
    Assertions.assertEquals(789, header.get(4).getEnd(), "Should return value for entries by index");
  }

  @Test
  public void testGiveAccessToOffsets() throws IOException
  {
    NullableOffsetsHeader header = NullableOffsetsHeader.create(new OnHeapMemorySegmentWriteOutMedium());
    header.writeOffset(123);
    header.writeNull();
    header.writeNull();
    header.writeOffset(456);
    header.writeOffset(789);
    header.writeNull();

    header = serde(header);

    Assertions.assertNull(header.get(6), "Should return null for 6");

    Assertions.assertNull(header.get(5), "Should return null for 5");

    Assertions.assertEquals(789, header.get(4).getEnd(), "Offset at 4");
    Assertions.assertEquals(456, header.get(4).getStart(), "Offset prior to 4");

    Assertions.assertEquals(456, header.get(3).getEnd(), "Offset at 3");
    Assertions.assertEquals(123, header.get(3).getStart(), "Offset prior to 3");

    Assertions.assertNull(header.get(2), "Should return null for 2");

    Assertions.assertNull(header.get(1), "Should return null for 1");

    Assertions.assertEquals(123, header.get(0).getEnd(), "Offset at 0");
    Assertions.assertEquals(0, header.get(0).getStart(), "Offset prior to 0");
  }

  @Test
  public void testGiveAccessToSingleOffsetNulls() throws IOException
  {
    NullableOffsetsHeader header = NullableOffsetsHeader.create(new OnHeapMemorySegmentWriteOutMedium());
    header.writeNull();
    header.writeOffset(123);
    header.writeNull();
    header.writeNull();
    header.writeNull();

    header = serde(header);

    Assertions.assertEquals(123, header.get(1).getEnd(), "Offset at 1");
    Assertions.assertEquals(0, header.get(1).getStart(), "Offset prior to 1");

    Assertions.assertNull(header.get(0), "Nulls for anything not set");
    Assertions.assertNull(header.get(-1), "Nulls for anything not set");
    Assertions.assertNull(header.get(3), "Nulls for anything not set");
    Assertions.assertNull(header.get(100), "Nulls for anything not set");
  }

  @Test
  public void testShouldSerializeAndDeserialize() throws IOException
  {
    NullableOffsetsHeader header = NullableOffsetsHeader.create(new OnHeapMemorySegmentWriteOutMedium());
    header.writeOffset(123);
    header.writeNull();
    header.writeNull();
    header.writeOffset(456);
    header.writeOffset(789);
    header.writeNull();

    // Length + BitmapLength + Bitmap + Offsets
    //      4 +            4 +      1 +      12 = 21 bytes
    Assertions.assertEquals(21, header.getSerializedSize(), "Serialized size should be minimal");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    header.writeTo(channel, null);
    channel.close();

    final ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
    Assertions.assertEquals(
        header.getSerializedSize(),
        byteBuffer.remaining(),
        "Reported size and actual size should match"
    );

    NullableOffsetsHeader deserialized = NullableOffsetsHeader.read(byteBuffer);
    Assertions.assertEquals(0, byteBuffer.remaining());

    Assertions.assertEquals(header.size(), deserialized.size(), "Deserialized should match pre-serialized size");

    // Nulls should return the previous offset
    List<NullableOffsetsHeader.Offset> expected = Arrays.asList(
        new NullableOffsetsHeader.Offset(0, 123),
        null,
        null,
        new NullableOffsetsHeader.Offset(123, 456),
        new NullableOffsetsHeader.Offset(456, 789),
        null
    );

    for (int i = 0; i < header.size(); i++) {
      Assertions.assertEquals(expected.get(i), deserialized.get(i), "Deserialized should match pre-serialized values");
    }
  }

  @Test
  public void testShouldSerializeAndDeserializeAllNulls() throws IOException
  {
    NullableOffsetsHeader header = NullableOffsetsHeader.create(new OnHeapMemorySegmentWriteOutMedium());
    for (int i = 0; i < 10000; i++) {
      header.writeNull();
    }

    // Length + BitmapLength + Bitmap + Offsets
    //      4 +            4 +      0 +       0 = 8 bytes
    Assertions.assertEquals(8, header.getSerializedSize(), "Serialized size should be minimal");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    header.writeTo(channel, null);
    channel.close();

    final ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
    Assertions.assertEquals(
        header.getSerializedSize(),
        byteBuffer.remaining(),
        "Reported size and actual size should match"
    );

    NullableOffsetsHeader deserialized = NullableOffsetsHeader.read(byteBuffer);
    Assertions.assertEquals(0, byteBuffer.remaining());

    Assertions.assertEquals(header.size(), deserialized.size(), "Deserialized should match pre-serialized size");

    for (int i = 0; i < header.size(); i++) {
      Assertions.assertNull(deserialized.get(i), "Deserialized should be null");
    }
  }

  @Test
  public void testShouldSerializeAndDeserializeAllValues() throws IOException
  {
    NullableOffsetsHeader header = NullableOffsetsHeader.create(new OnHeapMemorySegmentWriteOutMedium());
    for (int i = 0; i < 10000; i++) {
      header.writeOffset(i + 1);
    }

    // Length + BitmapLength + Bitmap + Offsets
    //      4 +            4 +      0 +   40000 = 40008 bytes
    // Bitmap is omitted if all values are set
    Assertions.assertEquals(40008, header.getSerializedSize(), "Serialized size should be minimal");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    header.writeTo(channel, null);
    channel.close();

    final ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
    Assertions.assertEquals(
        header.getSerializedSize(),
        byteBuffer.remaining(),
        "Reported size and actual size should match"
    );

    NullableOffsetsHeader deserialized = NullableOffsetsHeader.read(byteBuffer);
    Assertions.assertEquals(0, byteBuffer.remaining());

    Assertions.assertEquals(header.size(), deserialized.size(), "Deserialized should match pre-serialized size");

    for (int i = 0; i < header.size(); i++) {
      Assertions.assertNotNull(deserialized.get(i), "Deserialized should be set " + i);
      Assertions.assertEquals(i + 1, deserialized.get(i).getEnd(), "Deserialized should match pre-serialized nulls " + i);
    }
  }

  @Test
  public void testShouldFindOffsetFromIndexSingleWord() throws IOException
  {
    // Should return the exact index of the offset to read, or negative if not present
    List<Integer> expectedOffsetIndexes = ImmutableList.of(15, 21, 30, 31);
    NullableOffsetsHeader header = createHeaderWithIndexesSet(expectedOffsetIndexes);
    Assertions.assertEquals(32, header.size(), "Size should be count of entries");
    header = serde(header);

    for (int i = 0; i < header.size(); i++) {
      int offsetIndex = header.getOffsetIndex(i);
      int expected = expectedOffsetIndexes.indexOf(i);
      Assertions.assertEquals(expected, offsetIndex, "Offset " + i);
    }
  }

  @Test
  public void testShouldFindOffsetFromIndexMultipleWords() throws IOException
  {
    // Should return the exact index of the offset to read, or negative if not present
    List<Integer> expectedOffsetIndexes = ImmutableList.of(15, 21, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 70, 100);
    NullableOffsetsHeader header = createHeaderWithIndexesSet(expectedOffsetIndexes);
    Assertions.assertEquals(101, header.size(), "Size should be count of entries");
    header = serde(header);

    for (int i = 0; i < header.size(); i++) {
      int offsetIndex = header.getOffsetIndex(i);
      int expected = expectedOffsetIndexes.indexOf(i);
      Assertions.assertEquals(expected, offsetIndex, "Offset " + i);
    }
  }

  @Test
  public void testShouldFindOffsetFromIndexFull() throws IOException
  {
    // For a full header, the bitset is omitted.
    // The expected index, is the queried index.
    final int size = 500;
    NullableOffsetsHeader header = NullableOffsetsHeader.create(new OnHeapMemorySegmentWriteOutMedium());
    for (int i = 0; i < size; i++) {
      header.writeOffset(i + 1);
    }
    Assertions.assertEquals(size, header.size(), "Size should be count of entries");
    header = serde(header);

    for (int i = 0; i < size; i++) {
      int offsetIndex = header.getOffsetIndex(i);
      Assertions.assertEquals(i, offsetIndex, "Offset " + i);
    }
  }

  @Test
  public void testShouldFindOffsetFromIndexEmpty() throws IOException
  {
    // For an empty header, the bitset is omitted.
    // The expected index, is always -1.
    final int size = 500;
    NullableOffsetsHeader header = NullableOffsetsHeader.create(new OnHeapMemorySegmentWriteOutMedium());
    for (int i = 0; i < size; i++) {
      header.writeNull();
    }
    Assertions.assertEquals(size, header.size(), "Size should be count of entries");
    header = serde(header);

    for (int i = 0; i < size; i++) {
      int offsetIndex = header.getOffsetIndex(i);
      Assertions.assertEquals(-1, offsetIndex, "Offset " + i);
    }
  }

  @Test
  public void testShouldWorkWithBitsSetAfter64bitBoundary() throws IOException
  {
    List<Integer> expectedOffsetIndexes = ImmutableList.of(0, 1, 2, 3, 4, 256, 257);
    NullableOffsetsHeader header = createHeaderWithIndexesSet(expectedOffsetIndexes);
    Assertions.assertEquals(258, header.size(), "Size should be count of entries");
    header = serde(header);
    Assertions.assertEquals(258, header.size(), "Size should be count of entries");
    Assertions.assertEquals(expectedOffsetIndexes.size(), header.getCardinality(), "Cardinality should be count of non-nulls");

    for (int i = 0; i < header.size(); i++) {
      int offsetIndex = header.getOffsetIndex(i);
      int expectedOffset = expectedOffsetIndexes.indexOf(i);
      Assertions.assertEquals(expectedOffset, offsetIndex, "Offset " + i);

      NullableOffsetsHeader.Offset offset = header.get(i);
      if (expectedOffset < 0) {
        Assertions.assertNull(offset, "Null Offset " + i);
      } else {
        int expectedOffsetStart = expectedOffset;
        int expectedOffsetEnd = expectedOffset + 1;
        Assertions.assertEquals(expectedOffsetStart, offset.getStart(), "Offset Start " + i);
        Assertions.assertEquals(expectedOffsetEnd, offset.getEnd(), "Offset End " + i);
        Assertions.assertEquals(1, offset.getLength(), "Offset Length " + i);
      }
    }
  }

  @Test
  public void testShouldWorkOnLongByteBoundaries() throws IOException
  {
    for (int x = 1; x < 24; x++) {
      int boundary = ((int) Math.pow(2, x)) - 1;
      List<Integer> expectedOffsetIndexes = ImmutableList.of(boundary - 1);
      NullableOffsetsHeader header = createHeaderWithIndexesSet(expectedOffsetIndexes);
      Assertions.assertEquals(boundary, header.size(), "Size should be count of entries");
      header = serde(header);
      Assertions.assertEquals(boundary, header.size(), "Size should be count of entries");
      Assertions.assertEquals(
          expectedOffsetIndexes.size(),
          header.getCardinality(),
          "Cardinality should be count of non-nulls"
      );

      for (int i = 0; i < header.size(); i++) {
        int offsetIndex = header.getOffsetIndex(i);
        int expectedOffset = expectedOffsetIndexes.indexOf(i);
        Assertions.assertEquals(expectedOffset, offsetIndex, "Offset " + i);

        NullableOffsetsHeader.Offset offset = header.get(i);
        if (expectedOffset < 0) {
          Assertions.assertNull(offset, "Null Offset " + i);
        } else {
          int expectedOffsetStart = expectedOffset;
          int expectedOffsetEnd = expectedOffset + 1;
          Assertions.assertEquals(expectedOffsetStart, offset.getStart(), "Offset Start " + i);
          Assertions.assertEquals(expectedOffsetEnd, offset.getEnd(), "Offset End " + i);
          Assertions.assertEquals(1, offset.getLength(), "Offset Length " + i);
        }
      }
    }
  }

  /**
   * Test helper to serialize and deserialize a NullableOffsetsHeader
   *
   * @param in The NullableOffsetsHeader to serialize
   * @return The deserialized representation of in.
   */
  NullableOffsetsHeader serde(NullableOffsetsHeader in) throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    in.writeTo(channel, null);
    channel.close();

    final ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
    return NullableOffsetsHeader.read(byteBuffer);
  }

  /**
   * Helper to make a header with the provided indexes set
   */
  NullableOffsetsHeader createHeaderWithIndexesSet(List<Integer> indexes) throws IOException
  {
    NullableOffsetsHeader header = NullableOffsetsHeader.create(new OnHeapMemorySegmentWriteOutMedium());
    int offset = 1;
    for (Integer idx : indexes) {
      while (header.size() < idx) {
        header.writeNull();
      }
      header.writeOffset(offset++);
    }
    return header;
  }
}

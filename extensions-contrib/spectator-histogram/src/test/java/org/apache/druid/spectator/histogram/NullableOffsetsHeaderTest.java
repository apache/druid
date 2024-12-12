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
import org.junit.Assert;
import org.junit.Test;

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

    Assert.assertEquals("Size should be count of entries", 3, header.size());

    header = serde(header);
    Assert.assertEquals("Size should be count of entries", 3, header.size());

    Assert.assertNull("Should return null for null entries by index", header.get(0));
    Assert.assertNull("Should return null for null entries by index", header.get(1));
    Assert.assertNull("Should return null for null entries by index", header.get(2));
  }

  @Test
  public void testShouldAcceptOffsetWrites() throws IOException
  {
    NullableOffsetsHeader header = NullableOffsetsHeader.create(new OnHeapMemorySegmentWriteOutMedium());
    header.writeOffset(123);
    header.writeOffset(456);

    Assert.assertEquals("Size should be count of entries", 2, header.size());

    header = serde(header);
    Assert.assertEquals("Size should be count of entries", 2, header.size());

    Assert.assertNotNull("Should flag nulls by index", header.get(0));
    Assert.assertNotNull("Should flag nulls by index", header.get(1));

    Assert.assertEquals("Should return value for entries by index", 0, header.get(0).getStart());
    Assert.assertEquals("Should return value for entries by index", 123, header.get(0).getEnd());
    Assert.assertEquals("Should return value for entries by index", 123, header.get(1).getStart());
    Assert.assertEquals("Should return value for entries by index", 456, header.get(1).getEnd());
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

    Assert.assertEquals("Size should be count of entries", 6, header.size());

    header = serde(header);
    Assert.assertEquals("Size should be count of entries", 6, header.size());

    Assert.assertNotNull("Should flag nulls by index", header.get(0));
    Assert.assertNull("Should flag nulls by index", header.get(1));
    Assert.assertNull("Should flag nulls by index", header.get(2));
    Assert.assertNotNull("Should flag nulls by index", header.get(3));
    Assert.assertNotNull("Should flag nulls by index", header.get(4));
    Assert.assertNull("Should flag nulls by index", header.get(5));

    Assert.assertEquals("Should return value for entries by index", 0, header.get(0).getStart());
    Assert.assertEquals("Should return value for entries by index", 123, header.get(0).getEnd());
    Assert.assertEquals("Should return value for entries by index", 123, header.get(3).getStart());
    Assert.assertEquals("Should return value for entries by index", 456, header.get(3).getEnd());
    Assert.assertEquals("Should return value for entries by index", 456, header.get(4).getStart());
    Assert.assertEquals("Should return value for entries by index", 789, header.get(4).getEnd());
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

    Assert.assertNull("Should return null for 6", header.get(6));

    Assert.assertNull("Should return null for 5", header.get(5));

    Assert.assertEquals("Offset at 4", 789, header.get(4).getEnd());
    Assert.assertEquals("Offset prior to 4", 456, header.get(4).getStart());

    Assert.assertEquals("Offset at 3", 456, header.get(3).getEnd());
    Assert.assertEquals("Offset prior to 3", 123, header.get(3).getStart());

    Assert.assertNull("Should return null for 2", header.get(2));

    Assert.assertNull("Should return null for 1", header.get(1));

    Assert.assertEquals("Offset at 0", 123, header.get(0).getEnd());
    Assert.assertEquals("Offset prior to 0", 0, header.get(0).getStart());
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

    Assert.assertEquals("Offset at 1", 123, header.get(1).getEnd());
    Assert.assertEquals("Offset prior to 1", 0, header.get(1).getStart());

    Assert.assertNull("Nulls for anything not set", header.get(0));
    Assert.assertNull("Nulls for anything not set", header.get(-1));
    Assert.assertNull("Nulls for anything not set", header.get(3));
    Assert.assertNull("Nulls for anything not set", header.get(100));
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
    Assert.assertEquals("Serialized size should be minimal", 21, header.getSerializedSize());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    header.writeTo(channel, null);
    channel.close();

    final ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
    Assert.assertEquals(
        "Reported size and actual size should match",
        header.getSerializedSize(),
        byteBuffer.remaining()
    );

    NullableOffsetsHeader deserialized = NullableOffsetsHeader.read(byteBuffer);
    Assert.assertEquals(0, byteBuffer.remaining());

    Assert.assertEquals("Deserialized should match pre-serialized size", header.size(), deserialized.size());

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
      Assert.assertEquals("Deserialized should match pre-serialized values", expected.get(i), deserialized.get(i));
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
    Assert.assertEquals("Serialized size should be minimal", 8, header.getSerializedSize());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    header.writeTo(channel, null);
    channel.close();

    final ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
    Assert.assertEquals(
        "Reported size and actual size should match",
        header.getSerializedSize(),
        byteBuffer.remaining()
    );

    NullableOffsetsHeader deserialized = NullableOffsetsHeader.read(byteBuffer);
    Assert.assertEquals(0, byteBuffer.remaining());

    Assert.assertEquals("Deserialized should match pre-serialized size", header.size(), deserialized.size());

    for (int i = 0; i < header.size(); i++) {
      Assert.assertNull("Deserialized should be null", deserialized.get(i));
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
    Assert.assertEquals("Serialized size should be minimal", 40008, header.getSerializedSize());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    header.writeTo(channel, null);
    channel.close();

    final ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
    Assert.assertEquals(
        "Reported size and actual size should match",
        header.getSerializedSize(),
        byteBuffer.remaining()
    );

    NullableOffsetsHeader deserialized = NullableOffsetsHeader.read(byteBuffer);
    Assert.assertEquals(0, byteBuffer.remaining());

    Assert.assertEquals("Deserialized should match pre-serialized size", header.size(), deserialized.size());

    for (int i = 0; i < header.size(); i++) {
      Assert.assertNotNull("Deserialized should be set " + i, deserialized.get(i));
      Assert.assertEquals("Deserialized should match pre-serialized nulls " + i, i + 1, deserialized.get(i).getEnd());
    }
  }

  @Test
  public void testShouldFindOffsetFromIndexSingleWord() throws IOException
  {
    // Should return the exact index of the offset to read, or negative if not present
    List<Integer> expectedOffsetIndexes = ImmutableList.of(15, 21, 30, 31);
    NullableOffsetsHeader header = createHeaderWithIndexesSet(expectedOffsetIndexes);
    Assert.assertEquals("Size should be count of entries", 32, header.size());
    header = serde(header);

    for (int i = 0; i < header.size(); i++) {
      int offsetIndex = header.getOffsetIndex(i);
      int expected = expectedOffsetIndexes.indexOf(i);
      Assert.assertEquals("Offset " + i, expected, offsetIndex);
    }
  }

  @Test
  public void testShouldFindOffsetFromIndexMultipleWords() throws IOException
  {
    // Should return the exact index of the offset to read, or negative if not present
    List<Integer> expectedOffsetIndexes = ImmutableList.of(15, 21, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 70, 100);
    NullableOffsetsHeader header = createHeaderWithIndexesSet(expectedOffsetIndexes);
    Assert.assertEquals("Size should be count of entries", 101, header.size());
    header = serde(header);

    for (int i = 0; i < header.size(); i++) {
      int offsetIndex = header.getOffsetIndex(i);
      int expected = expectedOffsetIndexes.indexOf(i);
      Assert.assertEquals("Offset " + i, expected, offsetIndex);
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
    Assert.assertEquals("Size should be count of entries", size, header.size());
    header = serde(header);

    for (int i = 0; i < size; i++) {
      int offsetIndex = header.getOffsetIndex(i);
      Assert.assertEquals("Offset " + i, i, offsetIndex);
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
    Assert.assertEquals("Size should be count of entries", size, header.size());
    header = serde(header);

    for (int i = 0; i < size; i++) {
      int offsetIndex = header.getOffsetIndex(i);
      Assert.assertEquals("Offset " + i, -1, offsetIndex);
    }
  }

  @Test
  public void testShouldWorkWithBitsSetAfter64bitBoundary() throws IOException
  {
    List<Integer> expectedOffsetIndexes = ImmutableList.of(0, 1, 2, 3, 4, 256, 257);
    NullableOffsetsHeader header = createHeaderWithIndexesSet(expectedOffsetIndexes);
    Assert.assertEquals("Size should be count of entries", 258, header.size());
    header = serde(header);
    Assert.assertEquals("Size should be count of entries", 258, header.size());
    Assert.assertEquals("Cardinality should be count of non-nulls", expectedOffsetIndexes.size(), header.getCardinality());

    for (int i = 0; i < header.size(); i++) {
      int offsetIndex = header.getOffsetIndex(i);
      int expectedOffset = expectedOffsetIndexes.indexOf(i);
      Assert.assertEquals("Offset " + i, expectedOffset, offsetIndex);

      NullableOffsetsHeader.Offset offset = header.get(i);
      if (expectedOffset < 0) {
        Assert.assertNull("Null Offset " + i, offset);
      } else {
        int expectedOffsetStart = expectedOffset;
        int expectedOffsetEnd = expectedOffset + 1;
        Assert.assertEquals("Offset Start " + i, expectedOffsetStart, offset.getStart());
        Assert.assertEquals("Offset End " + i, expectedOffsetEnd, offset.getEnd());
        Assert.assertEquals("Offset Length " + i, 1, offset.getLength());
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
      Assert.assertEquals("Size should be count of entries", boundary, header.size());
      header = serde(header);
      Assert.assertEquals("Size should be count of entries", boundary, header.size());
      Assert.assertEquals(
          "Cardinality should be count of non-nulls",
          expectedOffsetIndexes.size(),
          header.getCardinality()
      );

      for (int i = 0; i < header.size(); i++) {
        int offsetIndex = header.getOffsetIndex(i);
        int expectedOffset = expectedOffsetIndexes.indexOf(i);
        Assert.assertEquals("Offset " + i, expectedOffset, offsetIndex);

        NullableOffsetsHeader.Offset offset = header.get(i);
        if (expectedOffset < 0) {
          Assert.assertNull("Null Offset " + i, offset);
        } else {
          int expectedOffsetStart = expectedOffset;
          int expectedOffsetEnd = expectedOffset + 1;
          Assert.assertEquals("Offset Start " + i, expectedOffsetStart, offset.getStart());
          Assert.assertEquals("Offset End " + i, expectedOffsetEnd, offset.getEnd());
          Assert.assertEquals("Offset Length " + i, 1, offset.getLength());
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

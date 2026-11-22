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

import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.file.SegmentFileChannel;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class ComplexMetricCompressionLevelTest
{
  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();

  @Test
  public void testLz4FastRoundTrip() throws Exception
  {
    assertBlobRoundTrip(CompressionStrategy.LZ4, 0);
  }

  @Test
  public void testLz4HighRoundTrip() throws Exception
  {
    assertBlobRoundTrip(CompressionStrategy.LZ4, 17);
  }

  @Test
  public void testZstdDefaultLevelRoundTrip() throws Exception
  {
    assertBlobRoundTrip(CompressionStrategy.ZSTD, 0);
  }

  @Test
  public void testZstdLevelNineRoundTrip() throws Exception
  {
    assertBlobRoundTrip(CompressionStrategy.ZSTD, 9);
  }

  @Test
  public void testUnspecifiedLevelPreservesReaderCompatibility() throws Exception
  {
    assertBlobRoundTrip(CompressionStrategy.LZ4, null);
  }

  private void assertBlobRoundTrip(CompressionStrategy compression, Integer level) throws Exception
  {
    final File segmentDir = temporaryFolder.newFolder();
    final File scratchDir = temporaryFolder.newFolder();
    final String filename = "complex_metric";
    final List<byte[]> expected = new ArrayList<>();

    try (SegmentWriteOutMedium medium =
             TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(scratchDir);
         FileSmoosher smoosher = new FileSmoosher(segmentDir)) {
      final CompressedVariableSizedBlobColumnSerializer serializer =
          new CompressedVariableSizedBlobColumnSerializer(filename, medium, compression, level);
      serializer.open();
      for (int row = 0; row < 96; row++) {
        final int length = row % 13 == 0 ? 0 : 1500 + (row * 91) % 4600;
        final byte[] value = new byte[length];
        for (int j = 0; j < length; j++) {
          value[j] = (byte) ((j + row * 3) % 41);
        }
        expected.add(value);
        serializer.addValue(value);
      }
      try (SegmentFileChannel channel = smoosher.addWithChannel(filename, serializer.getSerializedSize())) {
        serializer.writeTo(channel, smoosher);
      }
    }

    try (SmooshedFileMapper mapper = SmooshedFileMapper.load(segmentDir)) {
      final CompressedVariableSizedBlobColumnSupplier supplier =
          CompressedVariableSizedBlobColumnSupplier.fromByteBuffer(
              filename,
              mapper.mapFile(filename),
              ByteOrder.nativeOrder(),
              ByteOrder.nativeOrder(),
              mapper
          );
      try (CompressedVariableSizedBlobColumn column = supplier.get()) {
        Assertions.assertEquals(expected.size(), column.size());
        for (int row = 0; row < expected.size(); row++) {
          final ByteBuffer value = column.get(row);
          final byte[] actual = new byte[value.remaining()];
          value.get(actual);
          Assertions.assertArrayEquals(expected.get(row), actual, "row " + row);
        }
      }
    }
  }
}

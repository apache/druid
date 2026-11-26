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

import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.apache.druid.utils.CloseableUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.ByteBuffer;

public class DimensionCompressionLevelTest
{
  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();

  @Test
  public void testSingleValueDimensionRoundTrip() throws Exception
  {
    assertSingleValueRoundTrip(CompressionStrategy.LZ4, 0);
    assertSingleValueRoundTrip(CompressionStrategy.LZ4, 9);
    assertSingleValueRoundTrip(CompressionStrategy.ZSTD, 3);
  }

  @Test
  public void testMultiValueDimensionRoundTrip() throws Exception
  {
    assertMultiValueRoundTrip(CompressionStrategy.LZ4, 0);
    assertMultiValueRoundTrip(CompressionStrategy.ZSTD, 3);
  }

  private void assertSingleValueRoundTrip(CompressionStrategy compression, int level) throws Exception
  {
    try (OnHeapMemorySegmentWriteOutMedium medium = new OnHeapMemorySegmentWriteOutMedium();
         FileSmoosher smoosher = new FileSmoosher(temporaryFolder.newFolder())) {
      final CompressedVSizeColumnarIntsSerializer writer = CompressedVSizeColumnarIntsSerializer.create(
          "test",
          medium,
          "single",
          255,
          compression,
          level,
          medium.getCloser()
      );
      writer.open();
      for (int i = 0; i < 10_000; i++) {
        writer.addValue(i % 101);
      }
      final WriteOutBytes out = medium.makeWriteOutBytes();
      writer.writeTo(out, smoosher);
      // The encoded column is fully written to the in-memory channel above.

      final CompressedVSizeColumnarIntsSupplier supplier = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
          ByteBuffer.wrap(IOUtils.toByteArray(out.asInputStream())),
          IndexIO.BYTE_ORDER,
          null
      );
      final ColumnarInts column = supplier.get();
      try {
        for (int i = 0; i < 10_000; i++) {
          Assertions.assertEquals(i % 101, column.get(i));
        }
      }
      finally {
        CloseableUtils.closeAndWrapExceptions(column);
      }
    }
  }

  private void assertMultiValueRoundTrip(CompressionStrategy compression, int level) throws Exception
  {
    try (OnHeapMemorySegmentWriteOutMedium medium = new OnHeapMemorySegmentWriteOutMedium();
         FileSmoosher smoosher = new FileSmoosher(temporaryFolder.newFolder())) {
      final V3CompressedVSizeColumnarMultiIntsSerializer writer = V3CompressedVSizeColumnarMultiIntsSerializer.create(
          "test",
          medium,
          "multi",
          255,
          compression,
          level,
          GenericIndexedWriter.MAX_FILE_SIZE
      );
      writer.open();
      for (int i = 0; i < 1_000; i++) {
        writer.addValues(new ArrayBasedIndexedInts(new int[]{i % 101, (i + 1) % 101}));
      }
      final WriteOutBytes out = medium.makeWriteOutBytes();
      writer.writeTo(out, smoosher);
      // Read the completed in-memory column without relying on external smoosh files.

      final V3CompressedVSizeColumnarMultiIntsSupplier supplier =
          V3CompressedVSizeColumnarMultiIntsSupplier.fromByteBuffer(
              ByteBuffer.wrap(IOUtils.toByteArray(out.asInputStream())),
              IndexIO.BYTE_ORDER,
              null
          );
      try (ColumnarMultiInts column = supplier.get()) {
        Assertions.assertEquals(1_000, column.size());
        for (int i = 0; i < 1_000; i++) {
          final IndexedInts values = column.get(i);
          Assertions.assertEquals(2, values.size());
          Assertions.assertEquals(i % 101, values.get(0));
          Assertions.assertEquals((i + 1) % 101, values.get(1));
        }
      }
    }
  }
}

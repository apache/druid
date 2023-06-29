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
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class CompressedVariableSizeBlobColumnTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testSomeValues() throws IOException
  {
    // value sizes increase until they span at least 3 pages of compressed buffers
    final File tmpFile = tempFolder.newFolder();
    final FileSmoosher smoosher = new FileSmoosher(tmpFile);

    final File tmpFile2 = tempFolder.newFolder();
    final SegmentWriteOutMedium writeOutMedium =
        TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(tmpFile2);

    final String fileNameBase = "test";

    final CompressionStrategy compressionStrategy = CompressionStrategy.LZ4;
    CompressedVariableSizedBlobColumnSerializer serializer = new CompressedVariableSizedBlobColumnSerializer(
        fileNameBase,
        writeOutMedium,
        compressionStrategy
    );
    serializer.open();

    int numWritten = 0;
    final Random r = ThreadLocalRandom.current();
    final List<byte[]> values = new ArrayList<>();
    for (int i = 0, offset = 0; offset < CompressedPools.BUFFER_SIZE * 4; i++, offset = 1 << i) {
      byte[] value = new byte[offset];
      r.nextBytes(value);
      values.add(value);
      serializer.addValue(value);
      numWritten++;
    }

    SmooshedWriter writer = smoosher.addWithSmooshedWriter(fileNameBase, serializer.getSerializedSize());
    serializer.writeTo(writer, smoosher);
    writer.close();
    smoosher.close();
    SmooshedFileMapper fileMapper = SmooshedFileMapper.load(tmpFile);

    ByteBuffer base = fileMapper.mapFile(fileNameBase);

    CompressedVariableSizedBlobColumn column = CompressedVariableSizedBlobColumnSupplier.fromByteBuffer(
        fileNameBase,
        base,
        ByteOrder.nativeOrder(),
        fileMapper
    ).get();
    for (int row = 0; row < numWritten; row++) {
      ByteBuffer value = column.get(row);
      byte[] bytes = new byte[value.limit()];
      value.get(bytes);
      Assert.assertArrayEquals("Row " + row, values.get(row), bytes);
    }
    for (int rando = 0; rando < numWritten; rando++) {
      int row = ThreadLocalRandom.current().nextInt(0, numWritten - 1);
      ByteBuffer value = column.get(row);
      byte[] bytes = new byte[value.remaining()];
      value.get(bytes);
      Assert.assertArrayEquals("Row " + row, values.get(row), bytes);
    }
    column.close();
    fileMapper.close();
  }

  @Test
  public void testSomeValuesByteBuffers() throws IOException
  {
    // value sizes increase until they span at least 3 pages of compressed buffers
    final File tmpFile = tempFolder.newFolder();
    final FileSmoosher smoosher = new FileSmoosher(tmpFile);

    final File tmpFile2 = tempFolder.newFolder();
    final SegmentWriteOutMedium writeOutMedium =
        TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(tmpFile2);

    final String fileNameBase = "test";

    final CompressionStrategy compressionStrategy = CompressionStrategy.LZ4;
    CompressedVariableSizedBlobColumnSerializer serializer = new CompressedVariableSizedBlobColumnSerializer(
        fileNameBase,
        writeOutMedium,
        compressionStrategy
    );
    serializer.open();

    int numWritten = 0;
    final Random r = ThreadLocalRandom.current();
    final List<byte[]> values = new ArrayList<>();
    for (int i = 0, offset = 0; offset < CompressedPools.BUFFER_SIZE * 4; i++, offset = 1 << i) {
      byte[] value = new byte[offset];
      r.nextBytes(value);
      values.add(value);
      serializer.addValue(ByteBuffer.wrap(value));
      numWritten++;
    }

    SmooshedWriter writer = smoosher.addWithSmooshedWriter(fileNameBase, serializer.getSerializedSize());
    serializer.writeTo(writer, smoosher);
    writer.close();
    smoosher.close();
    SmooshedFileMapper fileMapper = SmooshedFileMapper.load(tmpFile);

    ByteBuffer base = fileMapper.mapFile(fileNameBase);

    CompressedVariableSizedBlobColumn column = CompressedVariableSizedBlobColumnSupplier.fromByteBuffer(
        fileNameBase,
        base,
        ByteOrder.nativeOrder(),
        fileMapper
    ).get();
    for (int row = 0; row < numWritten; row++) {
      ByteBuffer value = column.get(row);
      byte[] bytes = new byte[value.remaining()];
      value.get(bytes);
      Assert.assertArrayEquals("Row " + row, values.get(row), bytes);
    }
    for (int rando = 0; rando < numWritten; rando++) {
      int row = ThreadLocalRandom.current().nextInt(0, numWritten - 1);
      ByteBuffer value = column.get(row);
      byte[] bytes = new byte[value.remaining()];
      value.get(bytes);
      Assert.assertArrayEquals("Row " + row, values.get(row), bytes);
    }
    column.close();
    fileMapper.close();
  }

  @Test
  public void testLongs() throws IOException
  {
    // value sizes increase until they span at least 3 pages of compressed buffers
    final File tmpFile = tempFolder.newFolder();
    final FileSmoosher smoosher = new FileSmoosher(tmpFile);

    final File tmpFile2 = tempFolder.newFolder();
    final SegmentWriteOutMedium writeOutMedium =
        TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(tmpFile2);

    final String fileNameBase = "test";

    final CompressionStrategy compressionStrategy = CompressionStrategy.LZ4;
    CompressedLongsSerializer serializer = new CompressedLongsSerializer(
        writeOutMedium,
        compressionStrategy
    );
    serializer.open();

    final Random r = ThreadLocalRandom.current();
    int numWritten = 0;
    final List<Long> values = new ArrayList<>();
    for (int i = 0; i < 5_000_000; i++) {
      long l = r.nextLong();
      values.add(l);
      serializer.add(l);
      numWritten++;
    }

    SmooshedWriter writer = smoosher.addWithSmooshedWriter(fileNameBase, serializer.getSerializedSize());
    serializer.writeTo(writer, smoosher);
    writer.close();
    smoosher.close();
    SmooshedFileMapper fileMapper = SmooshedFileMapper.load(tmpFile);

    ByteBuffer base = fileMapper.mapFile(fileNameBase);

    CompressedLongsReader reader = CompressedLongsReader.fromByteBuffer(base, ByteOrder.nativeOrder()).get();
    for (int row = 0; row < numWritten; row++) {
      long l = reader.get(row);
      Assert.assertEquals("Row " + row, values.get(row).longValue(), l);
    }

    // test random access pt 1
    int random = 0;
    Assert.assertEquals("Row " + random, values.get(random).longValue(), reader.get(random));
    random = 2_000_000;
    Assert.assertEquals("Row " + random, values.get(random).longValue(), reader.get(random));
    random = 1_000_000;
    Assert.assertEquals("Row " + random, values.get(random).longValue(), reader.get(random));

    // test random access pt 2
    for (int rando = 0; rando < numWritten; rando++) {
      int row = ThreadLocalRandom.current().nextInt(0, numWritten - 1);
      long l = reader.get(row);
      Assert.assertEquals("Row " + row, values.get(row).longValue(), l);
    }

    reader.close();
    fileMapper.close();
  }
}

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

package org.apache.druid.java.util.common.io.smoosh;

import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.BufferUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.file.SegmentFileChannel;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 */
public class SmooshedFileMapperTest
{
  @TempDir
  public File folder;

  @Test
  public void testSanity() throws Exception
  {
    File baseDir = new File(folder, "base");
    baseDir.mkdir();

    try (FileSmoosher smoosher = new FileSmoosher(baseDir, 21)) {
      for (int i = 0; i < 20; ++i) {
        File tmpFile = new File(folder, StringUtils.format("smoosh-%s.bin", i));
        Files.write(Ints.toByteArray(i), tmpFile);
        smoosher.add(StringUtils.format("%d", i), tmpFile);
      }
    }
    validateOutput(baseDir);
  }

  @Test
  public void testWhenFirstWriterClosedInTheMiddle() throws Exception
  {
    File baseDir = new File(folder, "base");
    baseDir.mkdir();

    try (FileSmoosher smoosher = new FileSmoosher(baseDir, 21)) {
      final SegmentFileChannel writer = smoosher.addWithChannel(StringUtils.format("%d", 19), 4);

      for (int i = 0; i < 19; ++i) {
        File tmpFile = File.createTempFile(StringUtils.format("smoosh-%s", i), ".bin");
        Files.write(Ints.toByteArray(i), tmpFile);
        smoosher.add(StringUtils.format("%d", i), tmpFile);
        if (i == 10) {
          writer.write(ByteBuffer.wrap(Ints.toByteArray(19)));
          writer.close();
        }
        tmpFile.delete();
      }
    }
    validateOutput(baseDir);
  }

  @Test
  public void testColumnSerializedSizeExceedsMaximum() throws Exception
  {
    File baseDir = new File(folder, "base");
    baseDir.mkdir();
    try (FileSmoosher smoosher = new FileSmoosher(baseDir, 5)) {
      MatcherAssert.assertThat(
          Assertions.assertThrows(
              DruidException.class,
              () -> smoosher.addWithChannel("foo", 10)
          ),
          new DruidExceptionMatcher(DruidException.Persona.ADMIN, DruidException.Category.RUNTIME_FAILURE, "general")
              .expectMessageContains("Serialized buffer size[10] for column[foo] exceeds the maximum[5].")
      );
    }
  }

  @Test
  public void testExceptionForUnClosedFiles() throws Exception
  {
    File baseDir = new File(folder, "base");
    baseDir.mkdir();
    Assertions.assertThrows(ISE.class, () -> {
      try (FileSmoosher smoosher = new FileSmoosher(baseDir, 21)) {
        for (int i = 0; i < 19; ++i) {
          final SegmentFileChannel writer = smoosher.addWithChannel(StringUtils.format("%d", i), 4);
          writer.write(ByteBuffer.wrap(Ints.toByteArray(i)));
        }
      }
    });
  }

  @Test
  public void testWhenFirstWriterClosedAtTheEnd() throws Exception
  {
    File baseDir = new File(folder, "base");
    baseDir.mkdir();

    try (FileSmoosher smoosher = new FileSmoosher(baseDir, 21)) {
      final SegmentFileChannel writer = smoosher.addWithChannel(StringUtils.format("%d", 19), 4);
      writer.write(ByteBuffer.wrap(Ints.toByteArray(19)));

      for (int i = 0; i < 19; ++i) {
        File tmpFile = File.createTempFile(StringUtils.format("smoosh-%s", i), ".bin");
        Files.write(Ints.toByteArray(i), tmpFile);
        smoosher.add(StringUtils.format("%d", i), tmpFile);
        tmpFile.delete();
      }
      writer.close();
    }
    validateOutput(baseDir);
  }

  @Test
  public void testWhenWithPathyLookingFileNames() throws Exception
  {
    String prefix = "foo/bar/";
    File baseDir = new File(folder, "base");
    baseDir.mkdir();

    try (FileSmoosher smoosher = new FileSmoosher(baseDir, 21)) {
      final SegmentFileChannel writer = smoosher.addWithChannel(StringUtils.format("%s%d", prefix, 19), 4);
      writer.write(ByteBuffer.wrap(Ints.toByteArray(19)));

      for (int i = 0; i < 19; ++i) {
        File tmpFile = File.createTempFile(StringUtils.format("smoosh-%s", i), ".bin");
        Files.write(Ints.toByteArray(i), tmpFile);
        smoosher.add(StringUtils.format("%s%d", prefix, i), tmpFile);
        tmpFile.delete();
      }
      writer.close();
    }
    validateOutput(baseDir, prefix);
  }

  @Test
  public void testBehaviorWhenReportedSizesLargeAndExceptionIgnored() throws Exception
  {
    File baseDir = new File(folder, "base");
    baseDir.mkdir();

    try (FileSmoosher smoosher = new FileSmoosher(baseDir, 21)) {
      for (int i = 0; i < 20; ++i) {
        final SegmentFileChannel writer = smoosher.addWithChannel(StringUtils.format("%d", i), 7);
        writer.write(ByteBuffer.wrap(Ints.toByteArray(i)));
        try {
          writer.close();
          Assertions.fail("IOException expected");
        }
        catch (IOException ignored) {
          // expected
        }
      }
    }

    File[] files = baseDir.listFiles();
    Assertions.assertNotNull(files);
    Arrays.sort(files);

    Assertions.assertEquals(6, files.length); // 4 smoosh files and 1 meta file
    for (int i = 0; i < 4; ++i) {
      Assertions.assertEquals(FileSmoosher.makeChunkFile(baseDir, i), files[i]);
    }
    Assertions.assertEquals(FileSmoosher.metaFile(baseDir), files[files.length - 1]);

    try (SmooshedFileMapper mapper = SmooshedFileMapper.load(baseDir)) {
      for (int i = 0; i < 20; ++i) {
        ByteBuffer buf = mapper.mapFile(StringUtils.format("%d", i));
        Assertions.assertEquals(0, buf.position());
        Assertions.assertEquals(4, buf.remaining());
        Assertions.assertEquals(4, buf.capacity());
        Assertions.assertEquals(i, buf.getInt());
      }
    }
  }

  @Test
  public void testBehaviorWhenReportedSizesSmall() throws Exception
  {
    File baseDir = new File(folder, "base");
    baseDir.mkdir();

    try (FileSmoosher smoosher = new FileSmoosher(baseDir, 21)) {
      boolean exceptionThrown = false;
      try (final SegmentFileChannel writer = smoosher.addWithChannel("1", 2)) {
        writer.write(ByteBuffer.wrap(Ints.toByteArray(1)));
      }
      catch (ISE e) {
        Assertions.assertTrue(e.getMessage().contains("Liar!!!"));
        exceptionThrown = true;
      }

      Assertions.assertTrue(exceptionThrown);
      File[] files = baseDir.listFiles();
      Assertions.assertNotNull(files);
      Assertions.assertEquals(1, files.length);
    }
  }

  @Test
  public void testDeterministicFileUnmapping() throws IOException
  {
    File baseDir = new File(folder, "base");
    baseDir.mkdir();

    long totalMemoryUsedBeforeAddingFile = BufferUtils.totalMemoryUsedByDirectAndMappedBuffers();
    try (FileSmoosher smoosher = new FileSmoosher(baseDir)) {
      File dataFile = new File(folder, "data.bin");
      try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw")) {
        raf.setLength(1 << 20); // 1 MiB
      }
      smoosher.add(dataFile.getName(), dataFile);
    }
    long totalMemoryUsedAfterAddingFile = BufferUtils.totalMemoryUsedByDirectAndMappedBuffers();
    // Assert no hanging file mappings left by either smoosher or smoosher.add(file)
    Assertions.assertEquals(totalMemoryUsedBeforeAddingFile, totalMemoryUsedAfterAddingFile);
  }

  private void validateOutput(File baseDir) throws IOException
  {
    validateOutput(baseDir, "");
  }

  private void validateOutput(File baseDir, String prefix) throws IOException
  {
    File[] files = baseDir.listFiles();
    Arrays.sort(files);

    Assertions.assertEquals(5, files.length); // 4 smooshed files and 1 meta file
    for (int i = 0; i < 4; ++i) {
      Assertions.assertEquals(FileSmoosher.makeChunkFile(baseDir, i), files[i]);
    }
    Assertions.assertEquals(FileSmoosher.metaFile(baseDir), files[files.length - 1]);

    try (SmooshedFileMapper mapper = SmooshedFileMapper.load(baseDir)) {
      for (int i = 0; i < 20; ++i) {
        ByteBuffer buf = mapper.mapFile(StringUtils.format("%s%d", prefix, i));
        Assertions.assertEquals(0, buf.position());
        Assertions.assertEquals(4, buf.remaining());
        Assertions.assertEquals(4, buf.capacity());
        Assertions.assertEquals(i, buf.getInt());
      }
    }
  }
}

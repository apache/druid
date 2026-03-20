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

package org.apache.druid.segment.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import junit.framework.Assert;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.data.CompressionStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

class SegmentFileMapperV10Test
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @TempDir
  File tempDir;

  @Test
  void testWriteRead() throws IOException
  {
    File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      for (int i = 0; i < 20; ++i) {
        File tmpFile = new File(tempDir, StringUtils.format("smoosh-%s.bin", i));
        Files.write(Ints.toByteArray(i), tmpFile);
        builder.add(StringUtils.format("%d", i), tmpFile);
      }
    }

    File[] files = baseDir.listFiles();
    File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);

    Assert.assertEquals(1, files.length); // v10 format has a single file
    Assert.assertEquals(segmentFile, files[0]);

    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      validateInternalFiles(mapper);
    }
  }

  @Test
  void testWriteReadCompressed() throws IOException
  {
    File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir, CompressionStrategy.ZSTD)) {
      for (int i = 0; i < 20; ++i) {
        File tmpFile = new File(tempDir, StringUtils.format("smoosh-%s.bin", i));
        Files.write(Ints.toByteArray(i), tmpFile);
        builder.add(StringUtils.format("%d", i), tmpFile);
      }
    }

    File[] files = baseDir.listFiles();
    File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);

    Assert.assertEquals(1, files.length); // v10 format has a single file
    Assert.assertEquals(segmentFile, files[0]);

    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      validateInternalFiles(mapper);
    }
  }

  @Test
  void testWriteReadWithExternal() throws IOException
  {
    String externalName = "external.segment";
    File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      for (int i = 0; i < 20; ++i) {
        File tmpFile = new File(tempDir, StringUtils.format("smoosh-%s.bin", i));
        Files.write(Ints.toByteArray(i), tmpFile);
        builder.add(StringUtils.format("%d", i), tmpFile);
      }
      SegmentFileBuilder external = builder.getExternalBuilder(externalName);
      for (int i = 20; i < 40; ++i) {
        File tmpFile = new File(tempDir, StringUtils.format("smoosh-%s.bin", i));
        Files.write(Ints.toByteArray(i), tmpFile);
        external.add(StringUtils.format("%d", i), tmpFile);
      }
    }

    File[] files = baseDir.listFiles();
    Arrays.sort(files);
    File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);

    Assert.assertEquals(2, files.length);
    Assert.assertEquals(segmentFile, files[0]);
    Assert.assertEquals(new File(baseDir, externalName), files[1]);

    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER, List.of(externalName))) {
      validateInternalFiles(mapper);
      for (int i = 20; i < 40; ++i) {
        ByteBuffer buf = mapper.mapExternalFile(externalName, String.valueOf(i));
        Assert.assertEquals(0, buf.position());
        Assert.assertEquals(4, buf.remaining());
        Assert.assertEquals(4, buf.capacity());
        Assert.assertEquals(i, buf.getInt());
      }
    }
  }

  private static void validateInternalFiles(SegmentFileMapperV10 mapper) throws IOException
  {
    for (int i = 0; i < 20; ++i) {
      ByteBuffer buf = mapper.mapFile(String.valueOf(i));
      Assert.assertEquals(0, buf.position());
      Assert.assertEquals(4, buf.remaining());
      Assert.assertEquals(4, buf.capacity());
      Assert.assertEquals(i, buf.getInt());
    }
  }
}

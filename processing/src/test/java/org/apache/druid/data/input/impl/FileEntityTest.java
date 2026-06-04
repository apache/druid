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

package org.apache.druid.data.input.impl;

import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntity.CleanableFile;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.utils.CompressionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class FileEntityTest
{
  private static final String CONTENT = "the quick brown fox\n";

  @TempDir
  File tempDir;

  @Test
  public void test_openRaw_returnsRawBytesWithoutDecompressing() throws IOException
  {
    final File gzFile = gzipFile("data.txt.gz", CONTENT);
    try (InputStream in = new FileEntity(gzFile).openRaw()) {
      Assertions.assertArrayEquals(Files.readAllBytes(gzFile.toPath()), IOUtils.toByteArray(in));
    }
  }

  @Test
  public void test_open_decompressesBasedOnFileName() throws IOException
  {
    final File gzFile = gzipFile("data.txt.gz", CONTENT);
    try (InputStream in = new FileEntity(gzFile).open()) {
      Assertions.assertEquals(CONTENT, IOUtils.toString(in, StandardCharsets.UTF_8));
    }
  }

  @Test
  public void test_open_returnsContentForUncompressedFile() throws IOException
  {
    final File file = writeFile("data.txt", CONTENT);
    try (InputStream in = new FileEntity(file).open()) {
      Assertions.assertEquals(CONTENT, IOUtils.toString(in, StandardCharsets.UTF_8));
    }
  }

  @Test
  public void test_fetch_uncompressedReturnsSameFileWithoutCopying() throws IOException
  {
    final File file = writeFile("data.txt", CONTENT);
    final File fetchDir = makeDir("fetch-uncompressed");
    try (CleanableFile fetched = new FileEntity(file).fetch(fetchDir, fetchBuffer())) {
      Assertions.assertEquals(file, fetched.file());
    }
    // close() is a no-op for uncompressed files; the original file remains.
    Assertions.assertTrue(file.exists());
  }

  @Test
  public void test_fetch_compressedDecompressesIntoTempDirAndCleansUp() throws IOException
  {
    final File gzFile = gzipFile("data.txt.gz", CONTENT);
    final File fetchDir = makeDir("fetch-compressed");
    final File fetchedFile;
    try (CleanableFile fetched = new FileEntity(gzFile).fetch(fetchDir, fetchBuffer())) {
      fetchedFile = fetched.file();
      Assertions.assertEquals(fetchDir, fetchedFile.getParentFile());
      Assertions.assertEquals(CONTENT, StringUtils.fromUtf8(Files.readAllBytes(fetchedFile.toPath())));
    }
    // close() removes the decompressed temp file; the original compressed file remains.
    Assertions.assertFalse(fetchedFile.exists());
    Assertions.assertTrue(gzFile.exists());
  }

  private File writeFile(final String name, final String content) throws IOException
  {
    final File file = new File(tempDir, name);
    Files.write(file.toPath(), StringUtils.toUtf8(content));
    return file;
  }

  private File gzipFile(final String name, final String content) throws IOException
  {
    final File source = writeFile("source-for-" + name, content);
    final File gzFile = new File(tempDir, name);
    CompressionUtils.gzip(source, gzFile);
    return gzFile;
  }

  private File makeDir(final String name) throws IOException
  {
    final File dir = new File(tempDir, name);
    Files.createDirectories(dir.toPath());
    return dir;
  }

  private static byte[] fetchBuffer()
  {
    return new byte[InputEntity.DEFAULT_FETCH_BUFFER_SIZE];
  }
}

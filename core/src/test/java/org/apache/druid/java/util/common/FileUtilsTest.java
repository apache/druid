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

package org.apache.druid.java.util.common;

import com.google.common.base.Predicates;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;

public class FileUtilsTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testMap() throws IOException
  {
    File dataFile = temporaryFolder.newFile("data");
    long buffersMemoryBefore = BufferUtils.totalMemoryUsedByDirectAndMappedBuffers();
    try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw")) {
      raf.write(42);
      raf.setLength(1 << 20); // 1 MiB
    }
    try (MappedByteBufferHandler mappedByteBufferHandler = FileUtils.map(dataFile)) {
      Assert.assertEquals(42, mappedByteBufferHandler.get().get(0));
    }
    long buffersMemoryAfter = BufferUtils.totalMemoryUsedByDirectAndMappedBuffers();
    Assert.assertEquals(buffersMemoryBefore, buffersMemoryAfter);
  }

  @Test
  public void testMapFileTooLarge() throws IOException
  {
    File dataFile = temporaryFolder.newFile("data");
    try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw")) {
      raf.write(42);
      raf.setLength(1 << 20); // 1 MiB
    }
    final IllegalArgumentException e = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> FileUtils.map(dataFile, 0, (long) Integer.MAX_VALUE + 1)
    );
    MatcherAssert.assertThat(e.getMessage(), CoreMatchers.containsString("Cannot map region larger than"));
  }

  @Test
  public void testMapRandomAccessFileTooLarge() throws IOException
  {
    File dataFile = temporaryFolder.newFile("data");
    try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw")) {
      raf.write(42);
      raf.setLength(1 << 20); // 1 MiB
    }
    try (RandomAccessFile raf = new RandomAccessFile(dataFile, "r")) {
      final IllegalArgumentException e = Assert.assertThrows(
          IllegalArgumentException.class,
          () -> FileUtils.map(raf, 0, (long) Integer.MAX_VALUE + 1)
      );
      MatcherAssert.assertThat(e.getMessage(), CoreMatchers.containsString("Cannot map region larger than"));
    }
  }

  @Test
  public void testWriteAtomically() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder();
    final File tmpFile = new File(tmpDir, "file1");
    FileUtils.writeAtomically(tmpFile, out -> {
      out.write(StringUtils.toUtf8("foo"));
      return null;
    });
    Assert.assertEquals("foo", StringUtils.fromUtf8(Files.readAllBytes(tmpFile.toPath())));

    // Try writing again, throw error partway through.
    try {
      FileUtils.writeAtomically(tmpFile, out -> {
        out.write(StringUtils.toUtf8("bar"));
        out.flush();
        throw new ISE("OMG!");
      });
    }
    catch (IllegalStateException e) {
      // Suppress
    }
    Assert.assertEquals("foo", StringUtils.fromUtf8(Files.readAllBytes(tmpFile.toPath())));

    FileUtils.writeAtomically(tmpFile, out -> {
      out.write(StringUtils.toUtf8("baz"));
      return null;
    });
    Assert.assertEquals("baz", StringUtils.fromUtf8(Files.readAllBytes(tmpFile.toPath())));
  }

  @Test
  public void testCreateTempDir() throws IOException
  {
    final File tempDir = FileUtils.createTempDir();
    try {
      Assert.assertEquals(
          new File(System.getProperty("java.io.tmpdir")).toPath(),
          tempDir.getParentFile().toPath()
      );
    }
    finally {
      Files.delete(tempDir.toPath());
    }
  }

  @Test
  public void testCreateTempDirNonexistentBase()
  {
    final String oldJavaTmpDir = System.getProperty("java.io.tmpdir");
    final String nonExistentDir = oldJavaTmpDir + "/nonexistent";

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage(StringUtils.format("java.io.tmpdir (%s) does not exist", nonExistentDir));

    try {
      System.setProperty("java.io.tmpdir", nonExistentDir);
      FileUtils.createTempDir();
    }
    finally {
      System.setProperty("java.io.tmpdir", oldJavaTmpDir);
    }
  }

  @Test
  public void testCreateTempDirUnwritableBase() throws IOException
  {
    final File baseDir = FileUtils.createTempDir();
    try {
      expectedException.expect(IllegalStateException.class);
      expectedException.expectMessage("java.io.tmpdir (" + baseDir + ") is not writable");

      final String oldJavaTmpDir = System.getProperty("java.io.tmpdir");
      try {
        System.setProperty("java.io.tmpdir", baseDir.getPath());
        baseDir.setWritable(false);
        FileUtils.createTempDir();
      }
      finally {
        System.setProperty("java.io.tmpdir", oldJavaTmpDir);
      }
    }
    finally {
      baseDir.setWritable(true);
      Files.delete(baseDir.toPath());
    }
  }

  @Test
  public void testMkdirp() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder();
    final File testDirectory = new File(tmpDir, "test");

    FileUtils.mkdirp(testDirectory);
    Assert.assertTrue(testDirectory.isDirectory());

    FileUtils.mkdirp(testDirectory);
    Assert.assertTrue(testDirectory.isDirectory());
  }

  @Test
  public void testMkdirpCannotCreateOverExistingFile() throws IOException
  {
    final File tmpFile = temporaryFolder.newFile();

    expectedException.expect(IOException.class);
    expectedException.expectMessage("Cannot create directory");
    FileUtils.mkdirp(tmpFile);
  }

  @Test
  public void testMkdirpCannotCreateInNonWritableDirectory() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder();
    final File testDirectory = new File(tmpDir, "test");
    tmpDir.setWritable(false);
    try {
      final IOException e = Assert.assertThrows(IOException.class, () -> FileUtils.mkdirp(testDirectory));

      MatcherAssert.assertThat(
          e,
          ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Cannot create directory"))
      );
    }
    finally {
      tmpDir.setWritable(true);
    }

    // Now it should work.
    FileUtils.mkdirp(testDirectory);
    Assert.assertTrue(testDirectory.isDirectory());
  }

  @Test
  public void testCopyLarge() throws IOException
  {
    final File dstDirectory = temporaryFolder.newFolder();
    final File dstFile = new File(dstDirectory, "dst");
    final String data = "test data to write";

    final long result = FileUtils.copyLarge(
        () -> new ByteArrayInputStream(StringUtils.toUtf8(data)),
        dstFile,
        new byte[1024],
        Predicates.alwaysFalse(),
        3,
        null
    );

    Assert.assertEquals(data.length(), result);
    Assert.assertEquals(data, StringUtils.fromUtf8(Files.readAllBytes(dstFile.toPath())));
  }
}

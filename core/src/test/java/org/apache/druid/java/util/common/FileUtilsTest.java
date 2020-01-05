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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;

public class FileUtilsTest
{
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testMap() throws IOException
  {
    File dataFile = folder.newFile("data");
    long buffersMemoryBefore = BufferUtils.totalMemoryUsedByDirectAndMappedBuffers();
    try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw")) {
      raf.write(42);
      raf.setLength(1 << 20); // 1 MB
    }
    try (MappedByteBufferHandler mappedByteBufferHandler = FileUtils.map(dataFile)) {
      Assert.assertEquals(42, mappedByteBufferHandler.get().get(0));
    }
    long buffersMemoryAfter = BufferUtils.totalMemoryUsedByDirectAndMappedBuffers();
    Assert.assertEquals(buffersMemoryBefore, buffersMemoryAfter);
  }

  @Test
  public void testWriteAtomically() throws IOException
  {
    final File tmpDir = folder.newFolder();
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
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("java.io.tmpdir (/nonexistent) does not exist");

    final String oldJavaTmpDir = System.getProperty("java.io.tmpdir");
    try {
      System.setProperty("java.io.tmpdir", "/nonexistent");
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
}

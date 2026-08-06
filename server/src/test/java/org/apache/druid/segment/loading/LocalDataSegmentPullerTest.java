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

package org.apache.druid.segment.loading;

import com.google.common.io.Files;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.utils.CompressionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 *
 */
public class LocalDataSegmentPullerTest
{
  @TempDir
  public File temporaryFolder;
  private File tmpDir;
  private LocalDataSegmentPuller puller;

  @BeforeEach
  public void setup() throws IOException
  {
    tmpDir = newFolder(temporaryFolder, "junit");
    puller = new LocalDataSegmentPuller();
  }

  @AfterEach
  public void after() throws IOException
  {
    FileUtils.deleteDirectory(tmpDir);
  }

  @Test
  public void simpleZipTest() throws IOException, SegmentLoadingException
  {
    File file = new File(tmpDir, "test1data");
    File zipFile = new File(temporaryFolder, "ziptest.zip");
    Assertions.assertTrue(zipFile.createNewFile());
    try (OutputStream outputStream = new FileOutputStream(file)) {
      outputStream.write(new byte[0]);
      outputStream.flush();
    }
    CompressionUtils.zip(tmpDir, zipFile);
    file.delete();

    Assertions.assertFalse(file.exists());
    Assertions.assertTrue(zipFile.exists());
    puller.getSegmentFiles(zipFile, tmpDir);
    Assertions.assertTrue(file.exists());
  }

  @Test
  public void simpleGZTest() throws IOException, SegmentLoadingException
  {
    File zipFile = File.createTempFile("gztest", ".gz");
    File unZipFile = new File(
        tmpDir,
        Files.getNameWithoutExtension(
            zipFile.getAbsolutePath()
        )
    );
    unZipFile.delete();
    zipFile.delete();
    try (OutputStream fOutStream = new FileOutputStream(zipFile)) {
      try (OutputStream outputStream = new GZIPOutputStream(fOutStream)) {
        outputStream.write(new byte[0]);
        outputStream.flush();
      }
    }

    Assertions.assertTrue(zipFile.exists());
    Assertions.assertFalse(unZipFile.exists());
    puller.getSegmentFiles(zipFile, tmpDir);
    Assertions.assertTrue(unZipFile.exists());
  }

  @Test
  public void simpleDirectoryTest() throws IOException, SegmentLoadingException
  {
    File srcDir = newFolder(temporaryFolder, "junit");
    File tmpFile = File.createTempFile("test", "file", srcDir);
    File expectedOutput = new File(tmpDir, Files.getNameWithoutExtension(tmpFile.getAbsolutePath()));
    Assertions.assertFalse(expectedOutput.exists());
    puller.getSegmentFiles(srcDir, tmpDir);
    Assertions.assertTrue(expectedOutput.exists());
  }

  private static File newFolder(File root, String... subDirs) throws IOException
  {
    if (subDirs.length == 0 || (subDirs.length == 1 && "junit".equals(subDirs[0]))) {
      return java.nio.file.Files.createTempDirectory(root.toPath(), "junit").toFile();
    }
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }
}

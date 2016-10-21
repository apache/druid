/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.loading;

import com.google.common.io.Files;

import io.druid.java.util.common.CompressionUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File tmpDir;
  private LocalDataSegmentPuller puller;

  @Before
  public void setup() throws IOException
  {
    tmpDir = temporaryFolder.newFolder();
    tmpDir.deleteOnExit();
    puller = new LocalDataSegmentPuller();
  }

  @Test
  public void simpleZipTest() throws IOException, SegmentLoadingException
  {
    File file = new File(tmpDir, "test1data");
    File zipFile = temporaryFolder.newFile("ziptest.zip");
    try (OutputStream outputStream = new FileOutputStream(file)) {
      outputStream.write(new byte[0]);
      outputStream.flush();
    }
    CompressionUtils.zip(tmpDir, zipFile);
    file.delete();

    Assert.assertFalse(file.exists());
    Assert.assertTrue(zipFile.exists());
    puller.getSegmentFiles(zipFile, tmpDir);
    Assert.assertTrue(file.exists());
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

    Assert.assertTrue(zipFile.exists());
    Assert.assertFalse(unZipFile.exists());
    puller.getSegmentFiles(zipFile, tmpDir);
    Assert.assertTrue(unZipFile.exists());
  }

  @Test
  public void simpleDirectoryTest() throws IOException, SegmentLoadingException
  {
    File srcDir = temporaryFolder.newFolder();
    File tmpFile = File.createTempFile("test", "file", srcDir);
    File expectedOutput = new File(tmpDir, Files.getNameWithoutExtension(tmpFile.getAbsolutePath()));
    Assert.assertFalse(expectedOutput.exists());
    puller.getSegmentFiles(srcDir, tmpDir);
    Assert.assertTrue(expectedOutput.exists());
  }
}


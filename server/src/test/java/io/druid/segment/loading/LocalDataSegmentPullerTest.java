/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.loading;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.metamx.common.CompressionUtils;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 *
 */
public class LocalDataSegmentPullerTest
{
  private File tmpDir;
  private LocalDataSegmentPuller puller;

  @Before
  public void setup()
  {
    tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    puller = new LocalDataSegmentPuller();
  }

  @After
  public void tearDown() throws IOException
  {
    deleteFiles(tmpDir);
  }

  public static void deleteFiles(File... files) throws IOException
  {
    IOException ex = null;
    for (File file : files) {
      if (file == null || !file.exists()) {
        continue;
      }
      if (!file.delete()) {
        IOException e = new IOException("Could not delete " + file.getAbsolutePath());
        if (ex == null) {
          ex = e;
        } else {
          ex.addSuppressed(e);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  @Test
  public void simpleZipTest() throws IOException, SegmentLoadingException
  {
    File file = new File(tmpDir, "test1data");
    File zipFile = File.createTempFile("ziptest", ".zip");
    file.deleteOnExit();
    zipFile.deleteOnExit();
    zipFile.delete();
    try {
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
    finally {
      deleteFiles(file, zipFile);
    }
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
    zipFile.deleteOnExit();
    zipFile.delete();
    try {
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
    }finally{
      deleteFiles(zipFile, unZipFile);
    }
  }

  @Test
  public void simpleDirectoryTest() throws IOException, SegmentLoadingException
  {
    File srcDir = Files.createTempDir();
    File tmpFile = File.createTempFile("test", "file", srcDir);
    File expectedOutput = new File(tmpDir, Files.getNameWithoutExtension(tmpFile.getAbsolutePath()));
    expectedOutput.delete();
    try{
      Assert.assertFalse(expectedOutput.exists());
      puller.getSegmentFiles(srcDir, tmpDir);
      Assert.assertTrue(expectedOutput.exists());
    }finally{
      deleteFiles(expectedOutput, tmpFile, srcDir);
    }
  }
}


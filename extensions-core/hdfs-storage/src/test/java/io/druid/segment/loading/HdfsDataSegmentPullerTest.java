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

import com.google.common.io.ByteStreams;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.StringUtils;
import io.druid.storage.hdfs.HdfsDataSegmentPuller;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.zip.GZIPOutputStream;

/**
 *
 */
public class HdfsDataSegmentPullerTest
{
  private static MiniDFSCluster miniCluster;
  private static File hdfsTmpDir;
  private static URI uriBase;
  private static Path filePath = new Path("/tmp/foo");
  private static Path perTestPath = new Path("/tmp/tmp2");
  private static String pathContents = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum";
  private static byte[] pathByteContents = StringUtils.toUtf8(pathContents);
  private static Configuration conf;

  @BeforeClass
  public static void setupStatic() throws IOException, ClassNotFoundException
  {
    hdfsTmpDir = File.createTempFile("hdfsHandlerTest", "dir");
    if (!hdfsTmpDir.delete()) {
      throw new IOE("Unable to delete hdfsTmpDir [%s]", hdfsTmpDir.getAbsolutePath());
    }
    conf = new Configuration(true);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsTmpDir.getAbsolutePath());
    miniCluster = new MiniDFSCluster.Builder(conf).build();
    uriBase = miniCluster.getURI(0);

    final File tmpFile = File.createTempFile("hdfsHandlerTest", ".data");
    tmpFile.delete();
    try {
      Files.copy(new ByteArrayInputStream(pathByteContents), tmpFile.toPath());
      try (OutputStream stream = miniCluster.getFileSystem().create(filePath)) {
        Files.copy(tmpFile.toPath(), stream);
      }
    }
    finally {
      tmpFile.delete();
    }
  }

  @AfterClass
  public static void tearDownStatic() throws IOException
  {
    if (miniCluster != null) {
      miniCluster.shutdown(true);
    }
    FileUtils.deleteDirectory(hdfsTmpDir);
  }


  private HdfsDataSegmentPuller puller;

  @Before
  public void setUp()
  {
    puller = new HdfsDataSegmentPuller(conf);
  }

  @After
  public void tearDown() throws IOException
  {
    miniCluster.getFileSystem().delete(perTestPath, true);
  }

  @Test
  public void testZip() throws IOException, SegmentLoadingException
  {
    final File tmpDir = com.google.common.io.Files.createTempDir();
    final File tmpFile = File.createTempFile("zipContents", ".txt", tmpDir);

    final Path zipPath = new Path("/tmp/testZip.zip");

    final File outTmpDir = com.google.common.io.Files.createTempDir();

    final URI uri = URI.create(uriBase.toString() + zipPath.toString());

    try (final OutputStream stream = new FileOutputStream(tmpFile)) {
      ByteStreams.copy(new ByteArrayInputStream(pathByteContents), stream);
    }
    Assert.assertTrue(tmpFile.exists());

    final File outFile = new File(outTmpDir, tmpFile.getName());
    outFile.delete();

    try (final OutputStream stream = miniCluster.getFileSystem().create(zipPath)) {
      CompressionUtils.zip(tmpDir, stream);
    }
    try {
      Assert.assertFalse(outFile.exists());
      puller.getSegmentFiles(uri, outTmpDir);
      Assert.assertTrue(outFile.exists());

      Assert.assertArrayEquals(pathByteContents, Files.readAllBytes(outFile.toPath()));
    }
    finally {
      if (tmpFile.exists()) {
        tmpFile.delete();
      }
      if (outFile.exists()) {
        outFile.delete();
      }
      if (outTmpDir.exists()) {
        outTmpDir.delete();
      }
      if (tmpDir.exists()) {
        tmpDir.delete();
      }
    }
  }

  @Test
  public void testGZ() throws IOException, SegmentLoadingException
  {
    final Path zipPath = new Path("/tmp/testZip.gz");

    final File outTmpDir = com.google.common.io.Files.createTempDir();
    final File outFile = new File(outTmpDir, "testZip");
    outFile.delete();

    final URI uri = URI.create(uriBase.toString() + zipPath.toString());

    try (final OutputStream outputStream = miniCluster.getFileSystem().create(zipPath);
         final OutputStream gzStream = new GZIPOutputStream(outputStream);
         final InputStream inputStream = new ByteArrayInputStream(pathByteContents)) {
      ByteStreams.copy(inputStream, gzStream);
    }
    try {
      Assert.assertFalse(outFile.exists());
      puller.getSegmentFiles(uri, outTmpDir);
      Assert.assertTrue(outFile.exists());

      Assert.assertArrayEquals(pathByteContents, Files.readAllBytes(outFile.toPath()));
    }
    finally {
      if (outFile.exists()) {
        outFile.delete();
      }
      if (outTmpDir.exists()) {
        outTmpDir.delete();
      }
    }
  }

  @Test
  public void testDir() throws IOException, SegmentLoadingException
  {

    final Path zipPath = new Path(perTestPath, "test.txt");

    final File outTmpDir = com.google.common.io.Files.createTempDir();
    final File outFile = new File(outTmpDir, "test.txt");
    outFile.delete();

    final URI uri = URI.create(uriBase.toString() + perTestPath.toString());

    try (final OutputStream outputStream = miniCluster.getFileSystem().create(zipPath);
         final InputStream inputStream = new ByteArrayInputStream(pathByteContents)) {
      ByteStreams.copy(inputStream, outputStream);
    }
    try {
      Assert.assertFalse(outFile.exists());
      puller.getSegmentFiles(uri, outTmpDir);
      Assert.assertTrue(outFile.exists());

      Assert.assertArrayEquals(pathByteContents, Files.readAllBytes(outFile.toPath()));
    }
    finally {
      if (outFile.exists()) {
        outFile.delete();
      }
      if (outTmpDir.exists()) {
        outTmpDir.delete();
      }
    }
  }
}

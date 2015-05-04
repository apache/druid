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

import com.google.common.io.ByteStreams;
import com.metamx.common.CompressionUtils;
import com.metamx.common.FileUtils;
import com.metamx.common.StringUtils;
import io.druid.storage.hdfs.HdfsDataSegmentPuller;
import io.druid.storage.hdfs.HdfsLoadSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
  private static String pathContents = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum";
  private static byte[] pathByteContents = StringUtils.toUtf8(pathContents);
  private static Configuration conf;

  @BeforeClass
  public static void setupStatic() throws IOException, ClassNotFoundException
  {
    hdfsTmpDir = File.createTempFile("hdfsHandlerTest", "dir");
    hdfsTmpDir.deleteOnExit();
    if (!hdfsTmpDir.delete()) {
      throw new IOException(String.format("Unable to delete hdfsTmpDir [%s]", hdfsTmpDir.getAbsolutePath()));
    }
    conf = new Configuration(true);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsTmpDir.getAbsolutePath());
    miniCluster = new MiniDFSCluster.Builder(conf).build();
    uriBase = miniCluster.getURI(0);

    final File tmpFile = File.createTempFile("hdfsHandlerTest", ".data");
    tmpFile.delete();
    try {
      tmpFile.deleteOnExit();
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
  }


  private HdfsDataSegmentPuller puller;
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setUp()
  {
    puller = new HdfsDataSegmentPuller(conf);
  }

  @Test
  public void testZip() throws IOException, SegmentLoadingException
  {
    final File tmpDir = tmpFolder.newFolder();
    final File tmpFile = File.createTempFile("zipContents", ".txt", tmpDir);

    final Path zipPath = new Path("/tmp", tmpFile.getName() + ".zip");

    final File outTmpDir = tmpFolder.newFolder();

    final URI uri = URI.create(uriBase.toString() + zipPath.toString());

    try (final OutputStream stream = new FileOutputStream(tmpFile)) {
      ByteStreams.copy(new ByteArrayInputStream(pathByteContents), stream);
    }
    Assert.assertTrue(tmpFile.exists());

    final File expectedOutFile = new File(outTmpDir, tmpFile.getName());
    expectedOutFile.delete();

    try {
      final long byteCount;
      try (final OutputStream stream = miniCluster.getFileSystem().create(zipPath)) {
         byteCount = CompressionUtils.zip(tmpDir, stream);
      }
      Assert.assertEquals(pathByteContents.length, byteCount);
      Assert.assertFalse(expectedOutFile.exists());
      final FileUtils.FileCopyResult fileCopyResult = puller.getSegmentFiles(uri, outTmpDir);
      Assert.assertEquals(pathByteContents.length, fileCopyResult.size());
      Assert.assertTrue(expectedOutFile.exists());

      Assert.assertArrayEquals(pathByteContents, Files.readAllBytes(expectedOutFile.toPath()));
    }finally {
      miniCluster.getFileSystem().delete(zipPath, true);
    }
  }

  @Test
  public void testGZ() throws IOException, SegmentLoadingException
  {
    final Path zipPath = new Path("/tmp/testZip.gz");

    final File outTmpDir = tmpFolder.newFolder();
    final File outFile = new File(outTmpDir, "testZip");
    outFile.delete();

    final URI uri = URI.create(uriBase.toString() + zipPath.toString());

    try {
      try (final OutputStream outputStream = miniCluster.getFileSystem().create(zipPath)) {
        try (final OutputStream gzStream = new GZIPOutputStream(outputStream)) {
          try (final InputStream inputStream = new ByteArrayInputStream(pathByteContents)) {
            ByteStreams.copy(inputStream, gzStream);
          }
        }
      }
      Assert.assertFalse(outFile.exists());
      puller.getSegmentFiles(uri, outTmpDir);
      Assert.assertTrue(outFile.exists());

      Assert.assertArrayEquals(pathByteContents, Files.readAllBytes(outFile.toPath()));
    } finally {
      miniCluster.getFileSystem().delete(zipPath, true);
    }
  }

  @Test
  public void testDir() throws IOException, SegmentLoadingException
  {

    final Path zipPath = new Path("/tmp/tmp2/test.txt");

    final File outTmpDir = tmpFolder.newFolder();
    final File outFile = new File(outTmpDir, "test.txt");
    outFile.delete();

    final URI uri = URI.create(uriBase.toString() + "/tmp/tmp2");

    try (final OutputStream outputStream = miniCluster.getFileSystem().create(zipPath)) {
      try (final InputStream inputStream = new ByteArrayInputStream(pathByteContents)) {
        ByteStreams.copy(inputStream, outputStream);
      }
    }
    Assert.assertFalse(outFile.exists());
    puller.getSegmentFiles(uri, outTmpDir);
    Assert.assertTrue(outFile.exists());

    Assert.assertArrayEquals(pathByteContents, Files.readAllBytes(outFile.toPath()));
  }

  @Test
  public void testURISupplier() throws IOException, SegmentLoadingException
  {
    final Path zipPath = new Path("/tmp/testZip.gz");

    final File outTmpDir = tmpFolder.newFolder();
    final File outFile = new File(outTmpDir, "testZip");
    outFile.delete();
    URISupplier uriSupplier = new HdfsLoadSpec(puller, zipPath.toString());
    final URI uri = uriSupplier.getURI();


    try {
      try (final OutputStream outputStream = miniCluster.getFileSystem().create(zipPath)) {
        try (final OutputStream gzStream = new GZIPOutputStream(outputStream)) {
          try (final InputStream inputStream = new ByteArrayInputStream(pathByteContents)) {
            ByteStreams.copy(inputStream, gzStream);
          }
        }
      }
      Assert.assertFalse(outFile.exists());
      puller.getSegmentFiles(uri, outTmpDir);
      Assert.assertTrue(outFile.exists());

      Assert.assertArrayEquals(pathByteContents, Files.readAllBytes(outFile.toPath()));
    }finally{
      miniCluster.getFileSystem().delete(zipPath, true);
    }
  }
}

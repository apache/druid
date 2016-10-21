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

import io.druid.java.util.common.StringUtils;
import io.druid.storage.hdfs.HdfsFileTimestampVersionFinder;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.regex.Pattern;

public class HdfsFileTimestampVersionFinderTest
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


  private HdfsFileTimestampVersionFinder finder;

  @Before
  public void setUp()
  {
    finder = new HdfsFileTimestampVersionFinder(conf);
  }

  @After
  public void tearDown() throws IOException
  {
    miniCluster.getFileSystem().delete(perTestPath, true);
  }


  @Test
  public void testSimpleLatestVersion() throws IOException, InterruptedException
  {
    final Path oldPath = new Path(perTestPath, "555test.txt");
    Assert.assertFalse(miniCluster.getFileSystem().exists(oldPath));
    try (final OutputStream outputStream = miniCluster.getFileSystem().create(oldPath)) {
      try (final InputStream inputStream = new ByteArrayInputStream(pathByteContents)) {
        ByteStreams.copy(inputStream, outputStream);
      }
    }

    Thread.sleep(10);

    final Path newPath = new Path(perTestPath, "666test.txt");
    Assert.assertFalse(miniCluster.getFileSystem().exists(newPath));
    try (final OutputStream outputStream = miniCluster.getFileSystem().create(newPath)) {
      try (final InputStream inputStream = new ByteArrayInputStream(pathByteContents)) {
        ByteStreams.copy(inputStream, outputStream);
      }
    }

    Assert.assertEquals(newPath.toString(), finder.getLatestVersion(oldPath.toUri(), Pattern.compile(".*")).getPath());
  }

  @Test
  public void testAlreadyLatestVersion() throws IOException, InterruptedException
  {
    final Path oldPath = new Path(perTestPath, "555test.txt");
    Assert.assertFalse(miniCluster.getFileSystem().exists(oldPath));
    try (final OutputStream outputStream = miniCluster.getFileSystem().create(oldPath)) {
      try (final InputStream inputStream = new ByteArrayInputStream(pathByteContents)) {
        ByteStreams.copy(inputStream, outputStream);
      }
    }

    Thread.sleep(10);

    final Path newPath = new Path(perTestPath, "666test.txt");
    Assert.assertFalse(miniCluster.getFileSystem().exists(newPath));
    try (final OutputStream outputStream = miniCluster.getFileSystem().create(newPath)) {
      try (final InputStream inputStream = new ByteArrayInputStream(pathByteContents)) {
        ByteStreams.copy(inputStream, outputStream);
      }
    }

    Assert.assertEquals(newPath.toString(), finder.getLatestVersion(newPath.toUri(), Pattern.compile(".*")).getPath());
  }

  @Test
  public void testNoLatestVersion() throws IOException, InterruptedException
  {
    final Path oldPath = new Path(perTestPath, "555test.txt");
    Assert.assertFalse(miniCluster.getFileSystem().exists(oldPath));
    Assert.assertNull(finder.getLatestVersion(oldPath.toUri(), Pattern.compile(".*")));
  }

  @Test
  public void testSimpleLatestVersionInDir() throws IOException, InterruptedException
  {
    final Path oldPath = new Path(perTestPath, "555test.txt");
    Assert.assertFalse(miniCluster.getFileSystem().exists(oldPath));
    try (final OutputStream outputStream = miniCluster.getFileSystem().create(oldPath)) {
      try (final InputStream inputStream = new ByteArrayInputStream(pathByteContents)) {
        ByteStreams.copy(inputStream, outputStream);
      }
    }

    Thread.sleep(10);

    final Path newPath = new Path(perTestPath, "666test.txt");
    Assert.assertFalse(miniCluster.getFileSystem().exists(newPath));
    try (final OutputStream outputStream = miniCluster.getFileSystem().create(newPath)) {
      try (final InputStream inputStream = new ByteArrayInputStream(pathByteContents)) {
        ByteStreams.copy(inputStream, outputStream);
      }
    }

    Assert.assertEquals(
        newPath.toString(),
        finder.getLatestVersion(perTestPath.toUri(), Pattern.compile(".*test\\.txt")).getPath()
    );
  }

  @Test
  public void testSkipMismatch() throws IOException, InterruptedException
  {
    final Path oldPath = new Path(perTestPath, "555test.txt");
    Assert.assertFalse(miniCluster.getFileSystem().exists(oldPath));
    try (final OutputStream outputStream = miniCluster.getFileSystem().create(oldPath)) {
      try (final InputStream inputStream = new ByteArrayInputStream(pathByteContents)) {
        ByteStreams.copy(inputStream, outputStream);
      }
    }

    Thread.sleep(10);

    final Path newPath = new Path(perTestPath, "666test.txt2");
    Assert.assertFalse(miniCluster.getFileSystem().exists(newPath));
    try (final OutputStream outputStream = miniCluster.getFileSystem().create(newPath)) {
      try (final InputStream inputStream = new ByteArrayInputStream(pathByteContents)) {
        ByteStreams.copy(inputStream, outputStream);
      }
    }

    Assert.assertEquals(
        oldPath.toString(),
        finder.getLatestVersion(perTestPath.toUri(), Pattern.compile(".*test\\.txt")).getPath()
    );
  }
}

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

package sun.net.www.protocol.hdfs;

import com.metamx.common.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;

/**
 *
 */
public class HandlerTest
{
  private static MiniDFSCluster miniCluster;
  private static File tmpDir;
  private static URI uriBase;
  private static Path filePath = new Path("/tmp/foo");
  private static String pathContents = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum";
  private static byte[] pathByteContents = StringUtils.toUtf8(pathContents);

  @BeforeClass
  public static void setupStatic() throws IOException, ClassNotFoundException
  {
    tmpDir = File.createTempFile("hdfsHandlerTest", "dir");
    tmpDir.deleteOnExit();
    if (!tmpDir.delete()) {
      throw new IOException(String.format("Unable to delete tmpDir [%s]", tmpDir.getAbsolutePath()));
    }
    Configuration conf = new Configuration(true);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpDir.getAbsolutePath());
    miniCluster = new MiniDFSCluster.Builder(conf).build();
    uriBase = miniCluster.getURI(0);

    final File tmpFile = File.createTempFile("hdfsHandlerTest", "data");
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
  public static void tearDownStatic()
  {
    if (miniCluster != null) {
      miniCluster.getNameNode(0).stop();
    }
  }

  @Test
  public void testSimpleUrl() throws IOException
  {
    final URL url = new URL(uriBase.toString() + filePath.toString());
    final File file = File.createTempFile("hdfsHandlerTest", "data");
    file.delete();
    file.deleteOnExit();
    try {
      try (InputStream stream = url.openStream()) {
        Files.copy(stream, file.toPath());
      }
      Assert.assertArrayEquals(pathByteContents, Files.readAllBytes(file.toPath()));
    }
    finally {
      file.delete();
    }
  }

  @Test(expected = java.io.IOException.class)
  public void testBadUrl() throws IOException
  {
    final URL url = new URL(uriBase.toString() + "/dsfjkafhdsajklfhdjlkshfjhasfhaslhfasjk");
    try (InputStream stream = url.openStream()) {

    }
  }
}

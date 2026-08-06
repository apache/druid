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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.regex.Pattern;

public class LocalFileTimestampVersionFinderTest
{
  @TempDir
  public File temporaryFolder;
  private File tmpDir;
  private LocalFileTimestampVersionFinder finder;

  @BeforeEach
  public void setup() throws IOException
  {
    tmpDir = newFolder(temporaryFolder, "junit");
    finder = new LocalFileTimestampVersionFinder();
  }

  @Test
  public void testSimpleLatestVersion() throws IOException, InterruptedException
  {
    File oldFile = File.createTempFile("old", ".txt", tmpDir);
    oldFile.createNewFile();
    Thread.sleep(1_000); // In order to roll over to the next unix second
    File newFile = File.createTempFile("new", ".txt", tmpDir);
    newFile.createNewFile();
    Assertions.assertTrue(oldFile.exists());
    Assertions.assertTrue(newFile.exists());
    Assertions.assertNotEquals(oldFile.lastModified(), newFile.lastModified());
    Assertions.assertEquals(oldFile.getParentFile(), newFile.getParentFile());
    Assertions.assertEquals(
        newFile.getAbsolutePath(),
        finder.getLatestVersion(oldFile.toURI(), Pattern.compile(".*\\.txt")).getPath()
    );
  }

  @Test
  public void testSimpleOneFileLatestVersion() throws IOException
  {
    File oldFile = File.createTempFile("old", ".txt", tmpDir);
    Assertions.assertTrue(oldFile.exists());
    Assertions.assertEquals(
        oldFile.getAbsolutePath(),
        finder.getLatestVersion(oldFile.toURI(), Pattern.compile(".*\\.txt")).getPath()
    );
  }

  @Test
  public void testSimpleOneFileLatestVersionNullMatcher() throws IOException
  {
    File oldFile = File.createTempFile("old", ".txt", tmpDir);
    Assertions.assertTrue(oldFile.exists());
    Assertions.assertEquals(
        oldFile.getAbsolutePath(),
        finder.getLatestVersion(oldFile.toURI(), null).getPath()
    );
  }

  @Test
  public void testNoLatestVersion() throws IOException
  {
    File oldFile = File.createTempFile("test", ".txt", tmpDir);
    oldFile.delete();
    URI uri = oldFile.toURI();
    Assertions.assertNull(
        finder.getLatestVersion(uri, Pattern.compile(".*\\.txt"))
    );
  }

  @Test
  public void testLatestVersionInDir() throws IOException, InterruptedException
  {
    File oldFile = File.createTempFile("old", ".txt", tmpDir);
    oldFile.createNewFile();
    Thread.sleep(1_000); // In order to roll over to the next unix second
    File newFile = File.createTempFile("new", ".txt", tmpDir);
    newFile.createNewFile();
    Assertions.assertTrue(oldFile.exists());
    Assertions.assertTrue(newFile.exists());
    Assertions.assertEquals(
        newFile.getAbsolutePath(),
        finder.getLatestVersion(oldFile.getParentFile().toURI(), Pattern.compile(".*\\.txt")).getPath()
    );
  }

  @Test
  public void testExampleRegex() throws IOException
  {
    File tmpFile = new File(tmpDir, "renames-123.gz");
    tmpFile.createNewFile();
    Assertions.assertTrue(tmpFile.exists());
    Assertions.assertFalse(tmpFile.isDirectory());
    Assertions.assertEquals(
        tmpFile.getAbsolutePath(),
        finder.getLatestVersion(tmpDir.toURI(), Pattern.compile("renames-[0-9]*\\.gz")).getPath()
    );
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

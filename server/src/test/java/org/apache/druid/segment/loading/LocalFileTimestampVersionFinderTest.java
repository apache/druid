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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.regex.Pattern;

public class LocalFileTimestampVersionFinderTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File tmpDir;
  private LocalFileTimestampVersionFinder finder;

  @Before
  public void setup() throws IOException
  {
    tmpDir = temporaryFolder.newFolder();
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
    Assert.assertTrue(oldFile.exists());
    Assert.assertTrue(newFile.exists());
    Assert.assertNotEquals(oldFile.lastModified(), newFile.lastModified());
    Assert.assertEquals(oldFile.getParentFile(), newFile.getParentFile());
    Assert.assertEquals(
        newFile.getAbsolutePath(),
        finder.getLatestVersion(oldFile.toURI(), Pattern.compile(".*\\.txt")).getPath()
    );
  }

  @Test
  public void testSimpleOneFileLatestVersion() throws IOException
  {
    File oldFile = File.createTempFile("old", ".txt", tmpDir);
    Assert.assertTrue(oldFile.exists());
    Assert.assertEquals(
        oldFile.getAbsolutePath(),
        finder.getLatestVersion(oldFile.toURI(), Pattern.compile(".*\\.txt")).getPath()
    );
  }

  @Test
  public void testSimpleOneFileLatestVersionNullMatcher() throws IOException
  {
    File oldFile = File.createTempFile("old", ".txt", tmpDir);
    Assert.assertTrue(oldFile.exists());
    Assert.assertEquals(
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
    Assert.assertNull(
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
    Assert.assertTrue(oldFile.exists());
    Assert.assertTrue(newFile.exists());
    Assert.assertEquals(
        newFile.getAbsolutePath(),
        finder.getLatestVersion(oldFile.getParentFile().toURI(), Pattern.compile(".*\\.txt")).getPath()
    );
  }

  @Test
  public void testExampleRegex() throws IOException
  {
    File tmpFile = new File(tmpDir, "renames-123.gz");
    tmpFile.createNewFile();
    Assert.assertTrue(tmpFile.exists());
    Assert.assertFalse(tmpFile.isDirectory());
    Assert.assertEquals(
        tmpFile.getAbsolutePath(),
        finder.getLatestVersion(tmpDir.toURI(), Pattern.compile("renames-[0-9]*\\.gz")).getPath()
    );
  }
}

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

package org.apache.druid.testing;

import org.apache.druid.java.util.common.FileUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;

/**
 * JUnit 5 extension that creates a temporary folder and removes it after the configured scope.
 * Register it as follows:
 *
 * <pre>{@code
 * // A directory shared by all tests in the class.
 * @RegisterExtension
 * public static final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.classScoped();
 *
 * // A fresh directory for each test.
 * @RegisterExtension
 * public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.perTest();
 * }</pre>
 */
public class TemporaryFolderExtension implements BeforeAllCallback, BeforeEachCallback, AfterAllCallback, AfterEachCallback
{
  private enum Scope
  {
    CLASS,
    TEST
  }

  private final File parentDirectory;
  private final Scope scope;
  private File root;

  public TemporaryFolderExtension()
  {
    this(null, Scope.TEST);
  }

  public TemporaryFolderExtension(final File parentDirectory)
  {
    this(parentDirectory, Scope.TEST);
  }

  public static TemporaryFolderExtension classScoped()
  {
    return new TemporaryFolderExtension(null, Scope.CLASS);
  }

  public static TemporaryFolderExtension perTest()
  {
    return new TemporaryFolderExtension(null, Scope.TEST);
  }

  private TemporaryFolderExtension(final File parentDirectory, final Scope scope)
  {
    this.parentDirectory = parentDirectory;
    this.scope = scope;
  }

  @Override
  public void beforeAll(final ExtensionContext context) throws IOException
  {
    if (scope == Scope.CLASS) {
      create();
    }
  }

  @Override
  public void beforeEach(final ExtensionContext context) throws IOException
  {
    if (scope == Scope.TEST) {
      create();
    }
  }

  @Override
  public void afterAll(final ExtensionContext context) throws IOException
  {
    if (scope == Scope.CLASS) {
      delete();
    }
  }

  @Override
  public void afterEach(final ExtensionContext context) throws IOException
  {
    if (scope == Scope.TEST) {
      delete();
    }
  }

  public void create() throws IOException
  {
    if (root == null) {
      root = parentDirectory == null
             ? FileUtils.createTempDir("junit")
             : FileUtils.createTempDirInLocation(parentDirectory.toPath(), "junit");
    }
  }

  public File getRoot()
  {
    ensureCreated();
    return root;
  }

  public File newFolder() throws IOException
  {
    ensureCreated();
    return FileUtils.createTempDirInLocation(root.toPath(), "junit");
  }

  public File newFolder(final String... folderNames) throws IOException
  {
    ensureCreated();
    File folder = root;
    for (final String folderName : folderNames) {
      folder = new File(folder, folderName);
    }
    FileUtils.mkdirp(folder);
    return folder;
  }

  public File newFile() throws IOException
  {
    ensureCreated();
    return File.createTempFile("junit", null, root);
  }

  public File newFile(final String fileName) throws IOException
  {
    ensureCreated();
    final File file = new File(root, fileName);
    if (!file.createNewFile()) {
      throw new IOException("Unable to create temporary file " + file);
    }
    return file;
  }

  public void delete() throws IOException
  {
    if (root != null) {
      FileUtils.deleteDirectory(root);
      root = null;
    }
  }

  private void ensureCreated()
  {
    try {
      create();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

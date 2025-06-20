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

package org.apache.druid.testing.simulate;

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;

import java.io.File;
import java.io.IOException;

/**
 * Modeled after JUnit {@code TemporaryFolder} but uses Druid {@link FileUtils}.
 */
public class TestFolder implements EmbeddedResource
{
  private File rootFolder;

  @Override
  public void start()
  {
    rootFolder = FileUtils.createTempDir("druid-simulation");
  }

  @Override
  public void stop() throws Exception
  {
    if (rootFolder != null) {
      FileUtils.deleteDirectory(rootFolder);
    }
  }

  public File newFolder()
  {
    validateRootFolderInitialized();
    return FileUtils.createTempDirInLocation(rootFolder.toPath(), null);
  }

  public File getOrCreateFolder(String name)
  {
    validateRootFolderInitialized();
    try {
      final File folder = new File(rootFolder, name);
      FileUtils.mkdirp(folder);
      return folder;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void validateRootFolderInitialized()
  {
    if (rootFolder == null) {
      throw new ISE("Root folder is not initialized yet.");
    }
  }
}

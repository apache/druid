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

package org.apache.druid.storage.local;

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.StorageConnector;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;

/**
 * Implementation that uses local filesystem. All paths are appended with the base path, in such a way that its not visible
 * to the users of this class.
 */
public class LocalFileStorageConnector implements StorageConnector
{

  private final File basePath;

  public LocalFileStorageConnector(File basePath) throws IOException
  {
    this.basePath = basePath;
    FileUtils.mkdirp(basePath);
  }

  @Override
  public boolean pathExists(String path)
  {
    return fileWithBasePath(path).exists();
  }

  /**
   * Reads the file present as basePath + path. Will throw an IO exception in case the file is not present.
   * Closing of the stream is the responsibility of the caller.
   *
   * @param path
   * @return
   * @throws IOException
   */
  @Override
  public InputStream read(String path) throws IOException
  {
    return Files.newInputStream(fileWithBasePath(path).toPath());
  }

  /**
   * Writes the file present with the materialized location as basePath + path.
   * In case the parent directory does not exist, we create the parent dir recursively.
   * Closing of the stream is the responsibility of the caller.
   *
   * @param path
   * @return
   * @throws IOException
   */
  @Override
  public OutputStream write(String path) throws IOException
  {
    File toWrite = fileWithBasePath(path);
    FileUtils.mkdirp(toWrite.getParentFile());
    return Files.newOutputStream(toWrite.toPath());
  }

  /**
   * Deletes the file present at the location basePath + path. Throws an excecption in case a dir is encountered.
   *
   * @param path
   * @throws IOException
   */
  @Override
  public void deleteFile(String path) throws IOException
  {
    File toDelete = fileWithBasePath(path);
    if (toDelete.isDirectory()) {
      throw new IAE(StringUtils.format(
          "Found a directory on path[%s]. Please use deleteRecusively to delete dirs", path));
    }
    Files.delete(fileWithBasePath(path).toPath());
  }

  /**
   * Deletes the files and sub dirs present at the basePath + dirName. Also removes the dirName
   *
   * @param dirName path
   * @throws IOException
   */
  @Override
  public void deleteRecursively(String dirName) throws IOException
  {
    FileUtils.deleteDirectory(fileWithBasePath(dirName));
  }

  public File getBasePath()
  {
    return basePath;
  }

  private File fileWithBasePath(String path)
  {
    return new File(basePath, path);
  }

}

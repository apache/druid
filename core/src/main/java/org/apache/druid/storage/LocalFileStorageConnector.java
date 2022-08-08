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

package org.apache.druid.storage;

import org.apache.druid.java.util.common.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LocalFileStorageConnector implements StorageConnector
{

  public final String basePath;

  public LocalFileStorageConnector(String basePath) throws IOException
  {
    this.basePath = basePath;
    FileUtils.mkdirp(new File(basePath));
  }

  @Override
  public boolean pathExists(String path)
  {
    return new File(objectPath(path)).exists();
  }

  @Override
  public InputStream read(String path) throws IOException
  {
    return Files.newInputStream(new File(objectPath(path)).toPath());
  }

  @Override
  public OutputStream write(String path) throws IOException
  {
    File toWrite = new File(objectPath(path));
    FileUtils.mkdirp(toWrite.getParentFile());
    return Files.newOutputStream(toWrite.toPath());
  }

  @Override
  public void delete(String path) throws IOException
  {
    Files.delete(new File(objectPath(path)).toPath());
  }

  @Override
  public void deleteRecursively(String dirName) throws IOException
  {
    FileUtils.deleteDirectory(new File(objectPath(dirName)));
  }

  private String objectPath(String path)
  {
    return Paths.get(basePath, path).toString();
  }

}

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

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.StorageConnector;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Implementation that uses local filesystem. All paths are appended with the base path, in such a way that it is not visible
 * to the users of this class.
 */
public class LocalFileStorageConnector implements StorageConnector
{
  private static final Logger log = new Logger(LocalFileStorageConnector.class);

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

  @Override
  public InputStream read(String path) throws IOException
  {
    return Files.newInputStream(fileWithBasePath(path).toPath());
  }

  @Override
  public InputStream readRange(String path, long from, long size) throws IOException
  {
    if (!pathExists(path)) {
      throw new FileNotFoundException("Unable to find file " + fileWithBasePath(path).toPath() + " for reading");
    }
    long length = fileWithBasePath(path).length();
    if (from < 0 || size < 0 || (from + size) > length) {
      throw new IAE(
          "Invalid arguments for reading %s. from = %d, readSize = %d, fileSize = %d",
          fileWithBasePath(path).toPath(),
          from,
          size,
          length
      );
    }
    FileChannel fileChannel = FileChannel.open(fileWithBasePath(path).toPath(), StandardOpenOption.READ);
    return new BoundedInputStream(Channels.newInputStream(fileChannel.position(from)), size);
  }

  /**
   * Writes the file present with the materialized location as basePath + path.
   * In case the parent directory does not exist, we create the parent dir recursively.
   * Closing of the stream is the responsibility of the caller.
   *
   * @param path path to write contents to.
   * @return OutputStream which can be used by callers to write contents.
   * @throws IOException thrown in case of errors.
   */
  @Override
  public OutputStream write(String path) throws IOException
  {
    File toWrite = fileWithBasePath(path);
    FileUtils.mkdirp(toWrite.getParentFile());
    return Files.newOutputStream(toWrite.toPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  /**
   * Deletes the file present at the location basePath + path. Throws an exception in case a dir is encountered.
   *
   * @param path input path
   * @throws IOException thrown in case of errors.
   */
  @Override
  public void deleteFile(String path) throws IOException
  {
    log.debug("Deleting file at path: [%s]", path);
    File toDelete = fileWithBasePath(path);
    if (toDelete.isDirectory()) {
      throw new IAE(StringUtils.format(
          "Found a directory on path[%s]. Please use deleteRecursively to delete dirs", path));
    }
    Files.delete(fileWithBasePath(path).toPath());
  }

  /**
   * Deletes the files present at each basePath + path. Throws an exception in case a dir is encountered.
   *
   * @param paths list of path to delete
   * @throws IOException thrown in case of errors.
   */
  @Override
  public void deleteFiles(Iterable<String> paths) throws IOException
  {
    for (String path : paths) {
      log.debug("Deleting file at path: [%s]", path);
      deleteFile(path);
    }
  }

  /**
   * Deletes the files and sub dirs present at the basePath + dirName. Also removes the dirName
   *
   * @param dirName path
   * @throws IOException thrown in case of errors.
   */
  @Override
  public void deleteRecursively(String dirName) throws IOException
  {
    log.debug("Deleting directory at path: [%s]", dirName);
    FileUtils.deleteDirectory(fileWithBasePath(dirName));
  }

  @Override
  public Iterator<String> listDir(String dirName)
  {
    File directory = fileWithBasePath(dirName);
    if (!directory.exists()) {
      throw new IAE("No directory exists on path [%s]", dirName);
    }
    if (!directory.isDirectory()) {
      throw new IAE("Cannot list contents of [%s] since it is not a directory", dirName);
    }
    File[] files = directory.listFiles();
    if (files == null) {
      throw new ISE("Unable to fetch the file list from the path [%s]", dirName);
    }
    return Arrays.stream(files).map(File::getName).iterator();
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

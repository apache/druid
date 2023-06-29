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

import org.apache.druid.guice.annotations.UnstableApi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * Low level interface for interacting with different storage providers like S3, GCS, Azure and local file system.
 * <p>
 * For adding a new implementation of this interface in your extension extend {@link StorageConnectorProvider}.
 * <p>
 * For using the interface in your extension as a consumer, use JsonConfigProvider like:
 * <ol>
 * <li>{@code JsonConfigProvider.bind(binder, "druid.extension.custom.type", StorageConnectorProvider.class, Custom.class);}</li>
 * <li>// bind the storage config provider {@code binder.bind(Key.get(StorageConnector.class, Custom.class)).toProvider(Key.get(StorageConnectorProvider.class, Custom.class)).in(LazySingleton.class);} </li>
 * <li>// Use Named annotations to access the storageConnector instance in your custom extension.{@code @Custom StorageConnector storageConnector} </li>
 * </ol>
 * For end users, the runtime.properties file would look like
 * <ul>
 * <li>{@code druid.extension.custom.type="s3"}
 * <li>{@code druid.extension.custom.bucket="myBucket"}
 * </ul>
 * The final state of this inteface would have
 * <ol>
 * <li>Future Non blocking API's</li>
 * <li>Offset based fetch</li>
 * </ol>
 */

@UnstableApi
public interface StorageConnector
{

  /**
   * Check if the path exists in the underlying storage layer. Most implementations prepend the input path
   * with a basePath.
   *
   * @param path
   * @return true if path exists else false.
   * @throws IOException
   */
  @SuppressWarnings("all")
  boolean pathExists(String path) throws IOException;

  /**
   * Reads the data present at the path in the underlying storage system. Most implementations prepend the input path
   * with a basePath.
   * The caller should take care of closing the stream when done or in case of error.
   *
   * @param path
   * @return InputStream
   * @throws IOException if the path is not present or the unable to read the data present on the path.
   */
  InputStream read(String path) throws IOException;

  /**
   * Reads the data present for a given range at the path in the underlying storage system.
   * Most implementations prepend the input path with a basePath.
   * The caller should take care of closing the stream when done or in case of error. Further, the caller must ensure
   * that the start offset and the size of the read are valid parameters for the given path for correct behavior.
   * @param path The path to read data from
   * @param from Start offset of the read in the path
   * @param size Length of the read to be done
   * @return InputStream starting from the given offset limited by the given size
   * @throws IOException if the path is not present or the unable to read the data present on the path
   */
  InputStream readRange(String path, long from, long size) throws IOException;

  /**
   * Open an {@link OutputStream} for writing data to the path in the underlying storage system.
   * Most implementations prepend the input path with a basePath.
   * Callers are adivised to namespace there files as there might be race conditions.
   * The caller should take care of closing the stream when done or in case of error.
   *
   * @param path
   * @return
   * @throws IOException
   */
  OutputStream write(String path) throws IOException;

  /**
   * Delete file present at the input path. Most implementations prepend the input path
   * with a basePath.
   * If the path is a directory, this method throws an exception.
   *
   * @param path to delete
   * @throws IOException thrown in case of errors.
   */
  void deleteFile(String path) throws IOException;


  /**
   * Delete files present at the input paths. Most implementations prepend all the input paths
   * with the basePath.
   * <br/>
   * This method is <b>recommended</b> in case we need to delete a batch of files.
   * If the path is a directory, this method throws an exception.
   *
   * @param paths Iterable of the paths to delete.
   * @throws IOException thrown in case of errors.
   */
  void deleteFiles(Iterable<String> paths) throws IOException;

  /**
   * Delete a directory pointed to by the path and also recursively deletes all files/directories in said directory.
   * Most implementations prepend the input path with a basePath.
   *
   * @param path path
   * @throws IOException thrown in case of errors.
   */
  void deleteRecursively(String path) throws IOException;

  /**
   * Returns a lazy iterator containing all the files present in the path. The returned filenames should be such that joining
   * the dirName and the file name form the full path that can be used as the arguments for other methods of the storage
   * connector.
   * For example, for a S3 path such as s3://bucket/parent1/parent2/child, the filename returned for the input path
   * "parent1/parent2" should be "child" and for input "parent1" should be "parent2/child"
   */
  Iterator<String> listDir(String dirName) throws IOException;
}

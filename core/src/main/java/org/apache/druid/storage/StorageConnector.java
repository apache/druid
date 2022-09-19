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
   * Reads the data present at the path the underlying storage system. Most implementations prepend the input path
   * with a basePath.
   * The caller should take care of closing the stream when done or in case of error.
   *
   * @param path
   * @return InputStream
   * @throws IOException if the path is not present or the unable to read the data present on the path.
   */
  InputStream read(String path) throws IOException;

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
   * @param path
   * @throws IOException
   */
  void deleteFile(String path) throws IOException;

  /**
   * Delete a directory pointed to by the path and also recursively deletes all files/directories in said directory.
   * Most implementations prepend the input path with a basePath.
   *
   * @param path path
   * @throws IOException
   */
  void deleteRecursively(String path) throws IOException;
}

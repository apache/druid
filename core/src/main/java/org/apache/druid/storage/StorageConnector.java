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
 * For adding a new implementation of this interface in your extension extend {@link StorageConnectorProvider}.
 * For using the interface in your extension as a consumer, use JsonConfigProvider like:
 * 1. JsonConfigProvider.bind(binder, "druid,extension.custom.type", StorageConnectorProvider.class, Custom.class);
 * // bind the storage config provider
 * 2. binder.bind(Key.get(StorageConnector.class, Custom.class)).toProvider(Key.get(StorageConnectorProvider.class, Custom.class)).in(LazySingleton.class);
 * // Used Named annotations to access the storageConnector instance in your custom extension.
 * 3. @Custom StorageConnector storageConnector
 * <p>
 * The final state of this inteface would have
 * * 1. Future Non blocking API's
 * * 2. Offset based fetch
 */

@UnstableApi
public interface StorageConnector
{

  /**
   * Check if the relative path exists in the underlying storage layer.
   *
   * @param path
   * @return true if path exists else false.
   * @throws IOException
   */
  @SuppressWarnings("all")
  boolean pathExists(String path) throws IOException;

  /**
   * Reads the data present at the relative path from the underlying storage system.
   * The caller should take care of closing the stream when done or in case of error.
   *
   * @param path
   * @return InputStream
   * @throws IOException if the path is not present or the unable to read the data present on the path.
   */
  InputStream read(String path) throws IOException;

  /**
   * Open an {@link OutputStream} for writing data to the relative path in the underlying storage system.
   * The caller should take care of closing the stream when done or in case of error.
   *
   * @param path
   * @return
   * @throws IOException
   */
  OutputStream write(String path) throws IOException;

  /**
   * Delete file present at the relative path.
   *
   * @param path
   * @throws IOException
   */
  @SuppressWarnings("all")
  void delete(String path) throws IOException;

  /**
   * Delete a directory pointed to by the path and also recursively deletes all files in said directory.
   *
   * @param path path
   * @throws IOException
   */
  void deleteRecursively(String path) throws IOException;
}

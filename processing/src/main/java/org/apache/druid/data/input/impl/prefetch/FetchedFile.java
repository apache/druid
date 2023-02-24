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

package org.apache.druid.data.input.impl.prefetch;

import java.io.Closeable;
import java.io.File;

/**
 * A class containing meta information about fetched objects.  This class used by {@link Fetcher}.
 */
class FetchedFile<T>
{
  // Original object
  private final T object;
  // Fetched file stored in local disk
  private final File file;
  // Closer which is called when the file is not needed anymore. Usually this deletes the file except for cached files.
  private final Closeable resourceCloser;

  FetchedFile(T object, File file, Closeable resourceCloser)
  {
    this.object = object;
    this.file = file;
    this.resourceCloser = resourceCloser;
  }

  long length()
  {
    return file.length();
  }

  T getObject()
  {
    return object;
  }

  File getFile()
  {
    return file;
  }

  Closeable getResourceCloser()
  {
    return resourceCloser;
  }

  FetchedFile<T> cache()
  {
    return new FetchedFile<>(object, file, () -> {});
  }
}

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

package org.apache.druid.data.input;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * InputEntity abstracts an input entity and knows how to read bytes from the given entity.
 * Since the implementations of this interface assume that the given entity is not empty, the InputSources
 * should not create InputEntities for empty entities.
 */
@UnstableApi
public interface InputEntity
{
  Logger LOG = new Logger(InputEntity.class);

  int DEFAULT_FETCH_BUFFER_SIZE = 4 * 1024; // 4 KB
  int DEFAULT_MAX_NUM_FETCH_TRIES = 3; // 3 tries including the initial try

  /**
   * CleanableFile is the result type of {@link #fetch}.
   * It should clean up any temporary resource on {@link #close()}.
   */
  interface CleanableFile extends Closeable
  {
    File file();
  }

  /**
   * Returns an URI to identify the input entity. Implementations can return null if they don't have
   * an unique URI.
   */
  @Nullable
  URI getUri();

  /**
   * Opens an {@link InputStream} on the input entity directly.
   * This is the basic way to read the given entity.
   *
   * The behavior of this method is only defined fort the first call to open().
   * The behavior of subsequent calls is undefined and may vary between implementations.
   *
   * @see #fetch
   */
  InputStream open() throws IOException;

  /**
   * Fetches the input entity into the local storage.
   * This method might be preferred instead of {@link #open()}, for example
   *
   * - {@link InputFormat} requires expensive random access on remote storage.
   * - Holding a connection until you consume the entire InputStream is expensive.
   *
   * @param temporaryDirectory to store temp data. This directory will be removed automatically once
   *                           the task finishes.
   * @param fetchBuffer        is used to fetch remote entity into local storage.
   *
   * @see FileUtils#copyLarge
   */
  default CleanableFile fetch(File temporaryDirectory, byte[] fetchBuffer) throws IOException
  {
    final File tempFile = File.createTempFile("druid-input-entity", ".tmp", temporaryDirectory);
    LOG.debug("Fetching entity into file[%s]", tempFile.getAbsolutePath());
    try (InputStream is = open()) {
      FileUtils.copyLarge(
          is,
          tempFile,
          fetchBuffer,
          getRetryCondition(),
          DEFAULT_MAX_NUM_FETCH_TRIES,
          StringUtils.format("Failed to fetch into [%s]", tempFile.getAbsolutePath())
      );
    }

    return new CleanableFile()
    {
      @Override
      public File file()
      {
        return tempFile;
      }

      @Override
      public void close()
      {
        if (!tempFile.delete()) {
          LOG.warn("Failed to remove file[%s]", tempFile.getAbsolutePath());
        }
      }
    };
  }

  /**
   * Returns a retry condition that the caller should retry on.
   * The returned condition should be used when reading data from this InputEntity such as in {@link #fetch}
   * or {@link RetryingInputEntity}.
   */
  default Predicate<Throwable> getRetryCondition()
  {
    return Predicates.alwaysFalse();
  }
}

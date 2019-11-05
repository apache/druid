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
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * SplitSource abstracts an {@link InputSplit} and knows how to read bytes from the given split.
 */
@ExtensionPoint
public interface SplitSource<T>
{
  Logger LOG = new Logger(SplitSource.class);

  int DEFAULT_FETCH_BUFFER_SIZE = 4 * 1024; // 4 KB
  int DEFAULT_MAX_FETCH_RETRY = 2; // 3 tries including the initial try

  /**
   * CleanableFile is the result type of {@link #fetch}.
   * It should clean up any temporary resource on {@link #close()}.
   */
  interface CleanableFile extends Closeable
  {
    File file();
  }

  InputSplit<T> getSplit();

  /**
   * Opens an {@link InputStream} on the split directly.
   * This is the basic way to read the given split.
   *
   * @see #fetch as an alternative way to read data.
   */
  InputStream open() throws IOException;

  /**
   * Fetches the split into the local storage.
   * This method might be preferred instead of {@link #open()}, for example
   *
   * - {@link org.apache.druid.data.input.impl.InputFormat} requires expensive random access on remote storage.
   * - Holding a connection until you consume the entire InputStream is expensive.
   *
   * @param temporaryDirectory to store temp data. This directory will be removed automatically once
   *                           the task finishes.
   * @param fetchBuffer        is used to fetch remote split into local storage.
   *
   * @see FileUtils#copyLarge
   */
  default CleanableFile fetch(File temporaryDirectory, byte[] fetchBuffer) throws IOException
  {
    final File tempFile = File.createTempFile("druid-split", ".tmp", temporaryDirectory);
    LOG.debug("Fetching split into file[%s]", tempFile.getAbsolutePath());
    FileUtils.copyLarge(
        open(),
        tempFile,
        fetchBuffer,
        getRetryCondition(),
        DEFAULT_MAX_FETCH_RETRY,
        StringUtils.format("Failed to fetch into [%s]", tempFile.getAbsolutePath())
    );

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
   * {@link #fetch} will retry during the fetch if it sees an exception mathing to the returned predicate.
   */
  Predicate<Throwable> getRetryCondition();
}

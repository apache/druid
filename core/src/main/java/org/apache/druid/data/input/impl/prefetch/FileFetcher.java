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

import com.google.common.base.Predicate;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * A file fetcher used by {@link PrefetchableTextFilesFirehoseFactory}.
 * See the javadoc of {@link PrefetchableTextFilesFirehoseFactory} for more details.
 */
public class FileFetcher<T> extends Fetcher<T>
{
  private static final int BUFFER_SIZE = 1024 * 4;
  private final ObjectOpenFunction<T> openObjectFunction;
  private final Predicate<Throwable> retryCondition;
  private final byte[] buffer;
  // maximum retry for fetching an object from the remote site
  private final int maxFetchRetry;

  private int getMaxFetchRetry()
  {
    return maxFetchRetry;
  }

  FileFetcher(
      CacheManager<T> cacheManager,
      List<T> objects,
      ExecutorService fetchExecutor,
      @Nullable File temporaryDirectory,
      PrefetchConfig prefetchConfig,
      ObjectOpenFunction<T> openObjectFunction,
      Predicate<Throwable> retryCondition,
      int maxFetchRetries
  )
  {

    super(
        cacheManager,
        objects,
        fetchExecutor,
        temporaryDirectory,
        prefetchConfig
    );

    this.openObjectFunction = openObjectFunction;
    this.retryCondition = retryCondition;
    this.buffer = new byte[BUFFER_SIZE];
    this.maxFetchRetry = maxFetchRetries;
  }

  /**
   * Downloads an object. It retries downloading {@link #maxFetchRetry}
   * times and throws an exception.
   *
   * @param object  an object to be downloaded
   * @param outFile a file which the object data is stored
   *
   * @return number of downloaded bytes
   */
  @Override
  protected long download(T object, File outFile) throws IOException
  {
    return FileUtils.copyLarge(
        object,
        openObjectFunction,
        outFile,
        buffer,
        retryCondition,
        maxFetchRetry + 1,
        StringUtils.format("Failed to download object[%s]", object)
    );
  }

  /**
   * Generates an instance of {@link OpenedObject} for which the underlying stream may be re-opened and retried
   * based on the exception and retry condition.
   */
  @Override
  protected OpenedObject<T> generateOpenObject(T object) throws IOException
  {
    return new OpenedObject<>(
        object,
        new RetryingInputStream<>(object, openObjectFunction, retryCondition, getMaxFetchRetry()),
        getNoopCloser()
    );
  }

  private static Closeable getNoopCloser()
  {
    return () -> {
    };
  }
}

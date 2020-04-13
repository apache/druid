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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * A class managing cached files used by {@link PrefetchableTextFilesFirehoseFactory}.
 */
public class CacheManager<T>
{
  private static final Logger LOG = new Logger(CacheManager.class);

  // A roughly max size of total cached objects which means the actual cached size can be bigger. The reason is our
  // current client implementations for cloud storages like s3 don't support range scan yet, so we must download the
  // whole file at once. It's still possible for the size of cached data to not exceed these variables by estimating the
  // after-fetch size, but it makes us to consider the case when any files cannot be fetched due to their large size,
  // which makes the implementation complicated.
  private final long maxCacheCapacityBytes;

  private final List<FetchedFile<T>> files = new ArrayList<>();

  private long totalCachedBytes;

  public CacheManager(long maxCacheCapacityBytes)
  {
    this.maxCacheCapacityBytes = maxCacheCapacityBytes;
  }

  public boolean isEnabled()
  {
    return maxCacheCapacityBytes > 0;
  }

  public boolean cacheable()
  {
    // maxCacheCapacityBytes is a rough limit, so if totalCachedBytes is larger than it, no more caching is
    // allowed.
    return totalCachedBytes < maxCacheCapacityBytes;
  }

  FetchedFile<T> cache(FetchedFile<T> fetchedFile)
  {
    if (!cacheable()) {
      throw new ISE(
          "Cache space is full. totalCachedBytes[%d], maxCacheCapacityBytes[%d]",
          totalCachedBytes,
          maxCacheCapacityBytes
      );
    }

    final FetchedFile<T> cachedFile = fetchedFile.cache();
    files.add(cachedFile);
    totalCachedBytes += cachedFile.length();

    LOG.info("Object[%s] is cached. Current cached bytes is [%d]", cachedFile.getObject(), totalCachedBytes);
    return cachedFile;
  }

  List<FetchedFile<T>> getFiles()
  {
    return files;
  }

  @VisibleForTesting
  long getTotalCachedBytes()
  {
    return totalCachedBytes;
  }

  public long getMaxCacheCapacityBytes()
  {
    return maxCacheCapacityBytes;
  }
}

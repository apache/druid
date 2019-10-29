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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A file fetcher used by {@link PrefetchableTextFilesFirehoseFactory} and PrefetchSqlFirehoseFactory (in druid-server).
 * See the javadoc of {@link PrefetchableTextFilesFirehoseFactory} for more details.
 */
public abstract class Fetcher<T> implements Iterator<OpenObject<T>>
{
  private static final Logger LOG = new Logger(Fetcher.class);
  private static final String FETCH_FILE_PREFIX = "fetch-";
  private final CacheManager<T> cacheManager;
  private final List<T> objects;
  private final ExecutorService fetchExecutor;

  @Nullable
  private final File temporaryDirectory;

  private final boolean prefetchEnabled;

  private final LinkedBlockingQueue<FetchedFile<T>> fetchedFiles = new LinkedBlockingQueue<>();

  // Number of bytes of current fetched files.
  // This is updated when a file is successfully fetched, a fetched file is deleted, or a fetched file is
  // cached.
  private final AtomicLong fetchedBytes = new AtomicLong(0);
  private final Deque<Future<Void>> fetchFutures = new ArrayDeque<>();
  private final FetchConfig fetchConfig;

  // nextFetchIndex indicates which object should be downloaded when fetch is triggered.
  // This variable is always read by the same thread regardless of prefetch is enabled or not.
  private int nextFetchIndex;

  private int numRemainingObjects;

  public Fetcher(
      CacheManager<T> cacheManager,
      List<T> objects,
      ExecutorService fetchExecutor,
      @Nullable File temporaryDirectory,
      FetchConfig fetchConfig
  )
  {
    this.cacheManager = cacheManager;
    this.objects = objects;
    this.fetchExecutor = fetchExecutor;
    this.temporaryDirectory = temporaryDirectory;
    this.fetchConfig = fetchConfig;
    this.prefetchEnabled = fetchConfig.getMaxFetchCapacityBytes() > 0;
    this.numRemainingObjects = objects.size();

    // (*) If cache is initialized, put all cached files to the queue.
    this.fetchedFiles.addAll(cacheManager.getFiles());
    this.nextFetchIndex = fetchedFiles.size();
    if (cacheManager.isEnabled() || prefetchEnabled) {
      Preconditions.checkNotNull(temporaryDirectory, "temporaryDirectory");
    }
    if (prefetchEnabled) {
      fetchIfNeeded(0L);
    }
  }

  /**
   * Submit a fetch task if remainingBytes is smaller than prefetchTriggerBytes.
   */
  private void fetchIfNeeded(long remainingBytes)
  {
    if ((fetchFutures.isEmpty() || fetchFutures.peekLast().isDone())
        && remainingBytes <= fetchConfig.getPrefetchTriggerBytes()) {
      Future<Void> fetchFuture = fetchExecutor.submit(() -> {
        fetch();
        return null;
      });
      fetchFutures.add(fetchFuture);
    }
  }

  /**
   * Fetch objects to a local disk up to {@link FetchConfig#maxFetchCapacityBytes}.
   * This method is not thread safe and must be called by a single thread.  Note that even
   * {@link FetchConfig#maxFetchCapacityBytes} is 0, at least 1 file is always fetched.
   * This is for simplifying design, and should be improved when our client implementations for cloud storages
   * like S3 support range scan.
   * <p>
   * This method is called by {@link #fetchExecutor} if prefetch is enabled.  Otherwise, it is called by the same
   * thread.
   */
  private void fetch() throws Exception
  {
    for (; nextFetchIndex < objects.size()
           && fetchedBytes.get() <= fetchConfig.getMaxFetchCapacityBytes(); nextFetchIndex++) {
      final T object = objects.get(nextFetchIndex);
      LOG.info("Fetching [%d]th object[%s], fetchedBytes[%d]", nextFetchIndex, object, fetchedBytes.get());
      final File outFile = File.createTempFile(FETCH_FILE_PREFIX, null, temporaryDirectory);
      fetchedBytes.addAndGet(download(object, outFile));
      fetchedFiles.put(new FetchedFile<>(object, outFile, getFileCloser(outFile, fetchedBytes)));
    }
  }

  /**
   * Downloads an object into a file. The download process could be retried depending on the object source.
   *
   * @param object  an object to be downloaded
   * @param outFile a file which the object data is stored
   *
   * @return number of downloaded bytes
   */
  protected abstract long download(T object, File outFile) throws IOException;

  /**
   * Generates an instance of {@link OpenObject} for the given object.
   */
  protected abstract OpenObject<T> generateOpenObject(T object) throws IOException;

  protected FetchConfig getFetchConfig()
  {
    return fetchConfig;
  }

  @Override
  public boolean hasNext()
  {
    return numRemainingObjects > 0;
  }

  @Override
  public OpenObject<T> next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    // If fetch() fails, hasNext() always returns true and next() is always called. The below method checks that
    // fetch() threw an exception and propagates it if exists.
    checkFetchException(false);

    try {
      final OpenObject<T> openObject = prefetchEnabled ? openObjectFromLocal() : openObjectFromRemote();
      numRemainingObjects--;
      return openObject;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void checkFetchException(boolean wait)
  {
    try {
      for (Future<Void> fetchFuture; (fetchFuture = fetchFutures.poll()) != null; ) {
        if (wait) {
          fetchFuture.get(fetchConfig.getFetchTimeout(), TimeUnit.MILLISECONDS);
        } else {
          if (fetchFuture.isDone()) {
            fetchFuture.get();
          } else {
            fetchFutures.addFirst(fetchFuture);
            break;
          }
        }
      }
    }
    catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    catch (TimeoutException e) {
      throw new ISE(e, "Failed to fetch, but cannot check the reason in [%d] ms", fetchConfig.getFetchTimeout());
    }
  }

  private OpenObject<T> openObjectFromLocal() throws IOException
  {
    final FetchedFile<T> fetchedFile;

    if (!fetchedFiles.isEmpty()) {
      // If there are already fetched files, use them
      fetchedFile = fetchedFiles.poll();
    } else {
      // Otherwise, wait for fetching
      try {
        fetchIfNeeded(fetchedBytes.get());
        fetchedFile = fetchedFiles.poll(fetchConfig.getFetchTimeout(), TimeUnit.MILLISECONDS);
        if (fetchedFile == null) {
          // Check the latest fetch is failed
          checkFetchException(true);
          // Or throw a timeout exception
          throw new RuntimeException(new TimeoutException());
        }
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    final FetchedFile<T> maybeCached = cacheIfPossible(fetchedFile);
    // trigger fetch again for subsequent next() calls
    fetchIfNeeded(fetchedBytes.get());
    return new OpenObject<>(maybeCached);
  }

  private OpenObject<T> openObjectFromRemote() throws IOException
  {
    if (fetchedFiles.size() > 0) {
      // If fetchedFiles is not empty even though prefetching is disabled, they should be cached files.
      // We use them first. See (*).
      return new OpenObject<>(fetchedFiles.poll());
    } else if (cacheManager.cacheable()) {
      // If cache is enabled, first download an object to local storage and cache it.
      try {
        // Since maxFetchCapacityBytes is 0, at most one file is fetched.
        fetch();
        FetchedFile<T> fetchedFile = fetchedFiles.poll();
        if (fetchedFile == null) {
          throw new ISE("Cannot fetch object[%s]", objects.get(nextFetchIndex - 1));
        }
        final FetchedFile<T> cached = cacheIfPossible(fetchedFile);
        return new OpenObject<>(cached);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      final T object = objects.get(nextFetchIndex);
      LOG.info("Reading [%d]th object[%s]", nextFetchIndex, object);
      nextFetchIndex++;
      return generateOpenObject(object);
    }
  }

  private FetchedFile<T> cacheIfPossible(FetchedFile<T> fetchedFile)
  {
    if (cacheManager.cacheable()) {
      final FetchedFile<T> cachedFile = cacheManager.cache(fetchedFile);
      // If the fetchedFile is cached, make a room for fetching more data immediately.
      // This is because cache space and fetch space are separated.
      fetchedBytes.addAndGet(-fetchedFile.length());
      return cachedFile;
    } else {
      return fetchedFile;
    }
  }

  private static Closeable getFileCloser(
      final File file,
      final AtomicLong fetchedBytes
  )
  {
    return () -> {
      final long fileSize = file.length();
      file.delete();
      fetchedBytes.addAndGet(-fileSize);
    };
  }
}

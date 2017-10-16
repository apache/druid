/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.data.input.impl.prefetch;

import com.google.common.base.Throwables;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
 * A file fetcher used by {@link PrefetchableTextFilesFirehoseFactory}.
 * See the javadoc of {@link PrefetchableTextFilesFirehoseFactory} for more details.
 */
public class Fetcher<T> implements Iterator<OpenedObject<T>>
{
  private static final Logger LOG = new Logger(Fetcher.class);
  private static final String FETCH_FILE_PREFIX = "fetch-";
  private static final int BUFFER_SIZE = 1024 * 4;

  private final CacheManager<T> cacheManager;
  private final List<T> objects;
  private final ExecutorService fetchExecutor;
  private final File temporaryDirectory;

  // A roughly max size of total fetched objects, but the actual fetched size can be bigger. The reason is our current
  // client implementations for cloud storages like s3 don't support range scan yet, so we must download the whole file
  // at once. It's still possible for the size of cached/fetched data to not exceed these variables by estimating the
  // after-fetch size, but it makes us consider the case when any files cannot be fetched due to their large size, which
  // makes the implementation complicated.
  private final long maxFetchCapacityBytes;
  private final boolean prefetchEnabled;

  private final long prefetchTriggerBytes;

  // timeout for fetching an object from the remote site
  private final long fetchTimeout;

  // maximum retry for fetching an object from the remote site
  private final int maxFetchRetry;

  private final LinkedBlockingQueue<FetchedFile<T>> fetchedFiles = new LinkedBlockingQueue<>();

  // Number of bytes of current fetched files.
  // This is updated when a file is successfully fetched, a fetched file is deleted, or a fetched file is
  // cached.
  private final AtomicLong fetchedBytes = new AtomicLong(0);

  private final ObjectOpenFunction<T> openObjectFunction;
  private final byte[] buffer;

  private Future<Void> fetchFuture;

  // nextFetchIndex indicates which object should be downloaded when fetch is triggered.
  // This variable is always read by the same thread regardless of prefetch is enabled or not.
  private int nextFetchIndex;

  private int numRemainingObjects;

  Fetcher(
      CacheManager<T> cacheManager,
      List<T> objects,
      ExecutorService fetchExecutor,
      File temporaryDirectory,
      long maxFetchCapacityBytes,
      long prefetchTriggerBytes,
      long fetchTimeout,
      int maxFetchRetry,
      ObjectOpenFunction<T> openObjectFunction
  )
  {
    this.cacheManager = cacheManager;
    this.objects = objects;
    this.fetchExecutor = fetchExecutor;
    this.temporaryDirectory = temporaryDirectory;
    this.maxFetchCapacityBytes = maxFetchCapacityBytes;
    this.prefetchTriggerBytes = prefetchTriggerBytes;
    this.fetchTimeout = fetchTimeout;
    this.maxFetchRetry = maxFetchRetry;
    this.openObjectFunction = openObjectFunction;
    this.buffer = new byte[BUFFER_SIZE];

    this.prefetchEnabled = maxFetchCapacityBytes > 0;
    this.numRemainingObjects = objects.size();

    // (*) If cache is initialized, put all cached files to the queue.
    this.fetchedFiles.addAll(cacheManager.getFiles());
    this.nextFetchIndex = fetchedFiles.size();

    if (prefetchEnabled) {
      fetchIfNeeded(0L);
    }
  }

  /**
   * Submit a fetch task if remainingBytes is smaller than {@link #prefetchTriggerBytes}.
   */
  private void fetchIfNeeded(long remainingBytes)
  {
    if ((fetchFuture == null || fetchFuture.isDone())
        && remainingBytes <= prefetchTriggerBytes) {
      fetchFuture = fetchExecutor.submit(() -> {
        fetch();
        return null;
      });
    }
  }

  /**
   * Fetch objects to a local disk up to {@link PrefetchableTextFilesFirehoseFactory#maxFetchCapacityBytes}.
   * This method is not thread safe and must be called by a single thread.  Note that even
   * {@link PrefetchableTextFilesFirehoseFactory#maxFetchCapacityBytes} is 0, at least 1 file is always fetched.
   * This is for simplifying design, and should be improved when our client implementations for cloud storages
   * like S3 support range scan.
   *
   * This method is called by {@link #fetchExecutor} if prefetch is enabled.  Otherwise, it is called by the same
   * thread.
   */
  private void fetch() throws Exception
  {
    for (; nextFetchIndex < objects.size() && fetchedBytes.get() <= maxFetchCapacityBytes; nextFetchIndex++) {
      final T object = objects.get(nextFetchIndex);
      LOG.info("Fetching [%d]th object[%s], fetchedBytes[%d]", nextFetchIndex, object, fetchedBytes.get());
      final File outFile = File.createTempFile(FETCH_FILE_PREFIX, null, temporaryDirectory);
      fetchedBytes.addAndGet(download(object, outFile, 0));
      fetchedFiles.put(new FetchedFile<>(object, outFile, getFileCloser(outFile, fetchedBytes)));
    }
  }

  /**
   * Downloads an object. It retries downloading {@link PrefetchableTextFilesFirehoseFactory#maxFetchRetry}
   * times and throws an exception.
   *
   * @param object   an object to be downloaded
   * @param outFile  a file which the object data is stored
   * @param tryCount current retry count
   *
   * @return number of downloaded bytes
   */
  private long download(T object, File outFile, int tryCount) throws IOException
  {
    try (final InputStream is = openObjectFunction.open(object);
         final OutputStream os = new FileOutputStream(outFile)) {
      return IOUtils.copyLarge(is, os, buffer);
    }
    catch (IOException e) {
      final int nextTry = tryCount + 1;
      if (!Thread.currentThread().isInterrupted() && nextTry < maxFetchRetry) {
        LOG.error(e, "Failed to download object[%s], retrying (%d of %d)", object, nextTry, maxFetchRetry);
        outFile.delete();
        return download(object, outFile, nextTry);
      } else {
        LOG.error(e, "Failed to download object[%s], retries exhausted, aborting", object);
        throw e;
      }
    }
  }

  @Override
  public boolean hasNext()
  {
    return numRemainingObjects > 0;
  }

  @Override
  public OpenedObject<T> next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    // If fetch() fails, hasNext() always returns true and next() is always called. The below method checks that
    // fetch() threw an exception and propagates it if exists.
    checkFetchException(false);

    try {
      final OpenedObject<T> openedObject = prefetchEnabled ? openObjectFromLocal() : openObjectFromRemote();
      numRemainingObjects--;
      return openedObject;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void checkFetchException(boolean wait)
  {
    try {
      if (wait) {
        fetchFuture.get(fetchTimeout, TimeUnit.MILLISECONDS);
        fetchFuture = null;
      } else if (fetchFuture != null && fetchFuture.isDone()) {
        fetchFuture.get();
        fetchFuture = null;
      }
    }
    catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    catch (TimeoutException e) {
      throw new ISE(e, "Failed to fetch, but cannot check the reason in [%d] ms", fetchTimeout);
    }
  }

  private OpenedObject<T> openObjectFromLocal() throws IOException
  {
    final FetchedFile<T> fetchedFile;

    if (!fetchedFiles.isEmpty()) {
      // If there are already fetched files, use them
      fetchedFile = fetchedFiles.poll();
    } else {
      // Otherwise, wait for fetching
      try {
        fetchIfNeeded(fetchedBytes.get());
        fetchedFile = fetchedFiles.poll(fetchTimeout, TimeUnit.MILLISECONDS);
        if (fetchedFile == null) {
          // Check the latest fetch is failed
          checkFetchException(true);
          // Or throw a timeout exception
          throw new RuntimeException(new TimeoutException());
        }
      }
      catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
    final FetchedFile<T> maybeCached = cacheIfPossible(fetchedFile);
    // trigger fetch again for subsequent next() calls
    fetchIfNeeded(fetchedBytes.get());
    return new OpenedObject<>(maybeCached);
  }

  private OpenedObject<T> openObjectFromRemote() throws IOException
  {
    if (fetchedFiles.size() > 0) {
      // If fetchedFiles is not empty even though prefetching is disabled, they should be cached files.
      // We use them first. See (*).
      return new OpenedObject<>(fetchedFiles.poll());
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
        return new OpenedObject<>(cached);
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    } else {
      final T object = objects.get(nextFetchIndex);
      LOG.info("Reading [%d]th object[%s]", nextFetchIndex, object);
      nextFetchIndex++;
      return new OpenedObject<>(object, openObjectFunction.open(object), getNoopCloser());
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

  private static Closeable getNoopCloser()
  {
    return () -> {};
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

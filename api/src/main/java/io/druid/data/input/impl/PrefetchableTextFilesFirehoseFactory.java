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

package io.druid.data.input.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CountingOutputStream;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.druid.data.input.Firehose;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PrefetchableTextFilesFirehoseFactory is an abstract firehose factory for reading text files.  The firehose returned
 * by this class provides three key functionalities.
 *
 * <ul>
 *   <li>
 *     Caching: for the first call of {@link #connect(StringInputRowParser, File)}, it caches objects in a local disk
 *     up to {@link #maxCacheCapacityBytes}.  These caches are NOT deleted until the process terminates,
 *     and thus can be used for future reads.
 *   </li>
 *   <li>
 *     Fetching: when it reads all cached data, it fetches remaining objects into a local disk and reads data from
 *     them.  For the performance reason, prefetch technique is used, that is, when the size of remaining cached or
 *     fetched data is smaller than {@link #prefetchTriggerBytes}, a background prefetch thread automatically starts to
 *     fetch remaining objects.
 *   </li>
 *   <li>
 *     Retry: if an exception occurs while downloading an object, it retries again up to {@link #maxFetchRetry}.
 *   </li>
 * </ul>
 *
 * This implementation can be useful when the cost for reading input objects is large as reading from AWS S3 because
 * IndexTask can read the whole data twice for determining partition specs and generating segments if the intervals of
 * GranularitySpec is not specified.
 *
 * Prefetching can be turned on/off by setting {@link #maxFetchCapacityBytes}.  Depending on prefetching is enabled or
 * disabled, the behavior of the firehose is different like below.
 *
 * <ol>
 *   <li>
 *     If prefetch is enabled, PrefetchableTextFilesFirehose can fetch input objects in background.
 *   </li>
 *   <li> When next() is called, it first checks that there are already fetched files in local storage.
 *     <ol>
 *       <li>
 *         If exists, it simply chooses a fetched file and returns a {@link LineIterator} reading that file.
 *       </li>
 *       <li>
 *         If there is no fetched files in local storage but some objects are still remained to be read, the firehose
 *         fetches one of input objects in background immediately. If an IOException occurs while downloading the object,
 *         it retries up to the maximum retry count. Finally, the firehose returns a {@link LineIterator} only when the
 *         download operation is successfully finished.
 *         </li>
 *     </ol>
 *   </li>
 *   <li>
 *     If prefetch is disabled, the firehose returns a {@link LineIterator} which directly reads the stream opened by
 *     {@link #openObjectStream}. If there is an IOException, it will throw it and the read will fail.
 *   </li>
 * </ol>
 */
public abstract class PrefetchableTextFilesFirehoseFactory<ObjectType>
    extends AbstractTextFilesFirehoseFactory<ObjectType>
{
  private static final Logger LOG = new Logger(PrefetchableTextFilesFirehoseFactory.class);
  private static final long DEFAULT_MAX_CACHE_CAPACITY_BYTES = 1024 * 1024 * 1024; // 1GB
  private static final long DEFAULT_MAX_FETCH_CAPACITY_BYTES = 1024 * 1024 * 1024; // 1GB
  private static final long DEFAULT_FETCH_TIMEOUT = 60_000; // 60 secs
  private static final int DEFAULT_MAX_FETCH_RETRY = 3;
  private static final String FETCH_FILE_PREFIX = "fetch-";

  // The below two variables are roughly the max size of total cached/fetched objects, but the actual cached/fetched
  // size can be larger. The reason is our current client implementations for cloud storages like s3 don't support range
  // scan yet, so we must download the whole file at once. It's still possible for the size of cached/fetched data to
  // not exceed these variables by estimating the after-fetch size, but it makes us consider the case when any files
  // cannot be fetched due to their large size, which makes the implementation complicated.
  private final long maxCacheCapacityBytes;
  private final long maxFetchCapacityBytes;

  private final long prefetchTriggerBytes;

  // timeout for fetching an object from the remote site
  private final long fetchTimeout;

  // maximum retry for fetching an object from the remote site
  private final int maxFetchRetry;

  private final List<FetchedFile> cacheFiles = new ArrayList<>();
  private long totalCachedBytes;

  private List<ObjectType> objects;

  private static ExecutorService createFetchExecutor()
  {
    return Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder()
            .setNameFormat("firehose_fetch_%d")
            .build()
    );
  }

  public PrefetchableTextFilesFirehoseFactory(
      Long maxCacheCapacityBytes,
      Long maxFetchCapacityBytes,
      Long prefetchTriggerBytes,
      Long fetchTimeout,
      Integer maxFetchRetry
  )
  {
    this.maxCacheCapacityBytes = maxCacheCapacityBytes == null
                                 ? DEFAULT_MAX_CACHE_CAPACITY_BYTES
                                 : maxCacheCapacityBytes;
    this.maxFetchCapacityBytes = maxFetchCapacityBytes == null
                                 ? DEFAULT_MAX_FETCH_CAPACITY_BYTES
                                 : maxFetchCapacityBytes;
    this.prefetchTriggerBytes = prefetchTriggerBytes == null
                                ? this.maxFetchCapacityBytes / 2
                                : prefetchTriggerBytes;
    this.fetchTimeout = fetchTimeout == null ? DEFAULT_FETCH_TIMEOUT : fetchTimeout;
    this.maxFetchRetry = maxFetchRetry == null ? DEFAULT_MAX_FETCH_RETRY : maxFetchRetry;
  }

  @Override
  public Firehose connect(StringInputRowParser firehoseParser, File temporaryDirectory) throws IOException
  {
    if (maxCacheCapacityBytes == 0 && maxFetchCapacityBytes == 0) {
      return super.connect(firehoseParser, temporaryDirectory);
    }

    if (objects == null) {
      objects = ImmutableList.copyOf(Preconditions.checkNotNull(initObjects(), "objects"));
    }

    Preconditions.checkState(temporaryDirectory.exists(), "temporaryDirectory[%s] does not exist", temporaryDirectory);
    Preconditions.checkState(
        temporaryDirectory.isDirectory(),
        "temporaryDirectory[%s] is not a directory",
        temporaryDirectory
    );

    // fetchExecutor is responsible for background data fetching
    final ExecutorService fetchExecutor = createFetchExecutor();

    return new FileIteratingFirehose(
        new Iterator<LineIterator>()
        {
          // When prefetching is enabled, fetchFiles and nextFetchIndex are updated by the fetchExecutor thread, but
          // read by both the main thread (in hasNext()) and the fetchExecutor thread (in fetch()). To guarantee that
          // fetchFiles and nextFetchIndex are updated atomically, this lock must be held before updating
          // them.
          private final Object fetchLock = new Object();
          private final LinkedBlockingQueue<FetchedFile> fetchFiles = new LinkedBlockingQueue<>();

          // Number of bytes currently fetched files.
          // This is updated when a file is successfully fetched or a fetched file is deleted.
          private final AtomicLong fetchedBytes = new AtomicLong(0);
          private final boolean cacheInitialized;
          private final boolean prefetchEnabled;

          private Future<Void> fetchFuture;
          private int cacheIterateIndex;
          // nextFetchIndex indicates which object should be downloaded when fetch is triggered.
          private int nextFetchIndex;

          {
            cacheInitialized = totalCachedBytes > 0;
            prefetchEnabled = maxFetchCapacityBytes > 0;

            if (cacheInitialized) {
              nextFetchIndex = cacheFiles.size();
            }
            if (prefetchEnabled) {
              fetchIfNeeded(totalCachedBytes);
            }
          }

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
           */
          private void fetch() throws Exception
          {
            for (int i = nextFetchIndex; i < objects.size() && fetchedBytes.get() <= maxFetchCapacityBytes; i++) {
              final ObjectType object = objects.get(i);
              LOG.info("Fetching object[%s], fetchedBytes[%d]", object, fetchedBytes.get());
              final File outFile = File.createTempFile(FETCH_FILE_PREFIX, null, temporaryDirectory);
              fetchedBytes.addAndGet(download(object, outFile, 0));
              synchronized (fetchLock) {
                fetchFiles.put(new FetchedFile(object, outFile));
                nextFetchIndex++;
              }
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
           *
           * @throws IOException
           */
          private long download(ObjectType object, File outFile, int tryCount) throws IOException
          {
            try (final InputStream is = openObjectStream(object);
                 final CountingOutputStream cos = new CountingOutputStream(new FileOutputStream(outFile))) {
              IOUtils.copy(is, cos);
              return cos.getCount();
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
            synchronized (fetchLock) {
              return (cacheInitialized && cacheIterateIndex < cacheFiles.size())
                     || !fetchFiles.isEmpty()
                     || nextFetchIndex < objects.size();
            }
          }

          @Override
          public LineIterator next()
          {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }

            // If fetch() fails, hasNext() always returns true because nextFetchIndex must be smaller than the number
            // of objects, which means next() is always called. The below method checks that fetch() threw an exception
            // and propagates it if exists.
            checkFetchException();

            final OpenedObject openedObject;

            try {
              // Check cache first
              if (cacheInitialized && cacheIterateIndex < cacheFiles.size()) {
                final FetchedFile fetchedFile = cacheFiles.get(cacheIterateIndex++);
                openedObject = new OpenedObject(fetchedFile, getNoopCloser());
              } else if (prefetchEnabled) {
                openedObject = openObjectFromLocal();
              } else {
                openedObject = openObjectFromRemote();
              }

              final InputStream stream = wrapObjectStream(
                  openedObject.object,
                  openedObject.objectStream
              );

              return new ResourceCloseableLineIterator(
                  new InputStreamReader(stream, Charsets.UTF_8),
                  openedObject.resourceCloser
              );
            }
            catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }

          private void checkFetchException()
          {
            if (fetchFuture != null && fetchFuture.isDone()) {
              try {
                fetchFuture.get();
                fetchFuture = null;
              }
              catch (InterruptedException | ExecutionException e) {
                throw Throwables.propagate(e);
              }
            }
          }

          private OpenedObject openObjectFromLocal() throws IOException
          {
            final FetchedFile fetchedFile;
            final Closeable resourceCloser;

            if (!fetchFiles.isEmpty()) {
              // If there are already fetched files, use them
              fetchedFile = fetchFiles.poll();
              resourceCloser = cacheIfPossibleAndGetCloser(fetchedFile, fetchedBytes);
              fetchIfNeeded(fetchedBytes.get());
            } else {
              // Otherwise, wait for fetching
              try {
                fetchIfNeeded(fetchedBytes.get());
                fetchedFile = fetchFiles.poll(fetchTimeout, TimeUnit.MILLISECONDS);
                if (fetchedFile == null) {
                  // Check the latest fetch is failed
                  checkFetchException();
                  // Or throw a timeout exception
                  throw new RuntimeException(new TimeoutException());
                }
                resourceCloser = cacheIfPossibleAndGetCloser(fetchedFile, fetchedBytes);
                // trigger fetch again for subsequent next() calls
                fetchIfNeeded(fetchedBytes.get());
              }
              catch (InterruptedException e) {
                throw Throwables.propagate(e);
              }
            }
            return new OpenedObject(fetchedFile, resourceCloser);
          }

          private OpenedObject openObjectFromRemote() throws IOException
          {
            final OpenedObject openedObject;
            final Closeable resourceCloser = getNoopCloser();

            if (totalCachedBytes < maxCacheCapacityBytes) {
              LOG.info("Caching object[%s]", objects.get(nextFetchIndex));
              try {
                // Since maxFetchCapacityBytes is 0, at most one file is fetched.
                fetch();
                FetchedFile fetchedFile = fetchFiles.poll();
                if (fetchedFile == null) {
                  throw new ISE("Cannot fetch object[%s]", objects.get(nextFetchIndex));
                }
                cacheIfPossible(fetchedFile);
                fetchedBytes.addAndGet(-fetchedFile.length());
                openedObject = new OpenedObject(fetchedFile, resourceCloser);
              }
              catch (Exception e) {
                throw Throwables.propagate(e);
              }
            } else {
              final ObjectType object = objects.get(nextFetchIndex++);
              LOG.info("Reading object[%s]", object);
              openedObject = new OpenedObject(object, openObjectStream(object), resourceCloser);
            }
            return openedObject;
          }
        },
        firehoseParser,
        () -> {
          fetchExecutor.shutdownNow();
          try {
            Preconditions.checkState(fetchExecutor.awaitTermination(fetchTimeout, TimeUnit.MILLISECONDS));
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ISE("Failed to shutdown fetch executor during close");
          }
        }
    );
  }

  private boolean cacheIfPossible(FetchedFile fetchedFile)
  {
    // maxCacheCapacityBytes is a rough limit, so if totalCachedBytes is larger than it, no more caching is
    // allowed.
    if (totalCachedBytes < maxCacheCapacityBytes) {
      cacheFiles.add(fetchedFile);
      totalCachedBytes += fetchedFile.length();
      return true;
    } else {
      return false;
    }
  }

  private Closeable cacheIfPossibleAndGetCloser(FetchedFile fetchedFile, AtomicLong fetchedBytes)
  {
    final Closeable closeable;
    if (cacheIfPossible(fetchedFile)) {
      closeable = getNoopCloser();
      // If the fetchedFile is cached, make a room for fetching more data immediately.
      // This is because cache space and fetch space are separated.
      fetchedBytes.addAndGet(-fetchedFile.length());
    } else {
      closeable = getFetchedFileCloser(fetchedFile, fetchedBytes);
    }
    return closeable;
  }

  private Closeable getNoopCloser()
  {
    return () -> {};
  }

  private Closeable getFetchedFileCloser(
      final FetchedFile fetchedFile,
      final AtomicLong fetchedBytes
  )
  {
    return () -> {
      final long fileSize = fetchedFile.length();
      fetchedFile.delete();
      fetchedBytes.addAndGet(-fileSize);
    };
  }

  /**
   * This class calls the {@link Closeable#close()} method of the resourceCloser when it is closed.
   */
  static class ResourceCloseableLineIterator extends LineIterator
  {
    private final Closeable resourceCloser;

    public ResourceCloseableLineIterator(Reader reader, Closeable resourceCloser) throws IllegalArgumentException
    {
      super(reader);
      this.resourceCloser = resourceCloser;
    }

    @Override
    public void close()
    {
      super.close();
      try {
        resourceCloser.close();
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private class FetchedFile
  {
    private final ObjectType object;
    private final File file;

    public FetchedFile(ObjectType object, File file)
    {
      this.object = object;
      this.file = file;
    }

    public long length()
    {
      return file.length();
    }

    public void delete()
    {
      file.delete();
    }
  }

  private class OpenedObject
  {
    private final ObjectType object;
    private final InputStream objectStream;
    private final Closeable resourceCloser;

    public OpenedObject(FetchedFile fetchedFile, Closeable resourceCloser) throws IOException
    {
      this.object = fetchedFile.object;
      this.objectStream = FileUtils.openInputStream(fetchedFile.file);
      this.resourceCloser = resourceCloser;
    }

    public OpenedObject(ObjectType object, InputStream objectStream, Closeable resourceCloser)
    {
      this.object = object;
      this.objectStream = objectStream;
      this.resourceCloser = resourceCloser;
    }
  }
}

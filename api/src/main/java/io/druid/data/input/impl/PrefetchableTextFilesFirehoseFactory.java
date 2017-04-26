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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CountingOutputStream;
import io.druid.data.input.Firehose;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.BufferedReader;
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
 * <li>Caching: for the first call of {@link #connect(StringInputRowParser, File)}, it caches objects in a local disk
 * up to {@link #maxCacheCapacityBytes}.  These caches are NOT deleted until the process terminates,
 * and thus can be used for future reads.</li>
 * <li>Fetching: when it reads all cached data, it fetches remaining objects into a local disk and reads data from
 * them.  For the performance reason, prefetch technique is used, that is, when the size of remaining cached or
 * fetched data is smaller than {@link #prefetchTriggerBytes}, a background prefetch thread automatically starts to
 * fetch remaining objects.</li>
 * <li>Retry: if an exception occurs while downloading an object, it retries again up to
 * {@link #maxFetchRetry}.</li>
 * </ul>
 *
 * This implementation can be useful when the cost for reading input objects is large as reading from AWS S3 because
 * IndexTask can read the whole data twice for determining partition specs and generating segments if the intervals of
 * GranularitySpec is not specified.
 */
public abstract class PrefetchableTextFilesFirehoseFactory<ObjectType>
    extends AbstractTextFilesFirehoseFactory<ObjectType>
{
  private static final Logger LOG = new Logger(PrefetchableTextFilesFirehoseFactory.class);
  private static final long DEFAULT_MAX_CACHE_CAPACITY_BYTES = 1024 * 1024 * 1024; // 1GB
  private static final long DEFAULT_MAX_FETCH_CAPACITY_BYTES = 1024 * 1024 * 1024; // 1GB
  private static final long DEFAULT_FETCH_TIMEOUT = 60_000; // 60 secs
  private static final int DEFAULT_MAX_FETCH_RETRY = 3;
  private static final String CACHE_FILE_PREFIX = "cache-";
  private static final String FETCH_FILE_PREFIX = "fetch-";

  // The below two variables are roughly the max size of total cached/fetched objects, but the actual cached/fetched
  // size can be larger. The reason is our current client implementations for cloud storages like s3 don't support range
  // scan yet, so we must download the whole file at once. It's still possible for the size of cached/fetched data to
  // not exceed these variables by estimating the after-fetch size, but it makes us consider the case when any files
  // cannot be fetched due to their large size, which makes the implementation complicated.
  private long maxCacheCapacityBytes;
  private final long maxFetchCapacityBytes;

  private final long prefetchTriggerBytes;

  private final List<File> cacheFiles;
  private final LinkedBlockingQueue<File> fetchFiles;

  // Number of bytes currently fetched files.
  // This is updated when fetch a file is successfully fetched or a fetched file is deleted.
  private final AtomicLong fetchedBytes = new AtomicLong(0);

  // timeout for fetching an object from the remote site
  private final long fetchTimeout;

  // maximum retry for fetching an object from the remote site
  private final int maxFetchRetry;

  private volatile int nextFetchIndex;

  private Future<Void> fetchFuture;

  private boolean needCache = true;

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

    cacheFiles = new ArrayList<>();
    fetchFiles = new LinkedBlockingQueue<>();
  }

  @VisibleForTesting
  List<File> getCacheFiles()
  {
    return cacheFiles;
  }

  /**
   * Cache objects in a local disk up to {@link #maxCacheCapacityBytes}.
   */
  private void cache(final List<ObjectType> objects, File baseDir)
  {
    double totalFetchedBytes = 0;
    try {
      for (int i = 0; i < objects.size() && totalFetchedBytes < maxCacheCapacityBytes; i++) {
        final ObjectType object = objects.get(i);
        LOG.info("Caching object[%s]", object);
        final File outFile = File.createTempFile(CACHE_FILE_PREFIX, null, baseDir);
        totalFetchedBytes += download(object, outFile, 0);
        cacheFiles.add(outFile);
        nextFetchIndex++;
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Fetch objects to a local disk up to {@link #maxFetchCapacityBytes}.  This method is not thread safe and must be
   * called by a single thread.  Note that even {@link #maxFetchCapacityBytes} is 0, at least 1 file is always fetched.
   * This is for simplifying design, and should be improved when our client implementations for cloud storages like S3
   * support range scan.
   */
  private void fetch(final List<ObjectType> objects, File baseDir) throws Exception
  {
    for (int i = nextFetchIndex; i < objects.size() && fetchedBytes.get() <= maxFetchCapacityBytes; i++) {
      final ObjectType object = objects.get(i);
      LOG.info("Fetching object[%s]", object);
      final File outFile = File.createTempFile(FETCH_FILE_PREFIX, null, baseDir);
      fetchedBytes.addAndGet(download(object, outFile, 0));
      fetchFiles.put(outFile);
      nextFetchIndex++;
    }
  }

  /**
   * Downloads an object. It retires downloading {@link #maxFetchRetry} times and throws that exception.
   *
   * @param object  an object to be downloaded
   * @param outFile a file which the object data is stored
   * @param retry   current retry count
   *
   * @return number of downloaded bytes
   *
   * @throws IOException
   */
  private long download(ObjectType object, File outFile, int retry) throws IOException
  {
    try (final InputStream is = openStream(object);
         final CountingOutputStream cos = new CountingOutputStream(new FileOutputStream(outFile))) {
      IOUtils.copy(is, cos);
      return cos.getCount();
    }
    catch (IOException e) {
      if (retry < maxFetchRetry) {
        LOG.error(e, "Failed to download object[%s], retrying (%d of %d)", object, retry + 1, maxFetchRetry);
        outFile.delete();
        return download(object, outFile, retry + 1);
      } else {
        LOG.error(e, "Failed to download object[%s], retries exhausted, aborting", object);
        throw e;
      }
    }
  }

  @Override
  public Firehose connect(StringInputRowParser firehoseParser, File temporaryDirectory) throws IOException
  {
    final List<ObjectType> objects = ImmutableList.copyOf(Preconditions.checkNotNull(initObjects(), "objects"));
//    if (baseDir == null) {
//      baseDir = Files.createTempDir();
//      baseDir.deleteOnExit();
//      cache(objects);
//    } else {
//      nextFetchIndex = cacheFiles.size();
//    }

    Preconditions.checkState(temporaryDirectory.exists(), "temporaryDirectory[%s] does not exist", temporaryDirectory);
    Preconditions.checkState(
        temporaryDirectory.isDirectory(),
        "temporaryDirectory[%s] is not a directory",
        temporaryDirectory
    );

    if (needCache) {
      cache(objects, temporaryDirectory);
      needCache = false;
    } else {
      nextFetchIndex = cacheFiles.size();
    }

    // fetchExecutor is responsible for background data fetching
    final ExecutorService fetchExecutor = Executors.newSingleThreadExecutor();

    return new FileIteratingFirehose(
        new Iterator<LineIterator>()
        {
          final Iterator<File> cacheFileIterator = cacheFiles.iterator();
          long remainingCachedBytes = cacheFiles.stream()
                                                .mapToLong(File::length)
                                                .sum();

          {
            fetchIfNeeded(remainingCachedBytes);
          }

          @Override
          public boolean hasNext()
          {
            return cacheFileIterator.hasNext()
                   || !fetchFiles.isEmpty()
                   || nextFetchIndex < objects.size();
          }

          private void fetchIfNeeded(long remainingBytes)
          {
            if ((fetchFuture == null || fetchFuture.isDone())
                && remainingBytes <= prefetchTriggerBytes) {
              fetchFuture = fetchExecutor.submit(
                  () -> {
                    fetch(objects, temporaryDirectory);
                    return null;
                  }
              );
            }
          }

          @Override
          public LineIterator next()
          {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }

            // If fetch() fails, hasNext() always returns true because nextFetchIndex must be smaller than the number
            // of objects which means next() is always called. The below block checks that fetch() threw an exception
            // and propagates it if exists.
            if (fetchFuture != null && fetchFuture.isDone()) {
              try {
                fetchFuture.get();
              }
              catch (InterruptedException | ExecutionException e) {
                Throwables.propagate(e);
              }
            }

            final File fetchedFile;
            final Closeable closeable;
            // Check cache first
            if (cacheFileIterator.hasNext()) {
              fetchedFile = cacheFileIterator.next();
              remainingCachedBytes -= fetchedFile.length();
              fetchIfNeeded(remainingCachedBytes);
              closeable = () -> {
              };
            } else {
              if (!fetchFiles.isEmpty()) {
                // If there are already fetched files, use them
                fetchedFile = fetchFiles.poll();
                fetchIfNeeded(fetchedBytes.get());
              } else {
                // Otherwise, wait for fetching
                try {
                  fetchIfNeeded(fetchedBytes.get());
                  fetchedFile = fetchFiles.poll(fetchTimeout, TimeUnit.MILLISECONDS);
                  if (fetchedFile == null) {
                    throw new RuntimeException(new TimeoutException());
                  }
                }
                catch (InterruptedException e) {
                  throw Throwables.propagate(e);
                }
              }
              closeable = () -> {
                final long fileSize = fetchedFile.length();
                fetchedFile.delete();
                fetchedBytes.addAndGet(-fileSize);
              };
            }

            try {
              InputStream stream = FileUtils.openInputStream(fetchedFile);
              if (fetchedFile.getName().endsWith(".gz")) {
                stream = CompressionUtils.gzipInputStream(stream);
              }

              return new ResourceCloseableLineIterator(
                  new BufferedReader(
                      new InputStreamReader(stream, Charsets.UTF_8)
                  ),
                  closeable
              );
            }
            catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }
        },
        firehoseParser,
        fetchExecutor::shutdown
    );
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
}

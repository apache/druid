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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import io.druid.data.input.Firehose;
import io.druid.data.input.impl.AbstractTextFilesFirehoseFactory;
import io.druid.data.input.impl.FileIteratingFirehose;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.io.LineIterator;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * PrefetchableTextFilesFirehoseFactory is an abstract firehose factory for reading text files.  The firehose returned
 * by this class provides three key functionalities.
 * <p/>
 *
 * - Caching: for the first call of {@link #connect(StringInputRowParser, File)}, it caches objects in a local disk
 * up to maxCacheCapacityBytes.  These caches are NOT deleted until the process terminates, and thus can be used for
 * future reads.
 * <br/>
 * - Fetching: when it reads all cached data, it fetches remaining objects into a local disk and reads data from
 * them.  For the performance reason, prefetch technique is used, that is, when the size of remaining fetched data is
 * smaller than {@link #prefetchTriggerBytes}, a background prefetch thread automatically starts to fetch remaining
 * objects.
 * <br/>
 * - Retry: if an exception occurs while downloading an object, it retries again up to {@link #maxFetchRetry}.
 * <p/>
 *
 * This implementation can be useful when the cost for reading input objects is large as reading from AWS S3 because
 * batch tasks like IndexTask or HadoopIndexTask can read the whole data twice for determining partition specs and
 * generating segments if the intervals of GranularitySpec is not specified.
 * <br/>
 * Prefetching can be turned on/off by setting maxFetchCapacityBytes.  Depending on prefetching is enabled or
 * disabled, the behavior of the firehose is different like below.
 * <p/>
 *
 * 1. If prefetch is enabled, this firehose can fetch input objects in background.
 * <br/>
 * 2. When next() is called, it first checks that there are already fetched files in local storage.
 * <br/>
 *   2.1 If exists, it simply chooses a fetched file and returns a {@link LineIterator} reading that file.
 *   <br/>
 *   2.2 If there is no fetched files in local storage but some objects are still remained to be read, the firehose
 *   fetches one of input objects in background immediately. If an IOException occurs while downloading the object,
 *   it retries up to the maximum retry count. Finally, the firehose returns a {@link LineIterator} only when the
 *   download operation is successfully finished.
 *   <br/>
 * 3. If prefetch is disabled, the firehose returns a {@link LineIterator} which directly reads the stream opened by
 * {@link #openObjectStream}. If there is an IOException, it will throw it and the read will fail.
 */
public abstract class PrefetchableTextFilesFirehoseFactory<T>
    extends AbstractTextFilesFirehoseFactory<T>
{
  private static final Logger LOG = new Logger(PrefetchableTextFilesFirehoseFactory.class);
  private static final long DEFAULT_MAX_CACHE_CAPACITY_BYTES = 1024 * 1024 * 1024; // 1GB
  private static final long DEFAULT_MAX_FETCH_CAPACITY_BYTES = 1024 * 1024 * 1024; // 1GB
  private static final long DEFAULT_FETCH_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);
  private static final int DEFAULT_MAX_FETCH_RETRY = 3;

  private final CacheManager<T> cacheManager;
  private final long maxFetchCapacityBytes;
  private final long prefetchTriggerBytes;
  private final long fetchTimeout;
  private final int maxFetchRetry;

  private List<T> objects;

  public PrefetchableTextFilesFirehoseFactory(
      Long maxCacheCapacityBytes,
      Long maxFetchCapacityBytes,
      Long prefetchTriggerBytes,
      Long fetchTimeout,
      Integer maxFetchRetry
  )
  {
    this.cacheManager = new CacheManager<>(
        maxCacheCapacityBytes == null ? DEFAULT_MAX_CACHE_CAPACITY_BYTES : maxCacheCapacityBytes
    );
    this.maxFetchCapacityBytes = maxFetchCapacityBytes == null
                                 ? DEFAULT_MAX_FETCH_CAPACITY_BYTES
                                 : maxFetchCapacityBytes;
    this.prefetchTriggerBytes = prefetchTriggerBytes == null
                                ? this.maxFetchCapacityBytes / 2
                                : prefetchTriggerBytes;
    this.fetchTimeout = fetchTimeout == null ? DEFAULT_FETCH_TIMEOUT_MS : fetchTimeout;
    this.maxFetchRetry = maxFetchRetry == null ? DEFAULT_MAX_FETCH_RETRY : maxFetchRetry;
  }

  @JsonProperty
  public long getMaxCacheCapacityBytes()
  {
    return cacheManager.getMaxCacheCapacityBytes();
  }

  @JsonProperty
  public long getMaxFetchCapacityBytes()
  {
    return maxFetchCapacityBytes;
  }

  @JsonProperty
  public long getPrefetchTriggerBytes()
  {
    return prefetchTriggerBytes;
  }

  @JsonProperty
  public long getFetchTimeout()
  {
    return fetchTimeout;
  }

  @JsonProperty
  public int getMaxFetchRetry()
  {
    return maxFetchRetry;
  }

  @VisibleForTesting
  CacheManager<T> getCacheManager()
  {
    return cacheManager;
  }

  @Override
  public Firehose connect(StringInputRowParser firehoseParser, @Nullable File temporaryDirectory) throws IOException
  {
    if (objects == null) {
      objects = ImmutableList.copyOf(Preconditions.checkNotNull(initObjects(), "objects"));
    }

    if (cacheManager.isEnabled() || maxFetchCapacityBytes > 0) {
      Preconditions.checkNotNull(temporaryDirectory, "temporaryDirectory");
      Preconditions.checkArgument(
          temporaryDirectory.exists(),
          "temporaryDirectory[%s] does not exist",
          temporaryDirectory
      );
      Preconditions.checkArgument(
          temporaryDirectory.isDirectory(),
          "temporaryDirectory[%s] is not a directory",
          temporaryDirectory
      );
    }

    LOG.info("Create a new firehose for [%d] objects", objects.size());

    // fetchExecutor is responsible for background data fetching
    final ExecutorService fetchExecutor = Execs.singleThreaded("firehose_fetch_%d");
    final Fetcher<T> fetcher = new Fetcher<>(
        cacheManager,
        objects,
        fetchExecutor,
        temporaryDirectory,
        maxFetchCapacityBytes,
        prefetchTriggerBytes,
        fetchTimeout,
        maxFetchRetry,
        new ObjectOpenFunction<T>()
        {
          @Override
          public InputStream open(T object) throws IOException
          {
            return openObjectStream(object);
          }

          @Override
          public InputStream open(T object, long start) throws IOException
          {
            return openObjectStream(object, start);
          }
        },
        getRetryCondition()
    );

    return new FileIteratingFirehose(
        new Iterator<LineIterator>()
        {
          @Override
          public boolean hasNext()
          {
            return fetcher.hasNext();
          }

          @Override
          public LineIterator next()
          {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }

            final OpenedObject<T> openedObject = fetcher.next();
            final InputStream stream;
            try {
              stream = wrapObjectStream(
                  openedObject.getObject(),
                  openedObject.getObjectStream()
              );
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }

            return new ResourceCloseableLineIterator(
                new InputStreamReader(stream, StandardCharsets.UTF_8),
                openedObject.getResourceCloser()
            );
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

  /**
   * Returns a predicate describing retry conditions. {@link Fetcher} and {@link RetryingInputStream} will retry on the
   * errors satisfying this condition.
   */
  protected abstract Predicate<Throwable> getRetryCondition();

  /**
   * Open an input stream from the given object.  If the object is compressed, this method should return a byte stream
   * as it is compressed.  The object compression should be handled in {@link #wrapObjectStream(Object, InputStream)}.
   *
   * @param object an object to be read
   * @param start  start offset
   *
   * @return an input stream for the object
   */
  protected abstract InputStream openObjectStream(T object, long start) throws IOException;

  /**
   * This class calls the {@link Closeable#close()} method of the resourceCloser when it is closed.
   */
  static class ResourceCloseableLineIterator extends LineIterator
  {
    private final Closeable resourceCloser;

    ResourceCloseableLineIterator(Reader reader, Closeable resourceCloser) throws IllegalArgumentException
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
        throw new RuntimeException(e);
      }
    }
  }
}

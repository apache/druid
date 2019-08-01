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

package org.apache.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.LineIterator;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.prefetch.CacheManager;
import org.apache.druid.data.input.impl.prefetch.Fetcher;
import org.apache.druid.data.input.impl.prefetch.JsonIterator;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.data.input.impl.prefetch.OpenedObject;
import org.apache.druid.data.input.impl.prefetch.PrefetchConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * PrefetchSqlFirehoseFactory is an abstract firehose factory for reading prefetched sql resultset data. Regardless
 * of whether prefetching is enabled or not, for each sql object the entire result set is fetched into a file in the local disk.
 * This class defines prefetching as caching the resultsets into local disk in case multiple sql queries are present.
 * When prefetching is enabled, the following functionalities are provided:
 * <p/>
 * <p>
 * - Caching: for the first call of {@link #connect(InputRowParser, File)}, it caches objects in a local disk
 * up to maxCacheCapacityBytes.  These caches are NOT deleted until the process terminates, and thus can be used for
 * future reads.
 * <br/>
 * - Fetching: when it reads all cached data, it fetches remaining objects into a local disk and reads data from
 * them.  For the performance reason, prefetch technique is used, that is, when the size of remaining fetched data is
 * smaller than {@link PrefetchConfig#prefetchTriggerBytes}, a background prefetch thread automatically starts to fetch remaining
 * objects.
 * <br/>
 * <p/>
 * <p>
 * This implementation aims to avoid maintaining a persistent connection to the database by prefetching the resultset into disk.
 * <br/>
 * Prefetching can be turned on/off by setting maxFetchCapacityBytes.  Depending on prefetching is enabled or
 * disabled, the behavior of the firehose is different like below.
 * <p/>
 * <p>
 * 1. If prefetch is enabled this firehose can fetch input objects in background.
 * <br/>
 * 2. When next() is called, it first checks that there are already fetched files in local storage.
 * <br/>
 * 2.1 If exists, it simply chooses a fetched file and returns a {@link LineIterator} reading that file.
 * <br/>
 * 2.2 If there is no fetched files in local storage but some objects are still remained to be read, the firehose
 * fetches one of input objects in background immediately. Finally, the firehose returns an iterator of {@link JsonIterator}
 * for deserializing the saved resultset.
 * <br/>
 * 3. If prefetch is disabled, the firehose saves the resultset to file and returns an iterator of {@link JsonIterator}
 * which directly reads the stream opened by {@link #openObjectStream}. If there is an IOException, it will throw it
 * and the read will fail.
 */
public abstract class PrefetchSqlFirehoseFactory<T>
    implements FirehoseFactory<InputRowParser<Map<String, Object>>>
{
  private static final Logger LOG = new Logger(PrefetchSqlFirehoseFactory.class);

  private final PrefetchConfig prefetchConfig;
  private final CacheManager<T> cacheManager;
  private List<T> objects;
  private ObjectMapper objectMapper;


  public PrefetchSqlFirehoseFactory(
      Long maxCacheCapacityBytes,
      Long maxFetchCapacityBytes,
      Long prefetchTriggerBytes,
      Long fetchTimeout,
      ObjectMapper objectMapper
  )
  {
    this.prefetchConfig = new PrefetchConfig(
        maxCacheCapacityBytes,
        maxFetchCapacityBytes,
        prefetchTriggerBytes,
        fetchTimeout
    );
    this.cacheManager = new CacheManager<>(
        prefetchConfig.getMaxCacheCapacityBytes()
    );
    this.objectMapper = objectMapper;
  }

  @JsonProperty
  public long getMaxCacheCapacityBytes()
  {
    return cacheManager.getMaxCacheCapacityBytes();
  }

  @JsonProperty
  public long getMaxFetchCapacityBytes()
  {
    return prefetchConfig.getMaxFetchCapacityBytes();
  }

  @JsonProperty
  public long getPrefetchTriggerBytes()
  {
    return prefetchConfig.getPrefetchTriggerBytes();
  }

  @JsonProperty
  public long getFetchTimeout()
  {
    return prefetchConfig.getFetchTimeout();
  }

  @VisibleForTesting
  CacheManager<T> getCacheManager()
  {
    return cacheManager;
  }

  @Override
  public Firehose connect(InputRowParser<Map<String, Object>> firehoseParser, @Nullable File temporaryDirectory)
  {
    if (objects == null) {
      objects = ImmutableList.copyOf(Preconditions.checkNotNull(initObjects(), "objects"));
    }
    if (cacheManager.isEnabled() || prefetchConfig.getMaxFetchCapacityBytes() > 0) {
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

    LOG.info("Create a new firehose for [%d] queries", objects.size());

    // fetchExecutor is responsible for background data fetching
    final ExecutorService fetchExecutor = Execs.singleThreaded("firehose_fetch_%d");
    final Fetcher<T> fetcher = new SqlFetcher<>(
        cacheManager,
        objects,
        fetchExecutor,
        temporaryDirectory,
        prefetchConfig,
        new ObjectOpenFunction<T>()
        {
          @Override
          public InputStream open(T object, File outFile) throws IOException
          {
            return openObjectStream(object, outFile);
          }

          @Override
          public InputStream open(T object) throws IOException
          {
            final File outFile = File.createTempFile("sqlresults_", null, temporaryDirectory);
            return openObjectStream(object, outFile);
          }
        }
    );

    return new SqlFirehose(
        new Iterator<JsonIterator<Map<String, Object>>>()
        {
          @Override
          public boolean hasNext()
          {
            return fetcher.hasNext();
          }

          @Override
          public JsonIterator<Map<String, Object>> next()
          {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            try {
              TypeReference<Map<String, Object>> type = new TypeReference<Map<String, Object>>()
              {
              };
              final OpenedObject<T> openedObject = fetcher.next();
              final InputStream stream = openedObject.getObjectStream();
              return new JsonIterator<>(type, stream, openedObject.getResourceCloser(), objectMapper);
            }
            catch (Exception ioe) {
              throw new RuntimeException(ioe);
            }
          }
        },
        firehoseParser,
        () -> {
          fetchExecutor.shutdownNow();
          try {
            Preconditions.checkState(fetchExecutor.awaitTermination(
                prefetchConfig.getFetchTimeout(),
                TimeUnit.MILLISECONDS
            ));
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ISE("Failed to shutdown fetch executor during close");
          }
        }
    );
  }

  /**
   * Open an input stream from the given object.  The object is fetched into the file and an input
   * stream to the file is provided.
   *
   * @param object   an object to be read
   * @param filename file to which the object is fetched into
   *
   * @return an input stream to the file
   */
  protected abstract InputStream openObjectStream(T object, File filename) throws IOException;

  protected abstract Collection<T> initObjects();
}

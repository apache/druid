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

package org.apache.druid.client.cache;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * {@link CachePopulator} implementation that uses a {@link ExecutorService} thread pool to populate a cache in the
 * background. Used if config "druid.*.cache.numBackgroundThreads" is greater than 0.
 */
public class BackgroundCachePopulator implements CachePopulator
{
  private static final Logger log = new Logger(BackgroundCachePopulator.class);

  private final ListeningExecutorService exec;
  private final ObjectMapper objectMapper;
  private final CachePopulatorStats cachePopulatorStats;
  private final long maxEntrySize;

  public BackgroundCachePopulator(
      final ExecutorService exec,
      final ObjectMapper objectMapper,
      final CachePopulatorStats cachePopulatorStats,
      final long maxEntrySize
  )
  {
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.objectMapper = Preconditions.checkNotNull(objectMapper, "objectMapper");
    this.cachePopulatorStats = Preconditions.checkNotNull(cachePopulatorStats, "cachePopulatorStats");
    this.maxEntrySize = maxEntrySize;
  }

  @Override
  public <T, CacheType> Sequence<T> wrap(
      final Sequence<T> sequence,
      final Function<T, CacheType> cacheFn,
      final Cache cache,
      final Cache.NamedKey cacheKey
  )
  {
    final List<ListenableFuture<CacheType>> cacheFutures = new ArrayList<>();

    final Sequence<T> wrappedSequence = Sequences.map(
        sequence,
        input -> {
          cacheFutures.add(exec.submit(() -> cacheFn.apply(input)));
          return input;
        }
    );

    return Sequences.withEffect(
        wrappedSequence,
        () -> {
          Futures.addCallback(
              Futures.allAsList(cacheFutures),
              new FutureCallback<List<CacheType>>()
              {
                @Override
                public void onSuccess(List<CacheType> results)
                {
                  populateCache(cache, cacheKey, results);
                  // Help out GC by making sure all references are gone
                  cacheFutures.clear();
                }

                @Override
                public void onFailure(Throwable t)
                {
                  log.error(t, "Background caching failed");
                }
              },
              exec
          );
        },
        Execs.directExecutor()
    );
  }

  private <CacheType> void populateCache(
      final Cache cache,
      final Cache.NamedKey cacheKey,
      final List<CacheType> results
  )
  {
    try {
      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

      try (JsonGenerator gen = objectMapper.getFactory().createGenerator(bytes)) {
        for (CacheType result : results) {
          gen.writeObject(result);

          if (maxEntrySize > 0 && bytes.size() > maxEntrySize) {
            cachePopulatorStats.incrementOversized();
            return;
          }
        }
      }

      if (maxEntrySize > 0 && bytes.size() > maxEntrySize) {
        cachePopulatorStats.incrementOversized();
        return;
      }

      cache.put(cacheKey, bytes.toByteArray());
      cachePopulatorStats.incrementOk();
    }
    catch (Exception e) {
      log.warn(e, "Could not populate cache");
      cachePopulatorStats.incrementError();
    }
  }
}

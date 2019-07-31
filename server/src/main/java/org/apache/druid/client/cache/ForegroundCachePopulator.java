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
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Function;

/**
 * {@link CachePopulator} implementation that populates a cache on the same thread that is processing the
 * {@link Sequence}. Used if config "druid.*.cache.numBackgroundThreads" is 0 (the default).
 */
public class ForegroundCachePopulator implements CachePopulator
{
  private static final Logger log = new Logger(ForegroundCachePopulator.class);

  private final ObjectMapper objectMapper;
  private final CachePopulatorStats cachePopulatorStats;
  private final long maxEntrySize;

  public ForegroundCachePopulator(
      final ObjectMapper objectMapper,
      final CachePopulatorStats cachePopulatorStats,
      final long maxEntrySize
  )
  {
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
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final MutableBoolean tooBig = new MutableBoolean(false);
    final JsonGenerator jsonGenerator;

    try {
      jsonGenerator = objectMapper.getFactory().createGenerator(bytes);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    return Sequences.wrap(
        Sequences.map(
            sequence,
            input -> {
              if (!tooBig.isTrue()) {
                try {
                  jsonGenerator.writeObject(cacheFn.apply(input));

                  // Not flushing jsonGenerator before checking this, but should be ok since Jackson buffers are
                  // typically just a few KB, and we don't want to waste cycles flushing.
                  if (maxEntrySize > 0 && bytes.size() > maxEntrySize) {
                    tooBig.setValue(true);
                  }
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }

              return input;
            }
        ),
        new SequenceWrapper()
        {
          @Override
          public void after(final boolean isDone, final Throwable thrown) throws Exception
          {
            jsonGenerator.close();

            if (isDone) {
              // Check tooBig, then check maxEntrySize one more time, after closing/flushing jsonGenerator.
              if (tooBig.isTrue() || (maxEntrySize > 0 && bytes.size() > maxEntrySize)) {
                cachePopulatorStats.incrementOversized();
                return;
              }

              try {
                cache.put(cacheKey, bytes.toByteArray());
                cachePopulatorStats.incrementOk();
              }
              catch (Exception e) {
                log.warn(e, "Unable to write to cache");
                cachePopulatorStats.incrementError();
              }
            }
          }
        }
    );
  }
}

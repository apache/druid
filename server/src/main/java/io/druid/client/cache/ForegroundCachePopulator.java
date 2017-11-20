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

package io.druid.client.cache;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.SequenceWrapper;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.CacheStrategy;
import io.druid.query.Query;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ForegroundCachePopulator implements CachePopulator
{
  private static final Logger log = new Logger(ForegroundCachePopulator.class);

  private final Object lock = new Object();
  private final ObjectMapper objectMapper;
  private final long maxEntrySize;

  public ForegroundCachePopulator(final ObjectMapper objectMapper, final long maxEntrySize)
  {
    this.objectMapper = Preconditions.checkNotNull(objectMapper, "objectMapper");
    this.maxEntrySize = maxEntrySize;
  }

  @Override
  public <T, CacheType, QueryType extends Query<T>> Sequence<T> wrap(
      final Sequence<T> sequence,
      final CacheStrategy<T, CacheType, QueryType> cacheStrategy,
      final Cache cache,
      final Cache.NamedKey cacheKey
  )
  {
    final Function<T, CacheType> cacheFn = cacheStrategy.prepareForCache();
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final AtomicBoolean tooBig = new AtomicBoolean(false);
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
              if (!tooBig.get()) {
                synchronized (lock) {
                  try {
                    jsonGenerator.writeObject(cacheFn.apply(input));

                    // Not flushing jsonGenerator before checking this, but should be ok since Jackson buffers are
                    // typically just a few KB, and we don't want to waste cycles flushing.
                    if (maxEntrySize > 0 && bytes.size() > maxEntrySize) {
                      tooBig.set(true);
                    }
                  }
                  catch (IOException e) {
                    throw new RuntimeException(e);
                  }
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
            synchronized (lock) {
              jsonGenerator.close();

              if (isDone && !tooBig.get()) {
                // Check maxEntrySize one more time, after closing/flushing jsonGenerator.
                if (maxEntrySize > 0 && bytes.size() > maxEntrySize) {
                  return;
                }

                try {
                  cache.put(cacheKey, bytes.toByteArray());
                }
                catch (Exception e) {
                  log.warn(e, "Unable to write to cache");
                }
              }
            }
          }
        }
    );
  }
}

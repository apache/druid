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

package org.apache.druid.server.lookup.cache.loading;

import com.google.common.base.Preconditions;

/**
 * Statistics about the performance of a {@link LoadingCache}. Instances of this class are immutable.
 * Cache statistics are incremented according to the following rules:
 * <ul>
 * <li>When a cache lookup encounters an existing cache entry {@code hitCount} is incremented.
 * <li>When a cache lookup first encounters a missing cache entry {@code missCount} is incremented.
 * <li>When an entry is evicted from the cache, {@code evictionCount} is incremented.
 * </ul>
 */
public class LookupCacheStats
{
  private final long hitCount;
  private final long missCount;
  private final long evictionCount;

  /**
   * Constructs a new {@code CacheStats} instance.
   */
  public LookupCacheStats(long hitCount, long missCount, long evictionCount)
  {
    Preconditions.checkArgument(hitCount >= 0);
    Preconditions.checkArgument(missCount >= 0);
    Preconditions.checkArgument(evictionCount >= 0);

    this.hitCount = hitCount;
    this.missCount = missCount;
    this.evictionCount = evictionCount;
  }

  /**
   * Returns the number of times {@link LoadingCache} lookup methods have returned either a cached or
   * uncached value. This is defined as {@code hitCount + missCount}.
   */
  public long requestCount()
  {
    return hitCount + missCount;
  }

  /**
   * Returns the number of times {@link LoadingCache} lookup methods have returned a cached value.
   */
  public long hitCount()
  {
    return hitCount;
  }

  /**
   * Returns the ratio of cache requests which were hits. This is defined as
   * {@code hitCount / requestCount}, or {@code 1.0} when {@code requestCount == 0}.
   * Note that {@code hitRate + missRate =~ 1.0}.
   */
  public double hitRate()
  {
    long requestCount = requestCount();
    return (requestCount == 0) ? 1.0 : (double) hitCount / requestCount;
  }

  /**
   * Returns the number of times {@link LoadingCache} lookup methods have returned an uncached (newly
   * loaded) value, or null. Multiple concurrent calls to {@link LoadingCache} lookup methods on an absent
   * value can result in multiple misses, all returning the results of a single cache load
   * operation.
   */
  public long missCount()
  {
    return missCount;
  }

  /**
   * Returns the ratio of cache requests which were misses. This is defined as
   * {@code missCount / requestCount}, or {@code 0.0} when {@code requestCount == 0}.
   * Note that {@code hitRate + missRate =~ 1.0}. Cache misses include all requests which
   * weren't cache hits, including requests which resulted in either successful or failed loading
   * attempts, and requests which waited for other threads to finish loading. It is thus the case
   * that {@code missCount &gt;= loadSuccessCount + loadExceptionCount}. Multiple
   * concurrent misses for the same key will result in a single load operation.
   */
  public double missRate()
  {
    long requestCount = requestCount();
    return (requestCount == 0) ? 0.0 : (double) missCount / requestCount;
  }


  /**
   * Returns the number of times an entry has been evicted. This count does not include manual
   * {@linkplain LoadingCache#invalidate invalidations}.
   */
  public long evictionCount()
  {
    return evictionCount;
  }

  /**
   * Returns a new {@code CacheStats} representing the difference between this {@code CacheStats}
   * and {@code other}. Negative values, which aren't supported by {@code CacheStats} will be
   * rounded up to zero.
   */
  public LookupCacheStats minus(LookupCacheStats other)
  {
    return new LookupCacheStats(
        Math.max(0, hitCount - other.hitCount),
        Math.max(0, missCount - other.missCount),
        Math.max(0, evictionCount - other.evictionCount)
    );
  }

  /**
   * Returns a new {@code CacheStats} representing the sum of this {@code CacheStats}
   * and {@code other}.
   */
  public LookupCacheStats plus(LookupCacheStats other)
  {
    return new LookupCacheStats(
        hitCount + other.hitCount,
        missCount + other.missCount,
        evictionCount + other.evictionCount
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LookupCacheStats)) {
      return false;
    }

    LookupCacheStats that = (LookupCacheStats) o;

    if (hitCount != that.hitCount) {
      return false;
    }
    if (missCount != that.missCount) {
      return false;
    }

    return evictionCount == that.evictionCount;

  }

  @Override
  public int hashCode()
  {
    int result = (int) (hitCount ^ (hitCount >>> 32));
    result = 31 * result + (int) (missCount ^ (missCount >>> 32));
    result = 31 * result + (int) (evictionCount ^ (evictionCount >>> 32));
    return result;
  }

  @Override
  public String toString()
  {
    return "LookupCacheStats{" +
           "hitCount=" + hitCount +
           ", missCount=" + missCount +
           ", evictionCount=" + evictionCount +
           '}';
  }
}

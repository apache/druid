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

package org.apache.druid.server.lookup;

import org.apache.druid.server.lookup.cache.loading.LoadingCache;
import org.apache.druid.server.lookup.cache.loading.OnHeapLoadingCache;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadingLookupNegativeCachingTest
{
  private CountingFetcher fetcher;
  private LoadingCache<String, String> loadingCache;
  private LoadingCache<String, List<String>> reverseLoadingCache;
  private LoadingCache<String, String> negativeLoadingCache;

  @BeforeEach
  public void setUp()
  {
    fetcher = new CountingFetcher();
    fetcher.values.put("known", "value");
    loadingCache = new OnHeapLoadingCache<>(4, 16, null, null, null);
    reverseLoadingCache = new OnHeapLoadingCache<>(4, 16, null, null, null);
    negativeLoadingCache = new OnHeapLoadingCache<>(4, 16, null, null, null);
  }

  @Test
  public void testAbsentKeyIsFetchedEveryTimeWithoutNegativeCache()
  {
    final LoadingLookup lookup = new LoadingLookup(fetcher, loadingCache, reverseLoadingCache);

    Assertions.assertNull(lookup.apply("absent"));
    Assertions.assertNull(lookup.apply("absent"));
    Assertions.assertNull(lookup.apply("absent"));

    Assertions.assertEquals(3, fetcher.fetchCount("absent"));
  }

  @Test
  public void testAbsentKeyIsFetchedOnceWithNegativeCache()
  {
    final LoadingLookup lookup = new LoadingLookup(fetcher, loadingCache, reverseLoadingCache, negativeLoadingCache);

    Assertions.assertNull(lookup.apply("absent"));
    Assertions.assertNull(lookup.apply("absent"));
    Assertions.assertNull(lookup.apply("absent"));

    Assertions.assertEquals(1, fetcher.fetchCount("absent"));
  }

  @Test
  public void testNegativeCacheDoesNotAffectKnownKeys()
  {
    final LoadingLookup lookup = new LoadingLookup(fetcher, loadingCache, reverseLoadingCache, negativeLoadingCache);

    Assertions.assertEquals("value", lookup.apply("known"));
    Assertions.assertEquals("value", lookup.apply("known"));

    Assertions.assertEquals(1, fetcher.fetchCount("known"));
    Assertions.assertNull(negativeLoadingCache.getIfPresent("known"));
  }

  @Test
  public void testKeyInPositiveCacheDoesNotTouchNegativeCache()
  {
    loadingCache.putAll(Collections.singletonMap("known", "value"));

    // A strict mock with no expectations fails the test on any call.
    final LoadingCache<String, String> strictNegativeCache = EasyMock.createStrictMock(LoadingCache.class);
    EasyMock.replay(strictNegativeCache);
    final LoadingLookup lookup = new LoadingLookup(fetcher, loadingCache, reverseLoadingCache, strictNegativeCache);

    Assertions.assertEquals("value", lookup.apply("known"));

    Assertions.assertEquals(0, fetcher.totalFetches());
    EasyMock.verify(strictNegativeCache);
  }

  @Test
  public void testFetchFailureIsNotCachedForAbsentKey()
  {
    final LoadingLookup lookup = new LoadingLookup(fetcher, loadingCache, reverseLoadingCache, negativeLoadingCache);

    fetcher.failing = true;
    Assertions.assertThrows(IllegalStateException.class, () -> lookup.apply("absent"));
    Assertions.assertNull(negativeLoadingCache.getIfPresent("absent"));

    fetcher.failing = false;
    Assertions.assertNull(lookup.apply("absent"));

    // The failed attempt and the successful one both reached the fetcher.
    Assertions.assertEquals(2, fetcher.fetchCount("absent"));
  }

  @Test
  public void testFetchFailureIsNotCachedForKnownKey()
  {
    final LoadingLookup lookup = new LoadingLookup(fetcher, loadingCache, reverseLoadingCache, negativeLoadingCache);

    fetcher.failing = true;
    Assertions.assertThrows(IllegalStateException.class, () -> lookup.apply("known"));
    Assertions.assertNull(negativeLoadingCache.getIfPresent("known"));

    fetcher.failing = false;
    Assertions.assertEquals("value", lookup.apply("known"));
  }

  @Test
  public void testKeyIsFetchedAgainOnceItsNegativeEntryIsGone()
  {
    final LoadingLookup lookup = new LoadingLookup(fetcher, loadingCache, reverseLoadingCache, negativeLoadingCache);

    Assertions.assertNull(lookup.apply("late"));
    fetcher.values.put("late", "arrived");

    // Still remembered as absent.
    Assertions.assertNull(lookup.apply("late"));
    Assertions.assertEquals(1, fetcher.fetchCount("late"));

    negativeLoadingCache.invalidateAll();

    Assertions.assertEquals("arrived", lookup.apply("late"));
    Assertions.assertEquals("arrived", lookup.apply("late"));
    Assertions.assertEquals(2, fetcher.fetchCount("late"));
  }

  @Test
  public void testNullKeyIsNotCached()
  {
    final LoadingLookup lookup = new LoadingLookup(fetcher, loadingCache, reverseLoadingCache, negativeLoadingCache);

    Assertions.assertNull(lookup.apply(null));
    Assertions.assertEquals(0, fetcher.totalFetches());
  }

  @Test
  public void testCloseClosesNegativeCache()
  {
    final LoadingLookup lookup = new LoadingLookup(fetcher, loadingCache, reverseLoadingCache, negativeLoadingCache);

    lookup.close();

    Assertions.assertTrue(loadingCache.isClosed());
    Assertions.assertTrue(reverseLoadingCache.isClosed());
    Assertions.assertTrue(negativeLoadingCache.isClosed());
  }

  /**
   * A fetcher backed by a map that counts how many times each key was fetched.
   */
  private static class CountingFetcher implements DataFetcher<String, String>
  {
    private final Map<String, String> values = new HashMap<>();
    private final Map<String, AtomicInteger> counts = new HashMap<>();
    private volatile boolean failing;

    int fetchCount(final String key)
    {
      final AtomicInteger count = counts.get(key);
      return count == null ? 0 : count.get();
    }

    int totalFetches()
    {
      return counts.values().stream().mapToInt(AtomicInteger::get).sum();
    }

    @Override
    public String fetch(final String key)
    {
      counts.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
      if (failing) {
        throw new IllegalStateException("fetch failed");
      }
      return values.get(key);
    }

    @Override
    public Iterable<Map.Entry<String, String>> fetchAll()
    {
      return Collections.emptyList();
    }

    @Override
    public Iterable<Map.Entry<String, String>> fetch(final Iterable<String> keys)
    {
      final List<Map.Entry<String, String>> found = new ArrayList<>();
      for (final String key : keys) {
        final String value = fetch(key);
        if (value != null) {
          found.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
        }
      }
      return found;
    }

    @Override
    public List<String> reverseFetchKeys(final String value)
    {
      return Collections.emptyList();
    }
  }
}

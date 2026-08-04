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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class LoadingCacheTest
{
  private static final ImmutableMap IMMUTABLE_MAP = ImmutableMap.of("key", "value");

  public static Collection<Object[]> inputData()
  {
    return Arrays.asList(new Object[][]{
        {new OnHeapLoadingCache<>(4, 1000, null, null, null)},
        {new OffHeapLoadingCache(0, 0L, 0L, 0L)}
    });
  }

  private LoadingCache loadingCache;

  private void initLoadingCacheTest(LoadingCache loadingCache)
  {
    this.loadingCache = loadingCache;
    Assertions.assertFalse(loadingCache.isClosed());
    loadingCache.putAll(IMMUTABLE_MAP);
  }

  @AfterEach
  public void tearDown()
  {
    loadingCache.invalidateAll();
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testGetIfPresent(LoadingCache loadingCache)
  {
    initLoadingCacheTest(loadingCache);
    Assertions.assertNull(loadingCache.getIfPresent("not there"));
    Assertions.assertEquals(IMMUTABLE_MAP.get("key"), loadingCache.getIfPresent("key"));
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testGetAllPresent(LoadingCache loadingCache)
  {
    initLoadingCacheTest(loadingCache);
    Assertions.assertEquals(IMMUTABLE_MAP, loadingCache.getAllPresent(IMMUTABLE_MAP.keySet()));
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testPut(LoadingCache loadingCache) throws ExecutionException
  {
    initLoadingCacheTest(loadingCache);
    loadingCache.get("key2", new Callable()
    {
      @Override
      public Object call()
      {
        return "value2";
      }
    });
    Assertions.assertEquals("value2", loadingCache.getIfPresent("key2"));
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testInvalidate(LoadingCache loadingCache) throws ExecutionException
  {
    initLoadingCacheTest(loadingCache);
    loadingCache.get("key2", new Callable()
    {
      @Override
      public Object call()
      {
        return "value2";
      }
    });
    Assertions.assertEquals("value2", loadingCache.getIfPresent("key2"));
    loadingCache.invalidate("key2");
    Assertions.assertEquals(null, loadingCache.getIfPresent("key2"));
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testInvalidateAll(LoadingCache loadingCache) throws ExecutionException
  {
    initLoadingCacheTest(loadingCache);
    loadingCache.get("key2", new Callable()
    {
      @Override
      public Object call()
      {
        return "value2";
      }
    });
    Assertions.assertEquals("value2", loadingCache.getIfPresent("key2"));
    loadingCache.invalidateAll(Collections.singletonList("key2"));
    Assertions.assertEquals(null, loadingCache.getIfPresent("key2"));
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testInvalidateAll1(LoadingCache loadingCache) throws ExecutionException
  {
    initLoadingCacheTest(loadingCache);
    loadingCache.invalidateAll();
    loadingCache.get("key2", new Callable()
    {
      @Override
      public Object call()
      {
        return "value2";
      }
    });
    Assertions.assertEquals(loadingCache.getAllPresent(IMMUTABLE_MAP.keySet()), Collections.emptyMap());
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testGetStats(LoadingCache loadingCache)
  {
    initLoadingCacheTest(loadingCache);
    Assertions.assertTrue(loadingCache.getStats() != null && loadingCache.getStats() instanceof LookupCacheStats);
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testIsClosed(LoadingCache loadingCache)
  {
    initLoadingCacheTest(loadingCache);
    Assertions.assertFalse(loadingCache.isClosed());
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testSerDeser(LoadingCache loadingCache) throws IOException
  {
    initLoadingCacheTest(loadingCache);
    ObjectMapper mapper = new DefaultObjectMapper();
    Assertions.assertEquals(loadingCache, mapper.readerFor(LoadingCache.class).readValue(mapper.writeValueAsString(loadingCache)));
    Assertions.assertEquals(loadingCache.hashCode(), mapper.readerFor(LoadingCache.class).readValue(mapper.writeValueAsString(loadingCache)).hashCode());
  }

}

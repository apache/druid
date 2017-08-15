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

package io.druid.server.lookup.cache.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

@RunWith(Parameterized.class)
public class LoadingCacheTest
{
  private static final ImmutableMap IMMUTABLE_MAP = ImmutableMap.of("key", "value");

  @Parameterized.Parameters
  public static Collection<Object[]> inputData()
  {
    return Arrays.asList(new Object[][]{
        {new OnHeapLoadingCache<>(4, 1000, null, null, null)}, {
        new OffHeapLoadingCache(
            0,
            0L,
            0L,
            0L
        )
    }
    });
  }

  private final LoadingCache loadingCache;

  public LoadingCacheTest(LoadingCache loadingCache)
  {
    this.loadingCache = loadingCache;
  }

  @Before
  public void setUp() throws InterruptedException
  {
    Assert.assertFalse(loadingCache.isClosed());
    loadingCache.putAll(IMMUTABLE_MAP);
  }

  @After
  public void tearDown()
  {
    loadingCache.invalidateAll();
  }

  @Test
  public void testGetIfPresent()
  {
    Assert.assertNull(loadingCache.getIfPresent("not there"));
    Assert.assertEquals(IMMUTABLE_MAP.get("key"), loadingCache.getIfPresent("key"));
  }

  @Test
  public void testGetAllPresent()
  {
    Assert.assertEquals(IMMUTABLE_MAP, loadingCache.getAllPresent(IMMUTABLE_MAP.keySet()));
  }

  @Test
  public void testPut() throws InterruptedException, ExecutionException
  {
    loadingCache.get("key2", new Callable()
    {
      @Override
      public Object call() throws Exception
      {
        return "value2";
      }
    });
    Assert.assertEquals("value2", loadingCache.getIfPresent("key2"));
  }

  @Test
  public void testInvalidate() throws ExecutionException
  {
    loadingCache.get("key2", new Callable()
    {
      @Override
      public Object call() throws Exception
      {
        return "value2";
      }
    });
    Assert.assertEquals("value2", loadingCache.getIfPresent("key2"));
    loadingCache.invalidate("key2");
    Assert.assertEquals(null, loadingCache.getIfPresent("key2"));
  }

  @Test
  public void testInvalidateAll() throws ExecutionException
  {
    loadingCache.get("key2", new Callable()
    {
      @Override
      public Object call() throws Exception
      {
        return "value2";
      }
    });
    Assert.assertEquals("value2", loadingCache.getIfPresent("key2"));
    loadingCache.invalidateAll(Lists.newArrayList("key2"));
    Assert.assertEquals(null, loadingCache.getIfPresent("key2"));
  }

  @Test
  public void testInvalidateAll1() throws ExecutionException
  {
    loadingCache.invalidateAll();
    loadingCache.get("key2", new Callable()
    {
      @Override
      public Object call() throws Exception
      {
        return "value2";
      }
    });
    Assert.assertEquals(loadingCache.getAllPresent(IMMUTABLE_MAP.keySet()), Collections.EMPTY_MAP);
  }

  @Test
  public void testGetStats()
  {
    Assert.assertTrue(loadingCache.getStats() != null && loadingCache.getStats() instanceof LookupCacheStats);
  }

  @Test
  public void testIsClosed()
  {
    Assert.assertFalse(loadingCache.isClosed());
  }

  @Test
  public void testSerDeser() throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertEquals(loadingCache, mapper.reader(LoadingCache.class).readValue(mapper.writeValueAsString(loadingCache)));
    Assert.assertTrue(loadingCache.hashCode() == mapper.reader(LoadingCache.class).readValue(mapper.writeValueAsString(loadingCache)).hashCode());
  }

}

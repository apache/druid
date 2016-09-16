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

package io.druid.server.lookup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.druid.server.lookup.cache.loading.LoadingCache;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class LoadingLookupTest
{
  DataFetcher dataFetcher = EasyMock.createMock(DataFetcher.class);
  LoadingCache lookupCache = EasyMock.createStrictMock(LoadingCache.class);
  LoadingCache reverseLookupCache = EasyMock.createStrictMock(LoadingCache.class);
  LoadingLookup loadingLookup = new LoadingLookup(dataFetcher, lookupCache, reverseLookupCache);

  @Test
  public void testApplyEmptyOrNull()
  {
    Assert.assertEquals(null, loadingLookup.apply(null));
    Assert.assertEquals(null, loadingLookup.apply(""));
  }

  @Test
  public void testUnapplyNull()
  {
    Assert.assertEquals(Collections.EMPTY_LIST, loadingLookup.unapply(null));
  }

  @Test
  public void testApply() throws ExecutionException
  {
    EasyMock.expect(lookupCache.get(EasyMock.eq("key"), EasyMock.anyObject(Callable.class))).andReturn("value").once();
    EasyMock.replay(lookupCache);
    Assert.assertEquals(ImmutableMap.of("key","value"), loadingLookup.applyAll(ImmutableSet.of("key")));
    EasyMock.verify(lookupCache);
  }

  @Test
  public void testUnapplyAll() throws ExecutionException
  {
    EasyMock.expect(reverseLookupCache.get(EasyMock.eq("value"), EasyMock.anyObject(Callable.class)))
            .andReturn(Lists.newArrayList("key"))
            .once();
    EasyMock.replay(reverseLookupCache);
    Assert.assertEquals(ImmutableMap.of("value",Lists.newArrayList("key")), loadingLookup.unapplyAll(ImmutableSet.<String>of("value")));
    EasyMock.verify(reverseLookupCache);
  }

  @Test
  public void testClose()
  {
    lookupCache.close();
    reverseLookupCache.close();
    EasyMock.replay(lookupCache, reverseLookupCache);
    loadingLookup.close();
    EasyMock.verify(lookupCache, reverseLookupCache);
  }

  @Test
  public void testApplyWithExecutionError() throws ExecutionException
  {
    EasyMock.expect(lookupCache.get(EasyMock.eq("key"), EasyMock.anyObject(Callable.class)))
            .andThrow(new ExecutionException(null))
            .once();
    EasyMock.replay(lookupCache);
    Assert.assertEquals(null, loadingLookup.apply("key"));
    EasyMock.verify(lookupCache);
  }

  @Test
  public void testUnApplyWithExecutionError() throws ExecutionException
  {
    EasyMock.expect(reverseLookupCache.get(EasyMock.eq("value"), EasyMock.anyObject(Callable.class)))
            .andThrow(new ExecutionException(null))
            .once();
    EasyMock.replay(reverseLookupCache);
    Assert.assertEquals(Collections.EMPTY_LIST, loadingLookup.unapply("value"));
    EasyMock.verify(reverseLookupCache);
  }

  @Test
  public void testGetCacheKey()
  {
    Assert.assertFalse(Arrays.equals(loadingLookup.getCacheKey(), loadingLookup.getCacheKey()));
  }
}

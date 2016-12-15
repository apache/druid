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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.query.lookup.LookupExtractorFactory;
import io.druid.segment.TestHelper;
import io.druid.server.lookup.cache.loading.LoadingCache;
import io.druid.server.lookup.cache.loading.OffHeapLoadingCache;
import io.druid.server.lookup.cache.loading.OnHeapLoadingCache;
import org.codehaus.jackson.annotate.JsonCreator;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PrefetchedLoadingLookupFactoryTest
{
  PrefetchableFetcher prefetchableFetcher = EasyMock.createMock(PrefetchableFetcher.class);
  LoadingCache lookupCache = EasyMock.createStrictMock(LoadingCache.class);
  LoadingCache reverseLookupCache = EasyMock.createStrictMock(LoadingCache.class);
  PrefetchedLoadingLookup prefetchedLoadingLookup = EasyMock.createMock(PrefetchedLoadingLookup.class);
  LoadingLookupFactory prefetchedLoadingLookupFactory = new PrefetchedLoadingLookupFactory(
      prefetchableFetcher,
      lookupCache,
      reverseLookupCache,
      prefetchedLoadingLookup
  );

  @Test
  public void testStartStop()
  {
    EasyMock.expect(prefetchedLoadingLookup.isOpen()).andReturn(true).once();
    prefetchedLoadingLookup.close();
    EasyMock.expectLastCall().once();
    EasyMock.replay(prefetchedLoadingLookup);
    Assert.assertTrue(prefetchedLoadingLookupFactory.start());
    Assert.assertTrue(prefetchedLoadingLookupFactory.close());
    EasyMock.verify(prefetchedLoadingLookup);

  }

  @Test
  public void testReplacesWithNull()
  {
    Assert.assertTrue(prefetchedLoadingLookupFactory.replaces(null));
  }

  @Test
  public void testReplacesWithSame()
  {
    Assert.assertFalse(prefetchedLoadingLookupFactory.replaces(prefetchedLoadingLookupFactory));
  }

  @Test
  public void testReplacesWithDifferent()
  {
    Assert.assertTrue(prefetchedLoadingLookupFactory.replaces(new PrefetchedLoadingLookupFactory(
        EasyMock.createMock(PrefetchableFetcher.class),
        lookupCache,
        reverseLookupCache
    )));
    Assert.assertTrue(prefetchedLoadingLookupFactory.replaces(new PrefetchedLoadingLookupFactory(
        prefetchableFetcher,
        EasyMock.createMock(LoadingCache.class),
        reverseLookupCache
    )));
    Assert.assertTrue(prefetchedLoadingLookupFactory.replaces(new PrefetchedLoadingLookupFactory(
        prefetchableFetcher,
        lookupCache,
        EasyMock.createMock(LoadingCache.class)
    )));
  }


  @Test
  public void testGet()
  {
    Assert.assertEquals(prefetchedLoadingLookup, prefetchedLoadingLookupFactory.get());
  }

  @Test
  public void testSerDeser() throws IOException
  {
    ObjectMapper mapper = TestHelper.getObjectMapper();
    PrefetchedLoadingLookupFactory prefetchedLoadingLookupFactory = new PrefetchedLoadingLookupFactory(
        new MockPrefetchableFetcher(),
        new OnHeapLoadingCache<String, String>(
            0,
            100,
            100L,
            0L,
            0L
        ),
        new OffHeapLoadingCache<String, List<String>>(
            100,
            100L,
            0L,
            0L
        )
    );

    mapper.registerSubtypes(MockPrefetchableFetcher.class);
    for (Module module: new LookupExtractionModule().getJacksonModules()) {
      mapper.registerModule(module);
    }
    String factoryString = mapper.writeValueAsString(prefetchedLoadingLookupFactory);
    LookupExtractorFactory factory = mapper.reader(LookupExtractorFactory.class).readValue(factoryString);
    Assert.assertEquals(prefetchedLoadingLookupFactory, factory);
  }


  @JsonTypeName("prefetchMock")
  private static class MockPrefetchableFetcher extends PrefetchableFetcher
  {
    @JsonCreator
    public MockPrefetchableFetcher()
    {
    }

    @Override
    public Iterable fetchAll()
    {
      return Collections.emptyMap().entrySet();
    }

    @Override
    public Object fetch(Object key)
    {
      return null;
    }

    @Override
    public Iterable fetch(Iterable keys)
    {
      return null;
    }

    @Override
    public List reverseFetchKeys(Object value)
    {
      return null;
    }

    @Override
    public boolean equals(Object obj)
    {
      return obj instanceof MockPrefetchableFetcher;
    }

    @Override
    public Map prefetch(Object key)
    {
      return null;
    }
  }
}

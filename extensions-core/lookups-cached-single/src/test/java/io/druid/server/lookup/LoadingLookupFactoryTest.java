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

public class LoadingLookupFactoryTest
{
  DataFetcher dataFetcher = EasyMock.createMock(DataFetcher.class);
  LoadingCache lookupCache = EasyMock.createStrictMock(LoadingCache.class);
  LoadingCache reverseLookupCache = EasyMock.createStrictMock(LoadingCache.class);
  LoadingLookup loadingLookup = EasyMock.createMock(LoadingLookup.class);
  LoadingLookupFactory loadingLookupFactory = new LoadingLookupFactory(
      dataFetcher,
      lookupCache,
      reverseLookupCache,
      loadingLookup
  );

  @Test
  public void testStartStop()
  {
    EasyMock.expect(loadingLookup.isOpen()).andReturn(true).once();
    loadingLookup.close();
    EasyMock.expectLastCall().once();
    EasyMock.replay(loadingLookup);
    Assert.assertTrue(loadingLookupFactory.start());
    Assert.assertTrue(loadingLookupFactory.close());
    EasyMock.verify(loadingLookup);

  }

  @Test
  public void testReplacesWithNull()
  {
    Assert.assertTrue(loadingLookupFactory.replaces(null));
  }

  @Test
  public void testReplacesWithSame()
  {
    Assert.assertFalse(loadingLookupFactory.replaces(loadingLookupFactory));
  }

  @Test
  public void testReplacesWithDifferent()
  {
    Assert.assertTrue(loadingLookupFactory.replaces(new LoadingLookupFactory(
        EasyMock.createMock(DataFetcher.class),
        lookupCache,
        reverseLookupCache
    )));
    Assert.assertTrue(loadingLookupFactory.replaces(new LoadingLookupFactory(
        dataFetcher,
        EasyMock.createMock(LoadingCache.class),
        reverseLookupCache
    )));
    Assert.assertTrue(loadingLookupFactory.replaces(new LoadingLookupFactory(
        dataFetcher,
        lookupCache,
        EasyMock.createMock(LoadingCache.class)
    )));
  }


  @Test
  public void testGet()
  {
    Assert.assertEquals(loadingLookup, loadingLookupFactory.get());
  }

  @Test
  public void testSerDeser() throws IOException
  {
    ObjectMapper mapper = TestHelper.getJsonMapper();
    LoadingLookupFactory loadingLookupFactory = new LoadingLookupFactory(
        new MockDataFetcher(),
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

    mapper.registerSubtypes(MockDataFetcher.class);
    mapper.registerSubtypes(LoadingLookupFactory.class);
    Assert.assertEquals(
        loadingLookupFactory,
        mapper.reader(LookupExtractorFactory.class)
              .readValue(mapper.writeValueAsString(loadingLookupFactory))
    );
  }


  @JsonTypeName("mock")
  private static class MockDataFetcher implements DataFetcher
  {
    @JsonCreator
    public MockDataFetcher()
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
      return obj instanceof MockDataFetcher;
    }
  }

}

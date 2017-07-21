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
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.lookup.LookupExtractorFactory;
import io.druid.server.lookup.cache.polling.OffHeapPollingCache;
import io.druid.server.lookup.cache.polling.OnHeapPollingCache;
import io.druid.server.lookup.cache.polling.PollingCacheFactory;
import org.codehaus.jackson.annotate.JsonCreator;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;


@RunWith(Parameterized.class)
public class PollingLookupSerDeserTest
{
  @Parameterized.Parameters
  public static Collection<Object[]> inputData()
  {
    return Arrays.asList(new Object[][]{
        {new OffHeapPollingCache.OffHeapPollingCacheProvider()}, {new OnHeapPollingCache.OnHeapPollingCacheProvider<>()}
    });
  }

  private final PollingCacheFactory cacheFactory;
  private DataFetcher dataFetcher = new MockDataFetcher();

  public PollingLookupSerDeserTest(PollingCacheFactory cacheFactory)
  {
    this.cacheFactory = cacheFactory;
  }

  @Test
  public void testSerDeser() throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    PollingLookupFactory pollingLookupFactory = new PollingLookupFactory(Period.ZERO, dataFetcher, cacheFactory);
    mapper.registerSubtypes(MockDataFetcher.class);
    mapper.registerSubtypes(PollingLookupFactory.class);
    Assert.assertEquals(pollingLookupFactory, mapper.reader(LookupExtractorFactory.class).readValue(mapper.writeValueAsString(pollingLookupFactory)));
  }

  @JsonTypeName("mock")
  private static class MockDataFetcher implements DataFetcher
  {
    @JsonCreator
    public MockDataFetcher()
    {
    }

    @Override
    public Iterable<Map.Entry<Object,Object>> fetchAll()
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

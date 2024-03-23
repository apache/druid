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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.server.lookup.cache.polling.OffHeapPollingCache;
import org.apache.druid.server.lookup.cache.polling.OnHeapPollingCache;
import org.apache.druid.server.lookup.cache.polling.PollingCacheFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class PollingLookupTest extends InitializedNullHandlingTest
{
  private static final Map<String, String> FIRST_LOOKUP_MAP = ImmutableMap.of(
      "foo", "bar",
      "bad", "bar",
      "how about that", "foo",
      "empty string", ""
  );

  private static final Map<String, String> SECOND_LOOKUP_MAP = ImmutableMap.of(
      "new-foo", "new-bar",
      "new-bad", "new-bar"
  );

  private static final long POLL_PERIOD = 1000L;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @JsonTypeName("mock")
  private static class MockDataFetcher implements DataFetcher
  {
    private int callNumber = 0;

    @Override
    public Iterable fetchAll()
    {
      if (callNumber == 0) {
        callNumber++;
        return FIRST_LOOKUP_MAP.entrySet();
      }
      return SECOND_LOOKUP_MAP.entrySet();
    }

    @Nullable
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
    @SuppressWarnings("EqualsHashCode")
    public boolean equals(Object obj)
    {
      return obj instanceof MockDataFetcher;
    }
  }

  @Parameterized.Parameters
  public static Collection<Object[]> inputData()
  {
    return Arrays.asList(new Object[][]{
        {new OffHeapPollingCache.OffHeapPollingCacheProvider()},
        {new OnHeapPollingCache.OnHeapPollingCacheProvider<>()}
    });
  }

  private final PollingCacheFactory pollingCacheFactory;
  private final DataFetcher dataFetcher = new MockDataFetcher();
  private PollingLookup pollingLookup;

  public PollingLookupTest(PollingCacheFactory pollingCacheFactory)
  {
    this.pollingCacheFactory = pollingCacheFactory;
  }

  @Before
  public void setUp()
  {
    pollingLookup = new PollingLookup(POLL_PERIOD, dataFetcher, pollingCacheFactory);
  }

  @After
  public void tearDown()
  {
    if (pollingLookup != null) {
      pollingLookup.close();
    }
    pollingLookup = null;
  }

  @Test(expected = ISE.class)
  public void testClose()
  {
    pollingLookup.close();
    pollingLookup.apply("key");
  }

  @Test
  public void testApply()
  {
    assertMapLookup(FIRST_LOOKUP_MAP, pollingLookup);
  }

  @Test(timeout = POLL_PERIOD * 3)
  public void testApplyAfterDataChange() throws InterruptedException
  {
    assertMapLookup(FIRST_LOOKUP_MAP, pollingLookup);
    Thread.sleep(POLL_PERIOD * 2);
    assertMapLookup(SECOND_LOOKUP_MAP, pollingLookup);
  }

  @Test
  public void testUnapply()
  {
    Assert.assertEquals(
        "reverse lookup should match",
        Sets.newHashSet("foo", "bad"),
        Sets.newHashSet(pollingLookup.unapply("bar"))
    );
    Assert.assertEquals(
        "reverse lookup should match",
        Sets.newHashSet("how about that"),
        Sets.newHashSet(pollingLookup.unapply("foo"))
    );
    Assert.assertEquals(
        "reverse lookup should match",
        Sets.newHashSet("empty string"),
        Sets.newHashSet(pollingLookup.unapply(""))
    );
    Assert.assertEquals(
        "reverse lookup of none existing value should be empty list",
        Collections.emptyList(),
        pollingLookup.unapply("does't exist")
    );
  }

  @Test
  public void testBulkApply()
  {
    Map<String, String> map = pollingLookup.applyAll(FIRST_LOOKUP_MAP.keySet());
    Assert.assertEquals(FIRST_LOOKUP_MAP, Maps.transformValues(map, new Function<String, String>()
    {
      @Override
      public String apply(String input)
      {
        //make sure to rewrite null strings as empty.
        return NullHandling.nullToEmptyIfNeeded(input);
      }
    }));
  }

  @Test
  public void testGetCacheKey()
  {
    PollingLookup pollingLookup2 = new PollingLookup(1L, dataFetcher, pollingCacheFactory);
    Assert.assertFalse(Arrays.equals(pollingLookup2.getCacheKey(), pollingLookup.getCacheKey()));
  }

  @Test
  public void testSupportsAsMap()
  {
    Assert.assertFalse(pollingLookup.supportsAsMap());
  }

  @Test
  public void testAsMap()
  {
    expectedException.expect(UnsupportedOperationException.class);
    pollingLookup.asMap();
  }

  @Test
  public void testEstimateHeapFootprint()
  {
    Assert.assertEquals(
        pollingCacheFactory instanceof OffHeapPollingCache.OffHeapPollingCacheProvider ? 0L : 402L,
        pollingLookup.estimateHeapFootprint()
    );
  }

  private void assertMapLookup(Map<String, String> map, LookupExtractor lookup)
  {
    for (Map.Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String val = entry.getValue();
      Assert.assertEquals("non-null check", NullHandling.emptyToNullIfNeeded(val), lookup.apply(key));
    }
  }
}

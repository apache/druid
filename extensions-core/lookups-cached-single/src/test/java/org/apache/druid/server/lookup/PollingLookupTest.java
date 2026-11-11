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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.server.lookup.cache.polling.OffHeapPollingCache;
import org.apache.druid.server.lookup.cache.polling.OnHeapPollingCache;
import org.apache.druid.server.lookup.cache.polling.PollingCacheFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;

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
    public int hashCode()
    {
      return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
      return obj instanceof MockDataFetcher;
    }
  }

  public static Collection<Object[]> inputData()
  {
    return Arrays.asList(new Object[][]{
        {new OffHeapPollingCache.OffHeapPollingCacheProvider()},
        {new OnHeapPollingCache.OnHeapPollingCacheProvider<>()}
    });
  }

  private PollingCacheFactory pollingCacheFactory;
  private final DataFetcher dataFetcher = new MockDataFetcher();
  private PollingLookup pollingLookup;

  private void initPollingLookupTest(PollingCacheFactory pollingCacheFactory)
  {
    this.pollingCacheFactory = pollingCacheFactory;
    pollingLookup = new PollingLookup(POLL_PERIOD, dataFetcher, pollingCacheFactory);
  }

  @AfterEach
  public void tearDown()
  {
    if (pollingLookup != null) {
      pollingLookup.close();
    }
    pollingLookup = null;
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testClose(PollingCacheFactory pollingCacheFactory)
  {
    initPollingLookupTest(pollingCacheFactory);
    assertThrows(ISE.class, () -> {
      pollingLookup.close();
      pollingLookup.apply("key");
    });
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testApply(PollingCacheFactory pollingCacheFactory)
  {
    initPollingLookupTest(pollingCacheFactory);
    assertMapLookup(FIRST_LOOKUP_MAP, pollingLookup);
  }

  @MethodSource("inputData")
  @ParameterizedTest
  @Timeout(value = POLL_PERIOD * 3, unit = TimeUnit.MILLISECONDS)
  public void testApplyAfterDataChange(PollingCacheFactory pollingCacheFactory) throws InterruptedException
  {
    initPollingLookupTest(pollingCacheFactory);
    assertMapLookup(FIRST_LOOKUP_MAP, pollingLookup);
    Thread.sleep(POLL_PERIOD * 2);
    assertMapLookup(SECOND_LOOKUP_MAP, pollingLookup);
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testUnapply(PollingCacheFactory pollingCacheFactory)
  {
    initPollingLookupTest(pollingCacheFactory);
    Assertions.assertEquals(
        Sets.newHashSet("foo", "bad"),
        Sets.newHashSet(pollingLookup.unapply("bar")),
        "reverse lookup should match"
    );
    Assertions.assertEquals(
        Sets.newHashSet("how about that"),
        Sets.newHashSet(pollingLookup.unapply("foo")),
        "reverse lookup should match"
    );
    Assertions.assertEquals(
        Sets.newHashSet("empty string"),
        Sets.newHashSet(pollingLookup.unapply("")),
        "reverse lookup should match"
    );
    Assertions.assertEquals(
        Collections.emptyList(),
        pollingLookup.unapply("does't exist"),
        "reverse lookup of none existing value should be empty list"
    );
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testBulkApply(PollingCacheFactory pollingCacheFactory)
  {
    initPollingLookupTest(pollingCacheFactory);
    Map<String, String> map = pollingLookup.applyAll(FIRST_LOOKUP_MAP.keySet());
    Assertions.assertEquals(FIRST_LOOKUP_MAP, map);
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testGetCacheKey(PollingCacheFactory pollingCacheFactory)
  {
    initPollingLookupTest(pollingCacheFactory);
    PollingLookup pollingLookup2 = new PollingLookup(1L, dataFetcher, pollingCacheFactory);
    Assertions.assertFalse(Arrays.equals(pollingLookup2.getCacheKey(), pollingLookup.getCacheKey()));
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testSupportsAsMap(PollingCacheFactory pollingCacheFactory)
  {
    initPollingLookupTest(pollingCacheFactory);
    Assertions.assertFalse(pollingLookup.supportsAsMap());
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testAsMap(PollingCacheFactory pollingCacheFactory)
  {
    assertThrows(UnsupportedOperationException.class, () -> {
      initPollingLookupTest(pollingCacheFactory);
      pollingLookup.asMap();
    });
  }

  @MethodSource("inputData")
  @ParameterizedTest
  public void testEstimateHeapFootprint(PollingCacheFactory pollingCacheFactory)
  {
    initPollingLookupTest(pollingCacheFactory);
    Assertions.assertEquals(
        pollingCacheFactory instanceof OffHeapPollingCache.OffHeapPollingCacheProvider ? 0L : 402L,
        pollingLookup.estimateHeapFootprint()
    );
  }

  private void assertMapLookup(Map<String, String> map, LookupExtractor lookup)
  {
    for (Map.Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String val = entry.getValue();
      Assertions.assertEquals(val, lookup.apply(key), "non-null check");
    }
  }
}

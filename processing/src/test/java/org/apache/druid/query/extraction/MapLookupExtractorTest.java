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

package org.apache.druid.query.extraction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.commons.compress.utils.Lists;
import org.apache.druid.common.config.NullHandling;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapLookupExtractorTest
{
  private final Map<String, String> lookupMap =
      ImmutableMap.of(
          "foo", "bar",
          "null", "",
          "empty String", "",
          "", "empty_string"
      );
  private final MapLookupExtractor fn = new MapLookupExtractor(lookupMap, false);

  @BeforeClass
  public static void setUpClass()
  {
    NullHandling.initializeForTests();
  }

  @Test
  public void testUnApply()
  {
    Assert.assertEquals(Collections.singletonList("foo"), unapply("bar"));
    if (NullHandling.sqlCompatible()) {
      Assert.assertEquals(Collections.emptySet(), Sets.newHashSet(unapply(null)));
      Assert.assertEquals(Sets.newHashSet("null", "empty String"), Sets.newHashSet(unapply("")));
    } else {
      // Don't test unapply("") under replace-with-default mode, because it isn't allowed in that mode, and
      // implementation behavior is undefined. unapply is specced such that it requires its inputs to go
      // through nullToEmptyIfNeeded.
      Assert.assertEquals(Sets.newHashSet("null", "empty String"), Sets.newHashSet(unapply(null)));
    }
    Assert.assertEquals(Sets.newHashSet(""), Sets.newHashSet(unapply("empty_string")));
    Assert.assertEquals("not existing value returns empty list", Collections.emptyList(), unapply("not There"));
  }

  @Test
  public void testGetMap()
  {
    Assert.assertEquals(lookupMap, fn.getMap());
  }

  @Test
  public void testApply()
  {
    Assert.assertEquals("bar", fn.apply("foo"));
  }

  @Test
  public void testGetCacheKey()
  {
    final MapLookupExtractor fn2 = new MapLookupExtractor(ImmutableMap.copyOf(lookupMap), false);
    Assert.assertArrayEquals(fn.getCacheKey(), fn2.getCacheKey());
    final MapLookupExtractor fn3 = new MapLookupExtractor(ImmutableMap.of("foo2", "bar"), false);
    Assert.assertFalse(Arrays.equals(fn.getCacheKey(), fn3.getCacheKey()));
    final MapLookupExtractor fn4 = new MapLookupExtractor(ImmutableMap.of("foo", "bar2"), false);
    Assert.assertFalse(Arrays.equals(fn.getCacheKey(), fn4.getCacheKey()));
  }

  @Test
  public void testCanIterate()
  {
    Assert.assertTrue(fn.canIterate());
  }

  @Test
  public void testIterable()
  {
    Assert.assertEquals(
        ImmutableList.copyOf(lookupMap.entrySet()),
        ImmutableList.copyOf(fn.iterable())
    );
  }

  @Test
  public void testEstimateHeapFootprint()
  {
    Assert.assertEquals(0L, new MapLookupExtractor(Collections.emptyMap(), false).estimateHeapFootprint());
    Assert.assertEquals(388L, new MapLookupExtractor(ImmutableMap.copyOf(lookupMap), false).estimateHeapFootprint());
  }

  @Test
  public void testEstimateHeapFootprintStatic()
  {
    Assert.assertEquals(0L, MapLookupExtractor.estimateHeapFootprint(null));
    Assert.assertEquals(0L, MapLookupExtractor.estimateHeapFootprint(Collections.emptyMap()));
    Assert.assertEquals(388L, MapLookupExtractor.estimateHeapFootprint(ImmutableMap.copyOf(lookupMap)));
  }

  @Test
  public void testEstimateHeapFootprintStaticNullKeysAndValues()
  {
    final Map<String, String> mapWithNullKeysAndNullValues = new HashMap<>();
    mapWithNullKeysAndNullValues.put("foo", "bar");
    mapWithNullKeysAndNullValues.put("foo2", null);
    Assert.assertEquals(180L, MapLookupExtractor.estimateHeapFootprint(mapWithNullKeysAndNullValues));
  }

  @Test
  public void testEstimateHeapFootprintStaticNonStringKeysAndValues()
  {
    final Map<Long, Object> mapWithNonStringKeysAndValues = new HashMap<>();
    mapWithNonStringKeysAndValues.put(3L, 1);
    mapWithNonStringKeysAndValues.put(4L, 3.2);
    Assert.assertEquals(160L, MapLookupExtractor.estimateHeapFootprint(mapWithNonStringKeysAndValues));
  }

  @Test
  public void testEquals()
  {
    final MapLookupExtractor fn2 = new MapLookupExtractor(ImmutableMap.copyOf(lookupMap), false);
    Assert.assertEquals(fn, fn2);
    final MapLookupExtractor fn3 = new MapLookupExtractor(ImmutableMap.of("foo2", "bar"), false);
    Assert.assertNotEquals(fn, fn3);
    final MapLookupExtractor fn4 = new MapLookupExtractor(ImmutableMap.of("foo", "bar2"), false);
    Assert.assertNotEquals(fn, fn4);
  }

  @Test
  public void testHashCode()
  {
    final MapLookupExtractor fn2 = new MapLookupExtractor(ImmutableMap.copyOf(lookupMap), false);
    Assert.assertEquals(fn.hashCode(), fn2.hashCode());
    final MapLookupExtractor fn3 = new MapLookupExtractor(ImmutableMap.of("foo2", "bar"), false);
    Assert.assertNotEquals(fn.hashCode(), fn3.hashCode());
    final MapLookupExtractor fn4 = new MapLookupExtractor(ImmutableMap.of("foo", "bar2"), false);
    Assert.assertNotEquals(fn.hashCode(), fn4.hashCode());
  }

  private List<String> unapply(final String s)
  {
    return Lists.newArrayList(fn.unapplyAll(Collections.singleton(s)));
  }
}

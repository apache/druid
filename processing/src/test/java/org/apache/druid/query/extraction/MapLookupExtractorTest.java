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

import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.query.lookup.LookupExtractor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MapLookupExtractorTest extends MapBasedLookupExtractorTest
{
  @Override
  protected LookupExtractor makeLookupExtractor(Map<String, String> map)
  {
    return new MapLookupExtractor(map, false);
  }

  @Test
  public void test_getCacheKey()
  {
    final LookupExtractor fn = makeLookupExtractor(simpleLookupMap);
    final MapLookupExtractor fn2 = new MapLookupExtractor(ImmutableMap.copyOf(simpleLookupMap), false);
    Assert.assertArrayEquals(fn.getCacheKey(), fn2.getCacheKey());
    final MapLookupExtractor fn3 = new MapLookupExtractor(ImmutableMap.of("foo2", "bar"), false);
    Assert.assertFalse(Arrays.equals(fn.getCacheKey(), fn3.getCacheKey()));
    final MapLookupExtractor fn4 = new MapLookupExtractor(ImmutableMap.of("foo", "bar2"), false);
    Assert.assertFalse(Arrays.equals(fn.getCacheKey(), fn4.getCacheKey()));
  }

  @Test
  public void test_estimateHeapFootprint_static()
  {
    Assert.assertEquals(0L, MapLookupExtractor.estimateHeapFootprint(Collections.emptyMap().entrySet()));
    Assert.assertEquals(388L, MapLookupExtractor.estimateHeapFootprint(ImmutableMap.copyOf(simpleLookupMap).entrySet()));
  }

  @Test
  public void test_estimateHeapFootprint_staticNullKeysAndValues()
  {
    final Map<String, String> mapWithNullKeysAndNullValues = new HashMap<>();
    mapWithNullKeysAndNullValues.put("foo", "bar");
    mapWithNullKeysAndNullValues.put("foo2", null);
    Assert.assertEquals(180L, MapLookupExtractor.estimateHeapFootprint(mapWithNullKeysAndNullValues.entrySet()));
  }

  @Test
  public void test_estimateHeapFootprint_staticNonStringKeysAndValues()
  {
    final Map<Long, Object> mapWithNonStringKeysAndValues = new HashMap<>();
    mapWithNonStringKeysAndValues.put(3L, 1);
    mapWithNonStringKeysAndValues.put(4L, 3.2);
    Assert.assertEquals(160L, MapLookupExtractor.estimateHeapFootprint(mapWithNonStringKeysAndValues.entrySet()));
  }

  @Test
  public void test_equalsAndHashCode()
  {
    EqualsVerifier.forClass(MapLookupExtractor.class)
        .usingGetClass()
        .withNonnullFields("map")
        .verify();
  }
}

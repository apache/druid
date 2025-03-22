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
import com.google.common.collect.Sets;
import org.apache.commons.compress.utils.Lists;
import org.apache.druid.query.lookup.ImmutableLookupMap;
import org.apache.druid.query.lookup.LookupExtractor;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base test class for {@link MapLookupExtractor} and {@link ImmutableLookupMap.ImmutableLookupExtractor}.
 */
public abstract class MapBasedLookupExtractorTest
{
  protected final Map<String, String> simpleLookupMap =
      ImmutableMap.of(
          "foo", "bar",
          "null", "",
          "empty String", "",
          "", "empty_string"
      );

  /**
   * Subclasses implement this method to test the proper {@link LookupExtractor} implementation.
   */
  protected abstract LookupExtractor makeLookupExtractor(Map<String, String> map);

  @Test
  public void test_unapplyAll_simple()
  {
    final LookupExtractor lookup = makeLookupExtractor(simpleLookupMap);
    Assert.assertEquals(Collections.singletonList("foo"), unapply(lookup, "bar"));
    Assert.assertEquals(Collections.emptySet(), Sets.newHashSet(unapply(lookup, null)));
    Assert.assertEquals(Sets.newHashSet("null", "empty String"), Sets.newHashSet(unapply(lookup, "")));
    Assert.assertEquals(Sets.newHashSet(""), Sets.newHashSet(unapply(lookup, "empty_string")));
    Assert.assertEquals("not existing value returns empty list", Collections.emptyList(), unapply(lookup, "not There"));
  }

  @Test
  public void test_asMap_simple()
  {
    final LookupExtractor lookup = makeLookupExtractor(simpleLookupMap);
    Assert.assertTrue(lookup.supportsAsMap());
    Assert.assertEquals(simpleLookupMap, lookup.asMap());
  }

  @Test
  public void test_apply_simple()
  {
    final LookupExtractor lookup = makeLookupExtractor(simpleLookupMap);
    Assert.assertEquals("bar", lookup.apply("foo"));
    Assert.assertEquals("", lookup.apply("null"));
    Assert.assertEquals("", lookup.apply("empty String"));
    Assert.assertEquals("empty_string", lookup.apply(""));
    Assert.assertNull(lookup.apply(null));
  }

  @Test
  public void test_apply_nullKey()
  {
    final Map<String, String> mapWithNullKey = new HashMap<>();
    mapWithNullKey.put(null, "nv");
    final LookupExtractor lookup = makeLookupExtractor(mapWithNullKey);

    Assert.assertNull(lookup.apply("missing"));
    Assert.assertNull(lookup.apply(""));
    Assert.assertNull(lookup.apply(null));
  }

  @Test
  public void test_unapply_nullKey()
  {
    final Map<String, String> mapWithNullKey = new HashMap<>();
    mapWithNullKey.put(null, "nv");
    final LookupExtractor lookup = makeLookupExtractor(mapWithNullKey);

    Assert.assertEquals(
        Collections.emptyList(),
        unapply(lookup, "nv")
    );

    Assert.assertEquals(
        Collections.emptyList(),
        unapply(lookup, null)
    );
  }

  @Test
  public void test_apply_nullValue()
  {
    final Map<String, String> mapWithNullKey = new HashMap<>();
    mapWithNullKey.put("nk", null);
    final LookupExtractor lookup = makeLookupExtractor(mapWithNullKey);

    Assert.assertNull(lookup.apply("nk"));
  }

  @Test
  public void test_unapply_nullValue()
  {
    final Map<String, String> mapWithNullKey = new HashMap<>();
    mapWithNullKey.put("nk", null);
    final LookupExtractor lookup = makeLookupExtractor(mapWithNullKey);

    Assert.assertEquals(
        Collections.singletonList("nk"),
        unapply(lookup, null)
    );
  }

  @Test
  public void test_apply_emptyStringValue()
  {
    final Map<String, String> mapWithNullKey = new HashMap<>();
    mapWithNullKey.put("nk", "");
    final LookupExtractor lookup = makeLookupExtractor(mapWithNullKey);

    Assert.assertEquals(
        "",
        lookup.apply("nk")
    );
  }

  @Test
  public void test_unapply_emptyStringValue()
  {
    final Map<String, String> mapWithNullKey = new HashMap<>();
    mapWithNullKey.put("nk", "");
    final LookupExtractor lookup = makeLookupExtractor(mapWithNullKey);

    Assert.assertEquals(
        Collections.emptyList(),
        unapply(lookup, null)
    );

    Assert.assertEquals(
        Collections.singletonList("nk"),
        unapply(lookup, "")
    );
  }

  @Test
  public void test_apply_nullAndEmptyStringKey()
  {
    final Map<String, String> mapWithNullKey = new HashMap<>();
    mapWithNullKey.put(null, "nv");
    mapWithNullKey.put("", "empty");
    final LookupExtractor lookup = makeLookupExtractor(mapWithNullKey);

    Assert.assertNull(lookup.apply("missing"));
    Assert.assertEquals("empty", lookup.apply(""));
    Assert.assertNull(lookup.apply(null));
  }

  @Test
  public void test_unapply_nullAndEmptyStringKey()
  {
    final Map<String, String> mapWithNullKey = new HashMap<>();
    mapWithNullKey.put(null, "nv");
    mapWithNullKey.put("", "empty");
    final LookupExtractor lookup = makeLookupExtractor(mapWithNullKey);

    Assert.assertEquals(
        Collections.singletonList(""),
        unapply(lookup, "empty")
    );

    Assert.assertEquals(
        Collections.emptyList(),
        unapply(lookup, "nv")
    );
  }

  @Test
  public void test_estimateHeapFootprint()
  {
    Assert.assertEquals(0L, makeLookupExtractor(Collections.emptyMap()).estimateHeapFootprint());
    Assert.assertEquals(388L, makeLookupExtractor(simpleLookupMap).estimateHeapFootprint());
  }

  protected List<String> unapply(final LookupExtractor lookup, @Nullable final String s)
  {
    return Lists.newArrayList(lookup.unapplyAll(Collections.singleton(s)));
  }
}

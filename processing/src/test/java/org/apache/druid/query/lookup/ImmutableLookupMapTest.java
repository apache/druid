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

package org.apache.druid.query.lookup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.commons.compress.utils.Lists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ImmutableLookupMapTest
{
  private final Map<String, String> lookupMap =
      ImmutableMap.of(
          "foo", "bar",
          "null", "",
          "empty String", "",
          "", "empty_string"
      );
  private final LookupExtractor fn =
      new ImmutableLookupMap.Builder(lookupMap)
          .build()
          .asLookupExtractor(false, () -> StringUtils.toUtf8("hey"));

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
  public void testApply()
  {
    Assert.assertEquals(NullHandling.sqlCompatible() ? null : "empty_string", fn.apply(null));
    Assert.assertEquals("empty_string", fn.apply(""));
    Assert.assertEquals(NullHandling.emptyToNullIfNeeded(""), fn.apply("null"));
    Assert.assertEquals("bar", fn.apply("foo"));
    Assert.assertNull(fn.apply("nosuchkey"));
  }

  @Test
  public void testGetCacheKey()
  {
    Assert.assertArrayEquals(StringUtils.toUtf8("hey"), fn.getCacheKey());
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
        ImmutableSet.copyOf(lookupMap.entrySet()),
        ImmutableSet.copyOf(fn.iterable())
    );
  }

  @Test
  public void testEstimateHeapFootprint()
  {
    Assert.assertEquals(388L, fn.estimateHeapFootprint());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ImmutableLookupMap.class)
                  .usingGetClass()
                  .withNonnullFields("cacheKeySupplier")
                  .withIgnoredFields("keyToEntry")
                  .verify();
  }

  private List<String> unapply(final String s)
  {
    return Lists.newArrayList(fn.unapplyAll(Collections.singleton(s)));
  }
}

/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.query.extraction;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;


public class MapLookupExtractorTest
{
  private final MapLookupExtractor fn = new MapLookupExtractor(ImmutableMap.of("foo", "bar"));

  @Test
  public void testGetMap() throws Exception
  {
    Assert.assertEquals(ImmutableMap.of("foo", "bar"), fn.getMap());
  }

  @Test
  public void testApply() throws Exception
  {

    Assert.assertEquals("bar", fn.apply("foo"));
  }

  @Test
  public void testGetCacheKey() throws Exception
  {
    final MapLookupExtractor fn2 = new MapLookupExtractor(ImmutableMap.of("foo", "bar"));
    Assert.assertArrayEquals(fn.getCacheKey(), fn2.getCacheKey());
    final MapLookupExtractor fn3 = new MapLookupExtractor(ImmutableMap.of("foo2", "bar"));
    Assert.assertFalse(Arrays.equals(fn.getCacheKey(), fn3.getCacheKey()));
    final MapLookupExtractor fn4 = new MapLookupExtractor(ImmutableMap.of("foo", "bar2"));
    Assert.assertFalse(Arrays.equals(fn.getCacheKey(), fn4.getCacheKey()));
  }

  @Test
  public void testEquals() throws Exception
  {
    final MapLookupExtractor fn2 = new MapLookupExtractor(ImmutableMap.of("foo", "bar"));
    Assert.assertEquals(fn, fn2);
    final MapLookupExtractor fn3 = new MapLookupExtractor(ImmutableMap.of("foo2", "bar"));
    Assert.assertNotEquals(fn, fn3);
    final MapLookupExtractor fn4 = new MapLookupExtractor(ImmutableMap.of("foo", "bar2"));
    Assert.assertNotEquals(fn, fn4);
  }

  @Test
  public void testHashCode() throws Exception
  {
    final MapLookupExtractor fn2 = new MapLookupExtractor(ImmutableMap.of("foo", "bar"));
    Assert.assertEquals(fn.hashCode(), fn2.hashCode());
    final MapLookupExtractor fn3 = new MapLookupExtractor(ImmutableMap.of("foo2", "bar"));
    Assert.assertNotEquals(fn.hashCode(), fn3.hashCode());
    final MapLookupExtractor fn4 = new MapLookupExtractor(ImmutableMap.of("foo", "bar2"));
    Assert.assertNotEquals(fn.hashCode(), fn4.hashCode());
  }
}

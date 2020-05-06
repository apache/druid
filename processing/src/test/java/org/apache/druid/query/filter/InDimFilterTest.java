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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public class InDimFilterTest extends InitializedNullHandlingTest
{
  private ObjectMapper mapper = new DefaultObjectMapper();

  private final String serializedFilter =
      "{\"type\":\"in\",\"dimension\":\"dimTest\",\"values\":[\"bad\",\"good\"]}";

  @Test
  public void testDeserialization() throws IOException
  {
    final InDimFilter actualInDimFilter = mapper.readerFor(DimFilter.class).readValue(serializedFilter);
    final InDimFilter expectedInDimFilter = new InDimFilter("dimTest", Arrays.asList("good", "bad"), null);
    Assert.assertEquals(expectedInDimFilter, actualInDimFilter);
  }

  @Test
  public void testSerialization() throws IOException
  {
    final InDimFilter dimInFilter = new InDimFilter("dimTest", Arrays.asList("good", "bad"), null);
    final String actualSerializedFilter = mapper.writeValueAsString(dimInFilter);
    Assert.assertEquals(serializedFilter, actualSerializedFilter);
  }

  @Test
  public void testGetValuesWithValuesSetOfNonEmptyStringsUseTheGivenSet()
  {
    final Set<String> values = ImmutableSet.of("v1", "v2", "v3");
    final InDimFilter filter = new InDimFilter("dim", values, null, null);
    Assert.assertSame(values, filter.getValues());
  }

  @Test
  public void testGetValuesWithValuesSetIncludingEmptyString()
  {
    final Set<String> values = Sets.newHashSet("v1", "", "v3");
    final InDimFilter filter = new InDimFilter("dim", values, null, null);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertNotSame(values, filter.getValues());
      Assert.assertEquals(Sets.newHashSet("v1", null, "v3"), filter.getValues());
    } else {
      Assert.assertSame(values, filter.getValues());
    }
  }

  @Test
  public void testGetCacheKeyReturningSameKeyForValuesOfDifferentOrders()
  {
    final InDimFilter dimFilter1 = new InDimFilter("dim", ImmutableList.of("v1", "v2"), null);
    final InDimFilter dimFilter2 = new InDimFilter("dim", ImmutableList.of("v2", "v1"), null);
    Assert.assertArrayEquals(dimFilter1.getCacheKey(), dimFilter2.getCacheKey());
  }

  @Test
  public void testGetCacheKeyDifferentKeysForListOfStringsAndSingleStringOfLists()
  {
    final InDimFilter inDimFilter1 = new InDimFilter("dimTest", Arrays.asList("good", "bad"), null);
    final InDimFilter inDimFilter2 = new InDimFilter("dimTest", Collections.singletonList("good,bad"), null);
    Assert.assertFalse(Arrays.equals(inDimFilter1.getCacheKey(), inDimFilter2.getCacheKey()));
  }

  @Test
  public void testGetCacheKeyDifferentKeysForListOfStringsAndSingleStringOfListsWithExtractFn()
  {
    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    final InDimFilter inDimFilter1 = new InDimFilter("dimTest", Arrays.asList("good", "bad"), regexFn);
    final InDimFilter inDimFilter2 = new InDimFilter("dimTest", Collections.singletonList("good,bad"), regexFn);
    Assert.assertFalse(Arrays.equals(inDimFilter1.getCacheKey(), inDimFilter2.getCacheKey()));
  }

  @Test
  public void testGetCacheKeyNullValue() throws IOException
  {
    InDimFilter inDimFilter = mapper.readValue(
        "{\"type\":\"in\",\"dimension\":\"dimTest\",\"values\":[null]}",
        InDimFilter.class
    );
    Assert.assertNotNull(inDimFilter.getCacheKey());
  }

  @Test
  public void testGetCacheKeyReturningDifferentKeysWithAndWithoutNull()
  {
    InDimFilter filter1 = new InDimFilter("dim", Arrays.asList("val", null), null);
    InDimFilter filter2 = new InDimFilter("dim", Collections.singletonList("val"), null);
    Assert.assertFalse(Arrays.equals(filter1.getCacheKey(), filter2.getCacheKey()));
  }

  @Test
  public void testGetCacheKeyReturningCachedCacheKey()
  {
    final InDimFilter filter = new InDimFilter("dim", ImmutableList.of("v1", "v2"), null);
    // Compares the array object, not the elements of the array
    Assert.assertSame(filter.getCacheKey(), filter.getCacheKey());
  }

  @Test
  public void testGetDimensionRangeSetValuesOfDifferentOrdersReturningSameResult()
  {
    final InDimFilter dimFilter1 = new InDimFilter("dim", ImmutableList.of("v1", "v2", "v3"), null);
    final InDimFilter dimFilter2 = new InDimFilter("dim", ImmutableList.of("v3", "v2", "v1"), null);
    Assert.assertEquals(dimFilter1.getDimensionRangeSet("dim"), dimFilter2.getDimensionRangeSet("dim"));
  }

  @Test
  public void testOptimizeSingleValueInToSelector()
  {
    final InDimFilter filter = new InDimFilter("dim", Collections.singleton("v1"), null);
    Assert.assertEquals(new SelectorDimFilter("dim", "v1", null), filter.optimize());
  }
}

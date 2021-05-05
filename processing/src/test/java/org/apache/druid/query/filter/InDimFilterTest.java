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
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
  public void testGetCacheKeyForNullVsEmptyString()
  {
    final InDimFilter inDimFilter1 = new InDimFilter("dimTest", Arrays.asList(null, "abc"), null);
    final InDimFilter inDimFilter2 = new InDimFilter("dimTest", Arrays.asList("", "abc"), null);

    if (NullHandling.sqlCompatible()) {
      Assert.assertFalse(Arrays.equals(inDimFilter1.getCacheKey(), inDimFilter2.getCacheKey()));
    } else {
      Assert.assertArrayEquals(inDimFilter1.getCacheKey(), inDimFilter2.getCacheKey());
    }
  }

  @Test
  public void testGetCacheKeyReturningSameKeyForSetsOfDifferentTypesAndComparators()
  {
    final Set<String> reverseOrderSet = new TreeSet<>(Ordering.natural().reversed());
    final InDimFilter dimFilter1 = new InDimFilter("dim", Sets.newTreeSet(Arrays.asList("v1", "v2")));
    final InDimFilter dimFilter2 = new InDimFilter("dim", Sets.newHashSet("v2", "v1"));
    final InDimFilter dimFilter3 = new InDimFilter("dim", ImmutableSortedSet.copyOf(Arrays.asList("v2", "v1")));
    reverseOrderSet.addAll(Arrays.asList("v1", "v2"));
    final InDimFilter dimFilter4 = new InDimFilter("dim", reverseOrderSet);
    Assert.assertArrayEquals(dimFilter1.getCacheKey(), dimFilter2.getCacheKey());
    Assert.assertArrayEquals(dimFilter1.getCacheKey(), dimFilter3.getCacheKey());
    Assert.assertArrayEquals(dimFilter1.getCacheKey(), dimFilter4.getCacheKey());
  }

  @Test
  public void testGetCacheKeyDifferentKeysForListOfStringsAndSingleStringOfLists()
  {
    final InDimFilter inDimFilter1 = new InDimFilter("dimTest", Arrays.asList("good", "bad"), null);
    final InDimFilter inDimFilter2 = new InDimFilter("dimTest", Collections.singletonList("good,bad"), null);
    Assert.assertFalse(Arrays.equals(inDimFilter1.getCacheKey(), inDimFilter2.getCacheKey()));
  }

  @Test
  public void testGetCacheKeyDifferentKeysForNullAndFourZeroChars()
  {
    final InDimFilter inDimFilter1 = new InDimFilter("dimTest", Arrays.asList(null, "abc"), null);
    final InDimFilter inDimFilter2 = new InDimFilter("dimTest", Arrays.asList("\0\0\0\0", "abc"), null);
    Assert.assertFalse(Arrays.equals(inDimFilter1.getCacheKey(), inDimFilter2.getCacheKey()));
  }

  @Test
  public void testGetCacheKeyDifferentKeysWhenStringBoundariesMove()
  {
    final InDimFilter inDimFilter1 = new InDimFilter("dimTest", Arrays.asList("bar", "foo"), null);
    final InDimFilter inDimFilter2 = new InDimFilter("dimTest", Arrays.asList("barf", "oo"), null);
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

  @Test
  public void testContainsNullWhenValuesSetIsTreeSet()
  {
    // Regression test for NullPointerException caused by programmatically-generated InDimFilters that use
    // TreeSets with natural comparators. These Sets throw NullPointerException on contains(null).
    // InDimFilter wraps these contains methods in null-checking lambdas.

    final TreeSet<String> values = new TreeSet<>();
    values.add("foo");
    values.add("bar");

    final InDimFilter filter = new InDimFilter("dim", values, null);

    final Map<String, Object> row = new HashMap<>();
    row.put("dim", null);

    final RowBasedColumnSelectorFactory<MapBasedRow> columnSelectorFactory = RowBasedColumnSelectorFactory.create(
        RowAdapters.standardRow(),
        () -> new MapBasedRow(0, row),
        RowSignature.builder().add("dim", ValueType.STRING).build(),
        true
    );

    final ValueMatcher matcher = filter.toFilter().makeMatcher(columnSelectorFactory);

    // This would throw an exception without InDimFilter's null-checking lambda wrapping.
    Assert.assertFalse(matcher.matches());

    row.put("dim", "foo");
    // Now it should match.
    Assert.assertTrue(matcher.matches());

    row.put("dim", "fox");
    // Now it *shouldn't* match.
    Assert.assertFalse(matcher.matches());
  }
}

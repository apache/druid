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
import com.google.common.collect.Sets;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.segment.column.BitmapColumnIndex;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.LexicographicalRangeIndex;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.util.Arrays;

public class LikeDimFilterTest extends InitializedNullHandlingTest
{
  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter2 = objectMapper.readValue(objectMapper.writeValueAsString(filter), DimFilter.class);
    Assert.assertEquals(filter, filter2);
  }

  @Test
  public void testGetCacheKey()
  {
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter2 = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter3 = new LikeDimFilter("foo", "bar%", null, new SubstringDimExtractionFn(1, 2));
    Assert.assertArrayEquals(filter.getCacheKey(), filter2.getCacheKey());
    Assert.assertFalse(Arrays.equals(filter.getCacheKey(), filter3.getCacheKey()));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter2 = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter3 = new LikeDimFilter("foo", "bar%", null, new SubstringDimExtractionFn(1, 2));
    Assert.assertEquals(filter, filter2);
    Assert.assertNotEquals(filter, filter3);
    Assert.assertEquals(filter.hashCode(), filter2.hashCode());
    Assert.assertNotEquals(filter.hashCode(), filter3.hashCode());
  }

  @Test
  public void testGetRequiredColumns()
  {
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    Assert.assertEquals(filter.getRequiredColumns(), Sets.newHashSet("foo"));
  }

  @Test
  public void testEqualsContractForExtractionFnDruidPredicateFactory()
  {
    EqualsVerifier.forClass(LikeDimFilter.LikeMatcher.PatternDruidPredicateFactory.class)
                  .withNonnullFields("pattern")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void test_LikeMatcher_equals()
  {
    EqualsVerifier.forClass(LikeDimFilter.LikeMatcher.class)
                  .usingGetClass()
                  .withNonnullFields("suffixMatch", "prefix", "pattern")
                  .verify();
  }

  @Test
  public void testPrefixMatchUsesRangeIndex()
  {
    // An implementation test.
    // This test confirms that "like" filters with prefix matchers use index-range lookups without matcher predicates.

    final Filter likeFilter = new LikeDimFilter("dim0", "f%", null, null, null).toFilter();

    final ColumnIndexSelector indexSelector = Mockito.mock(ColumnIndexSelector.class);
    final ColumnIndexSupplier indexSupplier = Mockito.mock(ColumnIndexSupplier.class);
    final LexicographicalRangeIndex rangeIndex = Mockito.mock(LexicographicalRangeIndex.class);
    final BitmapColumnIndex bitmapColumnIndex = Mockito.mock(BitmapColumnIndex.class);

    Mockito.when(indexSelector.getIndexSupplier("dim0")).thenReturn(indexSupplier);
    Mockito.when(indexSupplier.as(LexicographicalRangeIndex.class)).thenReturn(rangeIndex);
    Mockito.when(
        // Verify that likeFilter uses forRange without a matcher predicate; it's unnecessary and slows things down
        rangeIndex.forRange("f", false, "f" + Character.MAX_VALUE, false)
    ).thenReturn(bitmapColumnIndex);

    final BitmapColumnIndex retVal = likeFilter.getBitmapColumnIndex(indexSelector);
    Assert.assertSame("likeFilter returns the intended bitmapColumnIndex", bitmapColumnIndex, retVal);
  }

  @Test
  public void testExactMatchUsesValueIndex()
  {
    // An implementation test.
    // This test confirms that "like" filters with exact matchers use index lookups.

    final Filter likeFilter = new LikeDimFilter("dim0", "f", null, null, null).toFilter();

    final ColumnIndexSelector indexSelector = Mockito.mock(ColumnIndexSelector.class);
    final ColumnIndexSupplier indexSupplier = Mockito.mock(ColumnIndexSupplier.class);
    final StringValueSetIndex valueIndex = Mockito.mock(StringValueSetIndex.class);
    final BitmapColumnIndex bitmapColumnIndex = Mockito.mock(BitmapColumnIndex.class);

    Mockito.when(indexSelector.getIndexSupplier("dim0")).thenReturn(indexSupplier);
    Mockito.when(indexSupplier.as(StringValueSetIndex.class)).thenReturn(valueIndex);
    Mockito.when(valueIndex.forValue("f")).thenReturn(bitmapColumnIndex);

    final BitmapColumnIndex retVal = likeFilter.getBitmapColumnIndex(indexSelector);
    Assert.assertSame("likeFilter returns the intended bitmapColumnIndex", bitmapColumnIndex, retVal);
  }
}

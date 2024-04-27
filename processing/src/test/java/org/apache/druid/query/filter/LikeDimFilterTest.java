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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.LexicographicalRangeIndexes;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
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
                  .withIgnoredFields("likePattern")
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
    final LexicographicalRangeIndexes rangeIndex = Mockito.mock(LexicographicalRangeIndexes.class);
    final BitmapColumnIndex bitmapColumnIndex = Mockito.mock(BitmapColumnIndex.class);

    Mockito.when(indexSelector.getIndexSupplier("dim0")).thenReturn(indexSupplier);
    Mockito.when(indexSupplier.as(LexicographicalRangeIndexes.class)).thenReturn(rangeIndex);
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
    final StringValueSetIndexes valueIndex = Mockito.mock(StringValueSetIndexes.class);
    final BitmapColumnIndex bitmapColumnIndex = Mockito.mock(BitmapColumnIndex.class);

    Mockito.when(indexSelector.getIndexSupplier("dim0")).thenReturn(indexSupplier);
    Mockito.when(indexSupplier.as(StringValueSetIndexes.class)).thenReturn(valueIndex);
    Mockito.when(valueIndex.forValue("f")).thenReturn(bitmapColumnIndex);

    final BitmapColumnIndex retVal = likeFilter.getBitmapColumnIndex(indexSelector);
    Assert.assertSame("likeFilter returns the intended bitmapColumnIndex", bitmapColumnIndex, retVal);
  }

  @Test
  public void testPatternCompilation()
  {
    assertCompilation("", ":[^$]");
    assertCompilation("a", "a:[^a$]");
    assertCompilation("abc", "abc:[^abc$]");
    assertCompilation("a%", "a:[^a]");
    assertCompilation("%a", ":[a$]");
    assertCompilation("%a%", ":[a]");
    assertCompilation("%_a", ":[.a$]");
    assertCompilation("_%a", ":[^., a$]");
    assertCompilation("_%_a", ":[^., .a$]");
    assertCompilation("abc%", "abc:[^abc]");
    assertCompilation("a%b", "a:[^a, b$]");
    assertCompilation("abc%x", "abc:[^abc, x$]");
    assertCompilation("abc%xyz", "abc:[^abc, xyz$]");
    assertCompilation("____", ":[^....$]");
    assertCompilation("%%%%", ":[]");
    assertCompilation("%_%_%%__", ":[., ., ..$]");
    assertCompilation("%_%a_%bc%_d_", ":[., a., bc, .d.$]");
    assertCompilation("%1 _ 5%6", ":[1 . 5, 6$]");
    assertCompilation("\\%_%a_\\%b\\\\c\\___%_%_d_w%x_y_z", "%:[^\\u0025., a.\\u0025b\\u005Cc_.., ., .d.w, x.y.z$]");
  }

  @Test
  public void testPatternEmpty()
  {
    assertMatch("", null, NullHandling.replaceWithDefault() ? DruidPredicateMatch.TRUE : DruidPredicateMatch.UNKNOWN);
    assertMatch("", "", DruidPredicateMatch.TRUE);
    assertMatch("", "a", DruidPredicateMatch.FALSE);
    assertMatch("", "This is a test!", DruidPredicateMatch.FALSE);
  }

  @Test
  public void testPatternExactMatch()
  {
    assertMatch("a\nb", "a\nb", DruidPredicateMatch.TRUE);
    assertMatch("a\nb", "a\nc", DruidPredicateMatch.FALSE);
    assertMatch("This is a test", "This is a test", DruidPredicateMatch.TRUE);
    assertMatch("This is a test", "this is a test", DruidPredicateMatch.FALSE);
    assertMatch("This is a test", "This is a tes", DruidPredicateMatch.FALSE);
    assertMatch("This is a test", "his is a test", DruidPredicateMatch.FALSE);
    assertMatch("This \\%is a\\_test", "This %is a_test", DruidPredicateMatch.TRUE);
    assertMatch("This \\%is a\\_test", "This \\%is a_test", DruidPredicateMatch.FALSE);
  }

  @Test
  public void testPatternTrickySuffixes()
  {
    assertMatch("%xyz", "abcxyzxyz", DruidPredicateMatch.TRUE);
    assertMatch("ab%bc", "abc", DruidPredicateMatch.FALSE);
  }

  @Test
  public void testPatternOnlySpecial()
  {
    assertMatch("%", null, NullHandling.replaceWithDefault() ? DruidPredicateMatch.TRUE : DruidPredicateMatch.UNKNOWN);
    assertMatch("%", "", DruidPredicateMatch.TRUE);
    assertMatch("%", "abcxyzxyz", DruidPredicateMatch.TRUE);
    assertMatch("_", null, NullHandling.replaceWithDefault() ? DruidPredicateMatch.FALSE : DruidPredicateMatch.UNKNOWN);
    assertMatch("_", "", DruidPredicateMatch.FALSE);
    assertMatch("_", "a", DruidPredicateMatch.TRUE);
    assertMatch("_", "ab", DruidPredicateMatch.FALSE);
    assertMatch("____", "abc", DruidPredicateMatch.FALSE);
    assertMatch("____", "abcd", DruidPredicateMatch.TRUE);
    assertMatch("____", "abcde", DruidPredicateMatch.FALSE);
    assertMatch("%____", "abcde", DruidPredicateMatch.TRUE);
    assertMatch("%____", "abcd", DruidPredicateMatch.TRUE);
    assertMatch("%____", "abc", DruidPredicateMatch.FALSE);
    assertMatch("__%_%%_", "abc", DruidPredicateMatch.FALSE);
    assertMatch("__%_%%_", "abcd", DruidPredicateMatch.TRUE);
    assertMatch("__%_%%_", "abcdxyz", DruidPredicateMatch.TRUE);
    assertMatch("%__%_%%_%", "abc", DruidPredicateMatch.FALSE);
    assertMatch("%__%_%%_%", "abcd", DruidPredicateMatch.TRUE);
    assertMatch("%__%_%%_%", "abcdxyz", DruidPredicateMatch.TRUE);
  }

  @Test
  public void testPatternTrailingWildcard()
  {
    assertMatch("ab%", "abc", DruidPredicateMatch.TRUE);
    assertMatch("ab%", "ab", DruidPredicateMatch.TRUE);
    assertMatch("ab%", "a", DruidPredicateMatch.FALSE);
  }

  @Test
  public void testPatternLeadingWildcard()
  {
    assertMatch("%yz", "xyz", DruidPredicateMatch.TRUE);
    assertMatch("%yz", "yz", DruidPredicateMatch.TRUE);
    assertMatch("%yz", "z", DruidPredicateMatch.FALSE);
    assertMatch("%yz", "wxyz", DruidPredicateMatch.TRUE);
    assertMatch("%yz", "xyza", DruidPredicateMatch.FALSE);
  }

  @Test
  public void testPatternTrailingAny()
  {
    assertMatch("ab_", "abc", DruidPredicateMatch.TRUE);
    assertMatch("ab_", "ab", DruidPredicateMatch.FALSE);
    assertMatch("ab_", "abcd", DruidPredicateMatch.FALSE);
    assertMatch("ab_", "xabc", DruidPredicateMatch.FALSE);
  }

  @Test
  public void testPatternLeadingAny()
  {
    assertMatch("_yz", "xyz", DruidPredicateMatch.TRUE);
    assertMatch("_yz", "yz", DruidPredicateMatch.FALSE);
    assertMatch("_yz", "wxyz", DruidPredicateMatch.FALSE);
    assertMatch("_yz", "xyza", DruidPredicateMatch.FALSE);
  }

  @Test
  public void testPatternLeadingAndTrailing()
  {
    assertMatch("_jkl_", "jkl", DruidPredicateMatch.FALSE);
    assertMatch("_jkl_", "ijklm", DruidPredicateMatch.TRUE);
    assertMatch("_jkl_", "ijklmn", DruidPredicateMatch.FALSE);
    assertMatch("_jkl_", "hijklm", DruidPredicateMatch.FALSE);
    assertMatch("%jkl%", "jkl", DruidPredicateMatch.TRUE);
    assertMatch("%jkl%", "ijklm", DruidPredicateMatch.TRUE);
    assertMatch("%jkl%", "ijklmn", DruidPredicateMatch.TRUE);
    assertMatch("%jkl%", "hijklm", DruidPredicateMatch.TRUE);
    assertMatch("_jkl%", "jkl", DruidPredicateMatch.FALSE);
    assertMatch("_jkl%", "ijklm", DruidPredicateMatch.TRUE);
    assertMatch("_jkl%", "ijklmn", DruidPredicateMatch.TRUE);
    assertMatch("_jkl%", "hijklm", DruidPredicateMatch.FALSE);
    assertMatch("_jkl%", "hijklmn", DruidPredicateMatch.FALSE);
    assertMatch("%jkl_", "jkl", DruidPredicateMatch.FALSE);
    assertMatch("%jkl_", "ijklm", DruidPredicateMatch.TRUE);
    assertMatch("%jkl_", "ijklmn", DruidPredicateMatch.FALSE);
    assertMatch("%jkl_", "hijklm", DruidPredicateMatch.TRUE);
    assertMatch("%jkl_", "hijklmn", DruidPredicateMatch.FALSE);
  }

  @Test
  public void testPatternSuffixWithManyParts()
  {
    assertMatch("%ba_", "foo bar", DruidPredicateMatch.TRUE);
    assertMatch("%ba_", "foo bar daz", DruidPredicateMatch.FALSE);
    assertMatch("%ba_%", "foo bar baz", DruidPredicateMatch.TRUE);
    assertMatch("a%b_d_", "abcde", DruidPredicateMatch.TRUE);
    assertMatch("a%b_d_", "abcdexyzbcde", DruidPredicateMatch.TRUE);
    assertMatch("%b_d_", "abcde", DruidPredicateMatch.TRUE);
    assertMatch("%b_d_", "abcdexyzbcde", DruidPredicateMatch.TRUE);
    assertMatch("%b_d_", "abcdexyzbcdef", DruidPredicateMatch.FALSE);
    assertMatch("%b_d_", "abcdexyzbcd", DruidPredicateMatch.FALSE);
    assertMatch("%z%_b_d_", "abcdexyzabcde", DruidPredicateMatch.TRUE);
    assertMatch("%z%_b_d_", "abcdexyzbcde", DruidPredicateMatch.FALSE);
    assertMatch("%z%_b_d_", "abcdexybcde", DruidPredicateMatch.FALSE);
    assertMatch("%z%_b_d_", "abcdexbcde", DruidPredicateMatch.FALSE);
  }

  @Test
  public void testPatternNoWildcards()
  {
    assertMatch("a_c_e_", "abcdef", DruidPredicateMatch.TRUE);
    assertMatch("a_c_e_", "abcde", DruidPredicateMatch.FALSE);
    assertMatch("x_c_e_", "abcdef", DruidPredicateMatch.FALSE);
    assertMatch("xa_c_e_", "abcdef", DruidPredicateMatch.FALSE);
    assertMatch("a_c_e_x", "abcde", DruidPredicateMatch.FALSE);
  }

  @Test
  public void testPatternFindsCorrectMiddleMatch()
  {
    assertMatch("%km%z", "akmz", DruidPredicateMatch.TRUE);
    assertMatch("%km%z", "akkmz", DruidPredicateMatch.TRUE);
    assertMatch("%xy%yz", "xyz", DruidPredicateMatch.FALSE);
    assertMatch("%xy%yz", "xyyz", DruidPredicateMatch.TRUE);
    assertMatch("%1 _ 5%6", "1 2 3 1 4 5 6", DruidPredicateMatch.TRUE);
    assertMatch("1 _ 5%6", "1 2 3 1 4 5 6", DruidPredicateMatch.FALSE);
  }

  private void assertCompilation(String pattern, String expected)
  {
    LikeDimFilter.LikeMatcher matcher = LikeDimFilter.LikeMatcher.from(pattern, '\\');
    Assert.assertEquals(pattern + " => " + expected, matcher.describeCompilation());
  }

  private void assertMatch(String pattern, String value, DruidPredicateMatch expected)
  {
    LikeDimFilter.LikeMatcher matcher = LikeDimFilter.LikeMatcher.from(pattern, '\\');
    Assert.assertEquals(matcher + " matches " + value, expected, matcher.matches(value));
  }
}

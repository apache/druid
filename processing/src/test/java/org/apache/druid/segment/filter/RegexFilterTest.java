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

package org.apache.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.RegexDimFilter;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;

@RunWith(Parameterized.class)
public class RegexFilterTest extends BaseFilterTest
{
  public RegexFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(testName, DEFAULT_ROWS, indexBuilder, finisher, cnf, optimize);
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @AfterClass
  public static void tearDown() throws Exception
  {
    BaseFilterTest.tearDown(RegexFilterTest.class.getName());
  }

  @Test
  public void testSingleValueStringColumnWithoutNulls()
  {
    assertFilterMatches(new RegexDimFilter("dim0", ".*", null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new RegexDimFilter("dim0", "0", null), ImmutableList.of("0"));
    assertFilterMatches(new RegexDimFilter("dim0", "5", null), ImmutableList.of("5"));
  }

  @Test
  public void testSingleValueStringColumnWithNulls()
  {
    // RegexFilter always returns false for null row values.
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new RegexDimFilter("dim1", ".*", null), ImmutableList.of("1", "2", "3", "4", "5"));
    } else {
      assertFilterMatches(new RegexDimFilter("dim1", ".*", null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    }
    assertFilterMatches(new RegexDimFilter("dim1", "10", null), ImmutableList.of("1"));
    assertFilterMatches(new RegexDimFilter("dim1", "2", null), ImmutableList.of("2"));
    assertFilterMatches(new RegexDimFilter("dim1", "1", null), ImmutableList.of("1", "3"));
    assertFilterMatches(new RegexDimFilter("dim1", ".*def", null), ImmutableList.of("4"));
    assertFilterMatches(new RegexDimFilter("dim1", "abc", null), ImmutableList.of("5"));
    assertFilterMatches(new RegexDimFilter("dim1", "ab.*", null), ImmutableList.of("4", "5"));
  }

  @Test
  public void testMultiValueStringColumn()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new RegexDimFilter("dim2", ".*", null), ImmutableList.of("0", "3", "4"));
    } else {
      assertFilterMatches(new RegexDimFilter("dim2", ".*", null), ImmutableList.of("0", "2", "3", "4"));
    }
    assertFilterMatches(new RegexDimFilter("dim2", "a", null), ImmutableList.of("0", "3"));
    assertFilterMatches(new RegexDimFilter("dim2", "b", null), ImmutableList.of("0"));
    assertFilterMatches(new RegexDimFilter("dim2", "c", null), ImmutableList.of("4"));
    assertFilterMatches(new RegexDimFilter("dim2", "d", null), ImmutableList.of());
  }

  @Test
  public void testMissingColumnSpecifiedInDimensionList()
  {
    assertFilterMatches(new RegexDimFilter("dim3", "", null), ImmutableList.of());
    assertFilterMatches(new RegexDimFilter("dim3", "a", null), ImmutableList.of());
    assertFilterMatches(new RegexDimFilter("dim3", "b", null), ImmutableList.of());
    assertFilterMatches(new RegexDimFilter("dim3", "c", null), ImmutableList.of());
  }

  @Test
  public void testMissingColumnNotSpecifiedInDimensionList()
  {
    assertFilterMatches(new RegexDimFilter("dim4", "", null), ImmutableList.of());
    assertFilterMatches(new RegexDimFilter("dim4", "a", null), ImmutableList.of());
    assertFilterMatches(new RegexDimFilter("dim4", "b", null), ImmutableList.of());
    assertFilterMatches(new RegexDimFilter("dim4", "c", null), ImmutableList.of());
  }

  @Test
  public void testRegexWithExtractionFn()
  {
    String nullJsFn = "function(str) { if (str === null) { return 'NOT_NULL_ANYMORE'; } else { return str;} }";
    ExtractionFn changeNullFn = new JavaScriptExtractionFn(nullJsFn, false, JavaScriptConfig.getEnabledInstance());
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new RegexDimFilter("dim1", ".*ANYMORE", changeNullFn), ImmutableList.of("0"));
      assertFilterMatches(new RegexDimFilter("dim2", ".*ANYMORE", changeNullFn), ImmutableList.of("1", "2", "5"));
    } else {
      assertFilterMatches(new RegexDimFilter("dim1", ".*ANYMORE", changeNullFn), ImmutableList.of());
      assertFilterMatches(new RegexDimFilter("dim2", ".*ANYMORE", changeNullFn), ImmutableList.of("1", "5"));
    }
    assertFilterMatches(new RegexDimFilter("dim1", "ab.*", changeNullFn), ImmutableList.of("4", "5"));

    assertFilterMatches(new RegexDimFilter("dim2", "a.*", changeNullFn), ImmutableList.of("0", "3"));

    assertFilterMatches(new RegexDimFilter("dim3", ".*ANYMORE", changeNullFn), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new RegexDimFilter("dim3", "a.*", changeNullFn), ImmutableList.of());

    assertFilterMatches(new RegexDimFilter("dim4", ".*ANYMORE", changeNullFn), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new RegexDimFilter("dim4", "a.*", changeNullFn), ImmutableList.of());
  }

  @Test
  public void testEqualsContract()
  {
    EqualsVerifier.forClass(RegexFilter.class)
                  .withNonnullFields("pattern")
                  .withIgnoredFields("predicateFactory")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testEqualsContractForPatternDruidPredicateFactory()
  {
    EqualsVerifier.forClass(RegexFilter.PatternDruidPredicateFactory.class)
                  .withNonnullFields("pattern")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testRequiredColumnRewrite()
  {
    Filter filter = new RegexDimFilter("dim0", ".*", null).toFilter();
    Filter filter2 = new RegexDimFilter("dim1", ".*", null).toFilter();

    Assert.assertTrue(filter.supportsRequiredColumnRewrite());
    Assert.assertTrue(filter2.supportsRequiredColumnRewrite());

    Filter rewrittenFilter = filter.rewriteRequiredColumns(ImmutableMap.of("dim0", "dim1"));
    Assert.assertEquals(filter2, rewrittenFilter);

    expectedException.expect(IAE.class);
    expectedException.expectMessage("Received a non-applicable rewrite: {invalidName=dim1}, filter's dimension: dim0");
    filter.rewriteRequiredColumns(ImmutableMap.of("invalidName", "dim1"));
  }
}

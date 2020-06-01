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
import org.apache.druid.query.filter.SearchQueryDimFilter;
import org.apache.druid.query.search.ContainsSearchQuerySpec;
import org.apache.druid.query.search.SearchQuerySpec;
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
public class SearchQueryFilterTest extends BaseFilterTest
{
  public SearchQueryFilterTest(
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
    BaseFilterTest.tearDown(SearchQueryFilterTest.class.getName());
  }

  private SearchQuerySpec specForValue(String value)
  {
    return new ContainsSearchQuerySpec(value, true);
  }
  
  @Test
  public void testSingleValueStringColumnWithoutNulls()
  {
    assertFilterMatches(new SearchQueryDimFilter("dim0", specForValue(""), null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new SearchQueryDimFilter("dim0", specForValue("0"), null), ImmutableList.of("0"));
    assertFilterMatches(new SearchQueryDimFilter("dim0", specForValue("5"), null), ImmutableList.of("5"));
  }

  @Test
  public void testSingleValueStringColumnWithNulls()
  {
    if (NullHandling.replaceWithDefault()) {
      // SearchQueryFilter always returns false for null row values.
      assertFilterMatches(
          new SearchQueryDimFilter("dim1", specForValue(""), null),
          ImmutableList.of("1", "2", "3", "4", "5")
      );
    } else {
      assertFilterMatches(
          new SearchQueryDimFilter("dim1", specForValue(""), null),
          ImmutableList.of("0", "1", "2", "3", "4", "5")
      );
    }
    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue("10"), null), ImmutableList.of("1"));
    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue("2"), null), ImmutableList.of("2"));
    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue("1"), null), ImmutableList.of("1", "3"));
    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue("def"), null), ImmutableList.of("4"));
    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue("abc"), null), ImmutableList.of("5"));
    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue("ab"), null), ImmutableList.of("4", "5"));
  }

  @Test
  public void testMultiValueStringColumn()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new SearchQueryDimFilter("dim2", specForValue(""), null), ImmutableList.of("0", "3", "4"));
    } else {
      assertFilterMatches(
          new SearchQueryDimFilter("dim2", specForValue(""), null),
          ImmutableList.of("0", "2", "3", "4")
      );
    }
    assertFilterMatches(new SearchQueryDimFilter("dim2", specForValue("a"), null), ImmutableList.of("0", "3"));
    assertFilterMatches(new SearchQueryDimFilter("dim2", specForValue("b"), null), ImmutableList.of("0"));
    assertFilterMatches(new SearchQueryDimFilter("dim2", specForValue("c"), null), ImmutableList.of("4"));
    assertFilterMatches(new SearchQueryDimFilter("dim2", specForValue("d"), null), ImmutableList.of());
  }

  @Test
  public void testMissingColumnSpecifiedInDimensionList()
  {
    assertFilterMatches(new SearchQueryDimFilter("dim3", specForValue(""), null), ImmutableList.of());
    assertFilterMatches(new SearchQueryDimFilter("dim3", specForValue("a"), null), ImmutableList.of());
    assertFilterMatches(new SearchQueryDimFilter("dim3", specForValue("b"), null), ImmutableList.of());
    assertFilterMatches(new SearchQueryDimFilter("dim3", specForValue("c"), null), ImmutableList.of());
  }


  @Test
  public void testMissingColumnNotSpecifiedInDimensionList()
  {
    assertFilterMatches(new SearchQueryDimFilter("dim4", specForValue(""), null), ImmutableList.of());
    assertFilterMatches(new SearchQueryDimFilter("dim4", specForValue("a"), null), ImmutableList.of());
    assertFilterMatches(new SearchQueryDimFilter("dim4", specForValue("b"), null), ImmutableList.of());
    assertFilterMatches(new SearchQueryDimFilter("dim4", specForValue("c"), null), ImmutableList.of());
  }


  @Test
  public void testSearchQueryWithExtractionFn()
  {
    String nullJsFn = "function(str) { if (str === null) { return 'NOT_NULL_ANYMORE'; } else { return str;} }";
    ExtractionFn changeNullFn = new JavaScriptExtractionFn(nullJsFn, false, JavaScriptConfig.getEnabledInstance());

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new SearchQueryDimFilter("dim1", specForValue("ANYMORE"), changeNullFn),
          ImmutableList.of("0")
      );
      assertFilterMatches(
          new SearchQueryDimFilter("dim2", specForValue("ANYMORE"), changeNullFn),
          ImmutableList.of("1", "2", "5")
      );

    } else {
      assertFilterMatches(
          new SearchQueryDimFilter("dim1", specForValue("ANYMORE"), changeNullFn),
          ImmutableList.of()
      );
      assertFilterMatches(
          new SearchQueryDimFilter("dim2", specForValue("ANYMORE"), changeNullFn),
          ImmutableList.of("1", "5")
      );
    }

    assertFilterMatches(
        new SearchQueryDimFilter("dim1", specForValue("ab"), changeNullFn),
        ImmutableList.of("4", "5")
    );
    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue("ab"), changeNullFn), ImmutableList.of("4", "5"));

    assertFilterMatches(new SearchQueryDimFilter("dim2", specForValue("a"), changeNullFn), ImmutableList.of("0", "3"));

    assertFilterMatches(new SearchQueryDimFilter("dim3", specForValue("ANYMORE"), changeNullFn), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new SearchQueryDimFilter("dim3", specForValue("a"), changeNullFn), ImmutableList.of());

    assertFilterMatches(new SearchQueryDimFilter("dim4", specForValue("ANYMORE"), changeNullFn), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new SearchQueryDimFilter("dim4", specForValue("a"), changeNullFn), ImmutableList.of());
  }

  @Test
  public void testEqualsContract()
  {
    EqualsVerifier.forClass(SearchQueryFilter.class)
                  .withIgnoredFields("predicateFactory")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testEqualsContractForSearchQueryDruidPredicateFactory()
  {
    EqualsVerifier.forClass(SearchQueryFilter.SearchQueryDruidPredicateFactory.class)
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testRequiredColumnRewrite()
  {
    Filter filter = new SearchQueryDimFilter("dim0", specForValue("a"), null).toFilter();
    Filter filter2 = new SearchQueryDimFilter("dim1", specForValue("a"), null).toFilter();

    Assert.assertTrue(filter.supportsRequiredColumnRewrite());
    Assert.assertTrue(filter2.supportsRequiredColumnRewrite());

    Filter rewrittenFilter = filter.rewriteRequiredColumns(ImmutableMap.of("dim0", "dim1"));
    Assert.assertEquals(filter2, rewrittenFilter);

    expectedException.expect(IAE.class);
    expectedException.expectMessage("Received a non-applicable rewrite: {invalidName=dim1}, filter's dimension: dim0");
    filter.rewriteRequiredColumns(ImmutableMap.of("invalidName", "dim1"));
  }
}

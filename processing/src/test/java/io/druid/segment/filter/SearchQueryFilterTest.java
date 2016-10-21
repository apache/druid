/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.Pair;
import io.druid.js.JavaScriptConfig;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.JavaScriptExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.SearchQueryDimFilter;
import io.druid.query.search.search.ContainsSearchQuerySpec;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.segment.IndexBuilder;
import io.druid.segment.StorageAdapter;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class SearchQueryFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", new DateTime("2000")),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1", "dim2", "dim3")),
              null,
              null
          )
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "0", "dim1", "", "dim2", ImmutableList.of("a", "b"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "1", "dim1", "10", "dim2", ImmutableList.of())),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "2", "dim1", "2", "dim2", ImmutableList.of(""))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "3", "dim1", "1", "dim2", ImmutableList.of("a"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "4", "dim1", "abdef", "dim2", ImmutableList.of("c"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "5", "dim1", "abc"))
  );

  public SearchQueryFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean optimize
  )
  {
    super(testName, ROWS, indexBuilder, finisher, optimize);
  }

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
    // SearchQueryFilter always returns false for null row values.
    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue(""), null), ImmutableList.of("1", "2", "3", "4", "5"));
    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue("10"), null), ImmutableList.of("1"));
    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue("2"), null), ImmutableList.of("2"));
    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue("1"), null), ImmutableList.of("1", "3"));
    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue("def"), null), ImmutableList.of("4"));
    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue("abc"), null), ImmutableList.of("5"));
    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue("ab"), null), ImmutableList.<String>of("4", "5"));
  }

  @Test
  public void testMultiValueStringColumn()
  {
    assertFilterMatches(new SearchQueryDimFilter("dim2", specForValue(""), null), ImmutableList.of("0", "3", "4"));
    assertFilterMatches(new SearchQueryDimFilter("dim2", specForValue("a"), null), ImmutableList.of("0", "3"));
    assertFilterMatches(new SearchQueryDimFilter("dim2", specForValue("b"), null), ImmutableList.of("0"));
    assertFilterMatches(new SearchQueryDimFilter("dim2", specForValue("c"), null), ImmutableList.of("4"));
    assertFilterMatches(new SearchQueryDimFilter("dim2", specForValue("d"), null), ImmutableList.<String>of());
  }

  @Test
  public void testMissingColumnSpecifiedInDimensionList()
  {
    assertFilterMatches(new SearchQueryDimFilter("dim3", specForValue(""), null), ImmutableList.<String>of());
    assertFilterMatches(new SearchQueryDimFilter("dim3", specForValue("a"), null), ImmutableList.<String>of());
    assertFilterMatches(new SearchQueryDimFilter("dim3", specForValue("b"), null), ImmutableList.<String>of());
    assertFilterMatches(new SearchQueryDimFilter("dim3", specForValue("c"), null), ImmutableList.<String>of());
  }


  @Test
  public void testMissingColumnNotSpecifiedInDimensionList()
  {
    assertFilterMatches(new SearchQueryDimFilter("dim4", specForValue(""), null), ImmutableList.<String>of());
    assertFilterMatches(new SearchQueryDimFilter("dim4", specForValue("a"), null), ImmutableList.<String>of());
    assertFilterMatches(new SearchQueryDimFilter("dim4", specForValue("b"), null), ImmutableList.<String>of());
    assertFilterMatches(new SearchQueryDimFilter("dim4", specForValue("c"), null), ImmutableList.<String>of());
  }


  @Test
  public void testSearchQueryWithExtractionFn()
  {
    String nullJsFn = "function(str) { if (str === null) { return 'NOT_NULL_ANYMORE'; } else { return str;} }";
    ExtractionFn changeNullFn = new JavaScriptExtractionFn(nullJsFn, false, JavaScriptConfig.getDefault());

    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue("ANYMORE"), changeNullFn), ImmutableList.of("0"));
    assertFilterMatches(new SearchQueryDimFilter("dim1", specForValue("ab"), changeNullFn), ImmutableList.<String>of("4", "5"));

    assertFilterMatches(new SearchQueryDimFilter("dim2", specForValue("ANYMORE"), changeNullFn),  ImmutableList.of("1", "2", "5"));
    assertFilterMatches(new SearchQueryDimFilter("dim2", specForValue("a"), changeNullFn),  ImmutableList.of("0", "3"));

    assertFilterMatches(new SearchQueryDimFilter("dim3", specForValue("ANYMORE"), changeNullFn),  ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new SearchQueryDimFilter("dim3", specForValue("a"), changeNullFn),  ImmutableList.<String>of());

    assertFilterMatches(new SearchQueryDimFilter("dim4", specForValue("ANYMORE"), changeNullFn),  ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new SearchQueryDimFilter("dim4", specForValue("a"), changeNullFn),  ImmutableList.<String>of());
  }

  private void assertFilterMatches(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    Assert.assertEquals(filter.toString(), expectedRows, selectColumnValuesMatchingFilter(filter, "dim0"));
    Assert.assertEquals(filter.toString(), expectedRows.size(), selectCountUsingFilteredAggregator(filter));
  }
}

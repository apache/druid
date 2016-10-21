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
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.lookup.LookupExtractor;
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
public class JavaScriptFilterTest extends BaseFilterTest
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
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "4", "dim1", "def", "dim2", ImmutableList.of("c"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "5", "dim1", "abc"))
  );

  public JavaScriptFilterTest(
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
    BaseFilterTest.tearDown(JavaScriptFilterTest.class.getName());
  }

  private final String jsNullFilter = "function(x) { return(x === null) }";

  private String jsValueFilter(String value)
  {
    String jsFn = "function(x) { return(x === '" + value + "') }";
    return jsFn;
  }

  @Test
  public void testSingleValueStringColumnWithoutNulls()
  {
    assertFilterMatches(newJavaScriptDimFilter("dim0", jsNullFilter, null), ImmutableList.<String>of());
    assertFilterMatches(newJavaScriptDimFilter("dim0", jsValueFilter(""), null), ImmutableList.<String>of());
    assertFilterMatches(newJavaScriptDimFilter("dim0", jsValueFilter("0"), null), ImmutableList.of("0"));
    assertFilterMatches(newJavaScriptDimFilter("dim0", jsValueFilter("1"), null), ImmutableList.of("1"));
  }

  @Test
  public void testSingleValueStringColumnWithNulls()
  {
    assertFilterMatches(newJavaScriptDimFilter("dim1", jsNullFilter, null), ImmutableList.of("0"));
    assertFilterMatches(newJavaScriptDimFilter("dim1", jsValueFilter("10"), null), ImmutableList.of("1"));
    assertFilterMatches(newJavaScriptDimFilter("dim1", jsValueFilter("2"), null), ImmutableList.of("2"));
    assertFilterMatches(newJavaScriptDimFilter("dim1", jsValueFilter("1"), null), ImmutableList.of("3"));
    assertFilterMatches(newJavaScriptDimFilter("dim1", jsValueFilter("def"), null), ImmutableList.of("4"));
    assertFilterMatches(newJavaScriptDimFilter("dim1", jsValueFilter("abc"), null), ImmutableList.of("5"));
    assertFilterMatches(newJavaScriptDimFilter("dim1", jsValueFilter("ab"), null), ImmutableList.<String>of());
  }

  @Test
  public void testMultiValueStringColumn()
  {
    // multi-val null......
    assertFilterMatches(newJavaScriptDimFilter("dim2", jsNullFilter, null), ImmutableList.of("1", "2", "5"));
    assertFilterMatches(newJavaScriptDimFilter("dim2", jsValueFilter("a"), null), ImmutableList.of("0", "3"));
    assertFilterMatches(newJavaScriptDimFilter("dim2", jsValueFilter("b"), null), ImmutableList.of("0"));
    assertFilterMatches(newJavaScriptDimFilter("dim2", jsValueFilter("c"), null), ImmutableList.of("4"));
    assertFilterMatches(newJavaScriptDimFilter("dim2", jsValueFilter("d"), null), ImmutableList.<String>of());
  }

  @Test
  public void testMissingColumnSpecifiedInDimensionList()
  {
    assertFilterMatches(newJavaScriptDimFilter("dim3", jsNullFilter, null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(newJavaScriptDimFilter("dim3", jsValueFilter("a"), null), ImmutableList.<String>of());
    assertFilterMatches(newJavaScriptDimFilter("dim3", jsValueFilter("b"), null), ImmutableList.<String>of());
    assertFilterMatches(newJavaScriptDimFilter("dim3", jsValueFilter("c"), null), ImmutableList.<String>of());
  }

  @Test
  public void testMissingColumnNotSpecifiedInDimensionList()
  {
    assertFilterMatches(newJavaScriptDimFilter("dim4", jsNullFilter, null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(newJavaScriptDimFilter("dim4", jsValueFilter("a"), null), ImmutableList.<String>of());
    assertFilterMatches(newJavaScriptDimFilter("dim4", jsValueFilter("b"), null), ImmutableList.<String>of());
    assertFilterMatches(newJavaScriptDimFilter("dim4", jsValueFilter("c"), null), ImmutableList.<String>of());
  }

  @Test
  public void testJavascriptFilterWithLookupExtractionFn()
  {
    final Map<String, String> stringMap = ImmutableMap.of(
        "1", "HELLO",
        "a", "HELLO",
        "def", "HELLO",
        "abc", "UNKNOWN"
    );
    LookupExtractor mapExtractor = new MapLookupExtractor(stringMap, false);
    LookupExtractionFn lookupFn = new LookupExtractionFn(mapExtractor, false, "UNKNOWN", false, true);

    assertFilterMatches(newJavaScriptDimFilter("dim0", jsValueFilter("HELLO"), lookupFn), ImmutableList.of("1"));
    assertFilterMatches(newJavaScriptDimFilter("dim0", jsValueFilter("UNKNOWN"), lookupFn), ImmutableList.of("0", "2", "3", "4", "5"));

    assertFilterMatches(newJavaScriptDimFilter("dim1", jsValueFilter("HELLO"), lookupFn), ImmutableList.of("3", "4"));
    assertFilterMatches(newJavaScriptDimFilter("dim1", jsValueFilter("UNKNOWN"), lookupFn), ImmutableList.of("0", "1", "2", "5"));

    assertFilterMatches(newJavaScriptDimFilter("dim2", jsValueFilter("HELLO"), lookupFn), ImmutableList.of("0", "3"));
    assertFilterMatches(newJavaScriptDimFilter("dim2", jsValueFilter("UNKNOWN"), lookupFn), ImmutableList.of("0", "1", "2", "4", "5"));

    assertFilterMatches(newJavaScriptDimFilter("dim3", jsValueFilter("HELLO"), lookupFn), ImmutableList.<String>of());
    assertFilterMatches(newJavaScriptDimFilter("dim3", jsValueFilter("UNKNOWN"), lookupFn), ImmutableList.of("0", "1", "2", "3", "4", "5"));

    assertFilterMatches(newJavaScriptDimFilter("dim4", jsValueFilter("HELLO"), lookupFn), ImmutableList.<String>of());
    assertFilterMatches(newJavaScriptDimFilter("dim4", jsValueFilter("UNKNOWN"), lookupFn), ImmutableList.of("0", "1", "2", "3", "4", "5"));
  }

  private JavaScriptDimFilter newJavaScriptDimFilter(
      final String dimension,
      final String function,
      final ExtractionFn extractionFn
  )
  {
    return new JavaScriptDimFilter(
        dimension,
        function,
        extractionFn,
        JavaScriptConfig.getDefault()
    );
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

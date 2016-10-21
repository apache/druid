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
import com.google.common.collect.Lists;
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
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
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
public class InFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", new DateTime("2000")),
          new DimensionsSpec(null, null, null)
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "a", "dim1", "", "dim2", ImmutableList.of("a", "b"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "b", "dim1", "10", "dim2", ImmutableList.of())),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "c", "dim1", "2", "dim2", ImmutableList.of(""))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "d", "dim1", "1", "dim2", ImmutableList.of("a"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "e", "dim1", "def", "dim2", ImmutableList.of("c"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "f", "dim1", "abc"))
  );

  public InFilterTest(
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
    BaseFilterTest.tearDown(InFilterTest.class.getName());
  }

  @Test
  public void testSingleValueStringColumnWithoutNulls()
  {
    assertFilterMatches(
        toInFilter("dim0", null),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        toInFilter("dim0", "", ""),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        toInFilter("dim0", "a", "c"),
        ImmutableList.of("a", "c")
    );

    assertFilterMatches(
        toInFilter("dim0", "e", "x"),
        ImmutableList.of("e")
    );
  }

  @Test
  public void testSingleValueStringColumnWithNulls()
  {
    assertFilterMatches(
        toInFilter("dim1", null, ""),
        ImmutableList.of("a")
    );

    assertFilterMatches(
        toInFilter("dim1", ""),
        ImmutableList.of("a")
    );

    assertFilterMatches(
        toInFilter("dim1", null, "10", "abc"),
        ImmutableList.of("a", "b", "f")
    );

    assertFilterMatches(
        toInFilter("dim1", "-1", "ab", "de"),
        ImmutableList.<String>of()
    );
  }

  @Test
  public void testMultiValueStringColumn()
  {
    assertFilterMatches(
        toInFilter("dim2", null),
        ImmutableList.of("b", "c", "f")
    );

    assertFilterMatches(
        toInFilter("dim2", "", (String)null),
        ImmutableList.of("b", "c", "f")
    );

    assertFilterMatches(
        toInFilter("dim2", null, "a"),
        ImmutableList.of("a", "b", "c", "d", "f")

    );

    assertFilterMatches(
        toInFilter("dim2", null, "b"),
        ImmutableList.of("a", "b", "c", "f")

    );

    assertFilterMatches(
        toInFilter("dim2", "c"),
        ImmutableList.of("e")
    );

    assertFilterMatches(
        toInFilter("dim2", "d"),
        ImmutableList.<String>of()
    );
  }

  @Test
  public void testMissingColumn()
  {
    assertFilterMatches(
        toInFilter("dim3", null, (String)null),
        ImmutableList.of("a", "b", "c", "d", "e", "f")
    );

    assertFilterMatches(
        toInFilter("dim3", ""),
        ImmutableList.of("a", "b", "c", "d", "e", "f")
    );

    assertFilterMatches(
        toInFilter("dim3", null, "a"),
        ImmutableList.of("a", "b", "c", "d", "e", "f")
    );

    assertFilterMatches(
        toInFilter("dim3", "a"),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        toInFilter("dim3", "b"),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        toInFilter("dim3", "c"),
        ImmutableList.<String>of()
    );
  }

  @Test
  public void testMatchWithExtractionFn()
  {
    String extractionJsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn superFn = new JavaScriptExtractionFn(extractionJsFn, false, JavaScriptConfig.getDefault());

    String nullJsFn = "function(str) { if (str === null) { return 'YES'; } else { return 'NO';} }";
    ExtractionFn yesNullFn = new JavaScriptExtractionFn(nullJsFn, false, JavaScriptConfig.getDefault());

    assertFilterMatches(
        toInFilterWithFn("dim2", superFn, "super-null", "super-a", "super-b"),
        ImmutableList.of("a", "b", "c", "d", "f")
    );

    assertFilterMatches(
        toInFilterWithFn("dim2", yesNullFn, "YES"),
        ImmutableList.of("b", "c", "f")
    );

    assertFilterMatches(
        toInFilterWithFn("dim1", superFn, "super-null", "super-10", "super-def"),
        ImmutableList.of("a", "b", "e")
    );

    assertFilterMatches(
        toInFilterWithFn("dim3", yesNullFn, "NO"),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        toInFilterWithFn("dim3", yesNullFn, "YES"),
        ImmutableList.of("a", "b", "c", "d", "e", "f")
    );

    assertFilterMatches(
        toInFilterWithFn("dim1", yesNullFn, "NO"),
        ImmutableList.of("b", "c", "d", "e", "f")
    );
  }

  @Test
  public void testMatchWithLookupExtractionFn() {
    final Map<String, String> stringMap = ImmutableMap.of(
        "a", "HELLO",
        "10", "HELLO",
        "def", "HELLO",
        "c", "BYE"
    );
    LookupExtractor mapExtractor = new MapLookupExtractor(stringMap, false);
    LookupExtractionFn lookupFn = new LookupExtractionFn(mapExtractor, false, "UNKNOWN", false, true);

    assertFilterMatches(toInFilterWithFn("dim0", lookupFn, null, "HELLO"), ImmutableList.of("a"));
    assertFilterMatches(toInFilterWithFn("dim0", lookupFn, "HELLO", "BYE"), ImmutableList.of("a", "c"));
    assertFilterMatches(toInFilterWithFn("dim0", lookupFn, "UNKNOWN"), ImmutableList.of("b", "d", "e", "f"));
    assertFilterMatches(toInFilterWithFn("dim1", lookupFn, "HELLO"), ImmutableList.of("b", "e"));
    assertFilterMatches(toInFilterWithFn("dim1", lookupFn, "N/A"), ImmutableList.<String>of());
    assertFilterMatches(toInFilterWithFn("dim2", lookupFn, "a"), ImmutableList.<String>of());
    assertFilterMatches(toInFilterWithFn("dim2", lookupFn, "HELLO"), ImmutableList.of("a", "d"));
    assertFilterMatches(toInFilterWithFn("dim2", lookupFn, "HELLO", "BYE", "UNKNOWN"),
            ImmutableList.of("a", "b", "c", "d", "e", "f"));

    final Map<String, String> stringMap2 = ImmutableMap.of(
            "a", "e"
    );
    LookupExtractor mapExtractor2 = new MapLookupExtractor(stringMap2, false);
    LookupExtractionFn lookupFn2 = new LookupExtractionFn(mapExtractor2, true, null, false, true);

    assertFilterMatches(toInFilterWithFn("dim0", lookupFn2, null, "e"), ImmutableList.of("a", "e"));
    assertFilterMatches(toInFilterWithFn("dim0", lookupFn2, "a"), ImmutableList.<String>of());

    final Map<String, String> stringMap3 = ImmutableMap.of(
            "c", "500",
            "100", "e"
    );
    LookupExtractor mapExtractor3 = new MapLookupExtractor(stringMap3, false);
    LookupExtractionFn lookupFn3 = new LookupExtractionFn(mapExtractor3, false, null, false, true);

    assertFilterMatches(toInFilterWithFn("dim0", lookupFn3, null, "c"), ImmutableList.of("a", "b", "d", "e", "f"));
    assertFilterMatches(toInFilterWithFn("dim0", lookupFn3, "e"), ImmutableList.<String>of());

  }

  private DimFilter toInFilter(String dim, String value, String... values)
  {
    return new InDimFilter(dim, Lists.asList(value, values), null);
  }

  private DimFilter toInFilterWithFn(String dim, ExtractionFn fn, String value, String... values)
  {
    return new InDimFilter(dim, Lists.asList(value, values), fn);
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

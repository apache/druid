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
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.extraction.TimeDimExtractionFn;
import io.druid.query.filter.ExtractionDimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.SelectorDimFilter;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class SelectorFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", new DateTime("2000")),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1", "dim2", "dim3", "dim6")),
              null,
              null
          )
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "0", "dim1", "", "dim2", ImmutableList.of("a", "b"), "dim6", "2017-07-25")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "1", "dim1", "10", "dim2", ImmutableList.of(), "dim6", "2017-07-25")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "2", "dim1", "2", "dim2", ImmutableList.of(""), "dim6", "2017-05-25")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "3", "dim1", "1", "dim2", ImmutableList.of("a"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "4", "dim1", "def", "dim2", ImmutableList.of("c"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "5", "dim1", "abc"))
  );

  public SelectorFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(testName, ROWS, indexBuilder, finisher, cnf, optimize);
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    BaseFilterTest.tearDown(SelectorFilterTest.class.getName());
  }

  @Test
  public void testWithTimeExtractionFnNull()
  {
    assertFilterMatches(new SelectorDimFilter("dim0", null, new TimeDimExtractionFn("yyyy-mm-dd", "yyyy-mm")), ImmutableList.<String>of());
    assertFilterMatches(new SelectorDimFilter("dim6", null, new TimeDimExtractionFn("yyyy-mm-dd", "yyyy-mm")), ImmutableList.<String>of("3", "4", "5"));
    assertFilterMatches(new SelectorDimFilter("dim6", "2017-07", new TimeDimExtractionFn("yyyy-mm-dd", "yyyy-mm")), ImmutableList.<String>of("0", "1"));
    assertFilterMatches(new SelectorDimFilter("dim6", "2017-05", new TimeDimExtractionFn("yyyy-mm-dd", "yyyy-mm")), ImmutableList.<String>of("2"));
  }

  @Test
  public void testSingleValueStringColumnWithoutNulls()
  {
    assertFilterMatches(new SelectorDimFilter("dim0", null, null), ImmutableList.<String>of());
    assertFilterMatches(new SelectorDimFilter("dim0", "", null), ImmutableList.<String>of());
    assertFilterMatches(new SelectorDimFilter("dim0", "0", null), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("dim0", "1", null), ImmutableList.of("1"));
  }

  @Test
  public void testSingleValueStringColumnWithNulls()
  {
    assertFilterMatches(new SelectorDimFilter("dim1", null, null), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("dim1", "", null), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("dim1", "10", null), ImmutableList.of("1"));
    assertFilterMatches(new SelectorDimFilter("dim1", "2", null), ImmutableList.of("2"));
    assertFilterMatches(new SelectorDimFilter("dim1", "1", null), ImmutableList.of("3"));
    assertFilterMatches(new SelectorDimFilter("dim1", "def", null), ImmutableList.of("4"));
    assertFilterMatches(new SelectorDimFilter("dim1", "abc", null), ImmutableList.of("5"));
    assertFilterMatches(new SelectorDimFilter("dim1", "ab", null), ImmutableList.<String>of());
  }

  @Test
  public void testMultiValueStringColumn()
  {
    assertFilterMatches(new SelectorDimFilter("dim2", null, null), ImmutableList.of("1", "2", "5"));
    assertFilterMatches(new SelectorDimFilter("dim2", "", null), ImmutableList.of("1", "2", "5"));
    assertFilterMatches(new SelectorDimFilter("dim2", "a", null), ImmutableList.of("0", "3"));
    assertFilterMatches(new SelectorDimFilter("dim2", "b", null), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("dim2", "c", null), ImmutableList.of("4"));
    assertFilterMatches(new SelectorDimFilter("dim2", "d", null), ImmutableList.<String>of());
  }

  @Test
  public void testMissingColumnSpecifiedInDimensionList()
  {
    assertFilterMatches(new SelectorDimFilter("dim3", null, null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new SelectorDimFilter("dim3", "", null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new SelectorDimFilter("dim3", "a", null), ImmutableList.<String>of());
    assertFilterMatches(new SelectorDimFilter("dim3", "b", null), ImmutableList.<String>of());
    assertFilterMatches(new SelectorDimFilter("dim3", "c", null), ImmutableList.<String>of());
  }

  @Test
  public void testMissingColumnNotSpecifiedInDimensionList()
  {
    assertFilterMatches(new SelectorDimFilter("dim4", null, null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new SelectorDimFilter("dim4", "", null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new SelectorDimFilter("dim4", "a", null), ImmutableList.<String>of());
    assertFilterMatches(new SelectorDimFilter("dim4", "b", null), ImmutableList.<String>of());
    assertFilterMatches(new SelectorDimFilter("dim4", "c", null), ImmutableList.<String>of());
  }

  @Test
  public void testExpressionVirtualColumn()
  {
    assertFilterMatches(new SelectorDimFilter("expr", "1.1", null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new SelectorDimFilter("expr", "1.2", null), ImmutableList.<String>of());
  }

  @Test
  public void testSelectorWithLookupExtractionFn()
  {
    final Map<String, String> stringMap = ImmutableMap.of(
        "1", "HELLO",
        "a", "HELLO",
        "def", "HELLO",
        "abc", "UNKNOWN"
    );
    LookupExtractor mapExtractor = new MapLookupExtractor(stringMap, false);
    LookupExtractionFn lookupFn = new LookupExtractionFn(mapExtractor, false, "UNKNOWN", false, true);

    assertFilterMatches(new SelectorDimFilter("dim0", "HELLO", lookupFn), ImmutableList.of("1"));
    assertFilterMatches(new SelectorDimFilter("dim0", "UNKNOWN", lookupFn), ImmutableList.of("0", "2", "3", "4", "5"));

    assertFilterMatches(new SelectorDimFilter("dim1", "HELLO", lookupFn), ImmutableList.of("3", "4"));
    assertFilterMatches(new SelectorDimFilter("dim1", "UNKNOWN", lookupFn), ImmutableList.of("0", "1", "2", "5"));

    assertFilterMatches(new SelectorDimFilter("dim2", "HELLO", lookupFn), ImmutableList.of("0", "3"));
    assertFilterMatches(new SelectorDimFilter("dim2", "UNKNOWN", lookupFn), ImmutableList.of("0", "1", "2", "4", "5"));

    assertFilterMatches(new SelectorDimFilter("dim3", "HELLO", lookupFn), ImmutableList.<String>of());
    assertFilterMatches(new SelectorDimFilter("dim3", "UNKNOWN", lookupFn), ImmutableList.of("0", "1", "2", "3", "4", "5"));

    assertFilterMatches(new SelectorDimFilter("dim4", "HELLO", lookupFn), ImmutableList.<String>of());
    assertFilterMatches(new SelectorDimFilter("dim4", "UNKNOWN", lookupFn), ImmutableList.of("0", "1", "2", "3", "4", "5"));

    final Map<String, String> stringMap2 = ImmutableMap.of(
        "2", "5"
    );
    LookupExtractor mapExtractor2 = new MapLookupExtractor(stringMap2, false);
    LookupExtractionFn lookupFn2 = new LookupExtractionFn(mapExtractor2, true, null, false, true);
    assertFilterMatches(new SelectorDimFilter("dim0", "5", lookupFn2), ImmutableList.of("2", "5"));

    final Map<String, String> stringMap3 = ImmutableMap.of(
        "1", ""
    );
    LookupExtractor mapExtractor3 = new MapLookupExtractor(stringMap3, false);
    LookupExtractionFn lookupFn3 = new LookupExtractionFn(mapExtractor3, false, null, false, true);
    assertFilterMatches(new SelectorDimFilter("dim0", null, lookupFn3), ImmutableList.of("0", "1", "2", "3", "4", "5"));

    final Map<String, String> stringMap4 = ImmutableMap.of(
        "9", "4"
    );
    LookupExtractor mapExtractor4 = new MapLookupExtractor(stringMap4, false);
    LookupExtractionFn lookupFn4 = new LookupExtractionFn(mapExtractor4, true, null, false, true);

    final Map<String, String> stringMap5 = ImmutableMap.of(
        "5", "44"
    );
    LookupExtractor mapExtractor5 = new MapLookupExtractor(stringMap5, false);
    LookupExtractionFn lookupFn5 = new LookupExtractionFn(mapExtractor5, true, null, false, true);

    final Map<String, String> stringMap6 = ImmutableMap.of(
        "5", "5"
    );
    LookupExtractor mapExtractor6 = new MapLookupExtractor(stringMap6, false);
    LookupExtractionFn lookupFn6 = new LookupExtractionFn(mapExtractor6, true, null, false, true);

    // optimize() tests, check that filter was converted to the proper form
    SelectorDimFilter optFilter1 = new SelectorDimFilter("dim1", "UNKNOWN", lookupFn);
    SelectorDimFilter optFilter2 = new SelectorDimFilter("dim0", "5", lookupFn2);
    SelectorDimFilter optFilter3 = new SelectorDimFilter("dim0", null, lookupFn3);
    SelectorDimFilter optFilter4 = new SelectorDimFilter("dim0", "5", lookupFn4);
    SelectorDimFilter optFilter5 = new SelectorDimFilter("dim0", "5", lookupFn5);
    SelectorDimFilter optFilter6 = new SelectorDimFilter("dim0", "5", lookupFn6);

    InDimFilter optFilter2Optimized = new InDimFilter("dim0", Arrays.asList("2", "5"), null);
    SelectorDimFilter optFilter4Optimized = new SelectorDimFilter("dim0", "5", null);
    SelectorDimFilter optFilter6Optimized = new SelectorDimFilter("dim0", "5", null);

    Assert.assertTrue(optFilter1.equals(optFilter1.optimize()));
    Assert.assertTrue(optFilter2Optimized.equals(optFilter2.optimize()));
    Assert.assertTrue(optFilter3.equals(optFilter3.optimize()));
    Assert.assertTrue(optFilter4Optimized.equals(optFilter4.optimize()));
    Assert.assertTrue(optFilter5.equals(optFilter5.optimize()));
    Assert.assertTrue(optFilter6Optimized.equals(optFilter6.optimize()));

    assertFilterMatches(optFilter1, ImmutableList.of("0", "1", "2", "5"));
    assertFilterMatches(optFilter2, ImmutableList.of("2", "5"));
    assertFilterMatches(optFilter3, ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(optFilter4, ImmutableList.of("5"));
    assertFilterMatches(optFilter5, ImmutableList.<String>of());
    assertFilterMatches(optFilter6, ImmutableList.of("5"));

    // tests that ExtractionDimFilter (identical to SelectorDimFilter now) optimize() with lookup works
    // remove these when ExtractionDimFilter is removed.
    assertFilterMatches(new ExtractionDimFilter("dim1", "UNKNOWN", lookupFn, null), ImmutableList.of("0", "1", "2", "5"));
    assertFilterMatches(new ExtractionDimFilter("dim0", "5", lookupFn2, null), ImmutableList.of("2", "5"));
    assertFilterMatches(new ExtractionDimFilter("dim0", null, lookupFn3, null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
  }
}

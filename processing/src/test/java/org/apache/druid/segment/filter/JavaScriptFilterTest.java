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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.JavaScriptDimFilter;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.junit.AfterClass;
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
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", DateTimes.of("2000")),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1", "dim2", "dim3")),
              null,
              null
          )
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parseBatch(ImmutableMap.of("dim0", "0", "dim1", "", "dim2", ImmutableList.of("a", "b"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "1", "dim1", "10", "dim2", ImmutableList.of())).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "2", "dim1", "2", "dim2", ImmutableList.of(""))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "3", "dim1", "1", "dim2", ImmutableList.of("a"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "4", "dim1", "def", "dim2", ImmutableList.of("c"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "5", "dim1", "abc")).get(0)
  );

  public JavaScriptFilterTest(
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
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim0", jsNullFilter, null), ImmutableList.of());
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim0", jsValueFilter(""), null), ImmutableList.of());
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim0", jsValueFilter("0"), null), ImmutableList.of("0"));
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim0", jsValueFilter("1"), null), ImmutableList.of("1"));
  }

  @Test
  public void testSingleValueStringColumnWithNulls()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim1", jsNullFilter, null), ImmutableList.of("0"));
    } else {
      assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim1", jsNullFilter, null), ImmutableList.of());
      assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim1", jsValueFilter(""), null), ImmutableList.of("0"));
    }
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim1", jsValueFilter("10"), null), ImmutableList.of("1"));
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim1", jsValueFilter("2"), null), ImmutableList.of("2"));
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim1", jsValueFilter("1"), null), ImmutableList.of("3"));
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim1", jsValueFilter("def"), null), ImmutableList.of("4"));
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim1", jsValueFilter("abc"), null), ImmutableList.of("5"));
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim1", jsValueFilter("ab"), null), ImmutableList.of());
  }

  @Test
  public void testMultiValueStringColumn()
  {
    // multi-val null......
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatchesSkipVectorize(
          newJavaScriptDimFilter("dim2", jsNullFilter, null),
          ImmutableList.of("1", "2", "5")
      );
    } else {
      assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim2", jsNullFilter, null), ImmutableList.of("1", "5"));
      assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim2", jsValueFilter(""), null), ImmutableList.of("2"));
    }
    assertFilterMatchesSkipVectorize(
        newJavaScriptDimFilter("dim2", jsValueFilter("a"), null),
        ImmutableList.of("0", "3")
    );
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim2", jsValueFilter("b"), null), ImmutableList.of("0"));
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim2", jsValueFilter("c"), null), ImmutableList.of("4"));
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim2", jsValueFilter("d"), null), ImmutableList.of());
  }

  @Test
  public void testMissingColumnSpecifiedInDimensionList()
  {
    assertFilterMatchesSkipVectorize(
        newJavaScriptDimFilter("dim3", jsNullFilter, null),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim3", jsValueFilter("a"), null), ImmutableList.of());
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim3", jsValueFilter("b"), null), ImmutableList.of());
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim3", jsValueFilter("c"), null), ImmutableList.of());
  }

  @Test
  public void testMissingColumnNotSpecifiedInDimensionList()
  {
    assertFilterMatchesSkipVectorize(
        newJavaScriptDimFilter("dim4", jsNullFilter, null),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim4", jsValueFilter("a"), null), ImmutableList.of());
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim4", jsValueFilter("b"), null), ImmutableList.of());
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim4", jsValueFilter("c"), null), ImmutableList.of());
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

    assertFilterMatchesSkipVectorize(
        newJavaScriptDimFilter("dim0", jsValueFilter("HELLO"), lookupFn),
        ImmutableList.of("1")
    );
    assertFilterMatchesSkipVectorize(
        newJavaScriptDimFilter("dim0", jsValueFilter("UNKNOWN"), lookupFn),
        ImmutableList.of("0", "2", "3", "4", "5")
    );

    assertFilterMatchesSkipVectorize(
        newJavaScriptDimFilter("dim1", jsValueFilter("HELLO"), lookupFn),
        ImmutableList.of("3", "4")
    );
    assertFilterMatchesSkipVectorize(
        newJavaScriptDimFilter("dim1", jsValueFilter("UNKNOWN"), lookupFn),
        ImmutableList.of("0", "1", "2", "5")
    );

    assertFilterMatchesSkipVectorize(
        newJavaScriptDimFilter("dim2", jsValueFilter("HELLO"), lookupFn),
        ImmutableList.of("0", "3")
    );
    assertFilterMatchesSkipVectorize(
        newJavaScriptDimFilter("dim2", jsValueFilter("UNKNOWN"), lookupFn),
        ImmutableList.of("0", "1", "2", "4", "5")
    );

    assertFilterMatchesSkipVectorize(
        newJavaScriptDimFilter("dim3", jsValueFilter("HELLO"), lookupFn),
        ImmutableList.of()
    );
    assertFilterMatchesSkipVectorize(
        newJavaScriptDimFilter("dim3", jsValueFilter("UNKNOWN"), lookupFn),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );

    assertFilterMatchesSkipVectorize(
        newJavaScriptDimFilter("dim4", jsValueFilter("HELLO"), lookupFn),
        ImmutableList.of()
    );
    assertFilterMatchesSkipVectorize(
        newJavaScriptDimFilter("dim4", jsValueFilter("UNKNOWN"), lookupFn),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
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
        JavaScriptConfig.getEnabledInstance()
    );
  }
}

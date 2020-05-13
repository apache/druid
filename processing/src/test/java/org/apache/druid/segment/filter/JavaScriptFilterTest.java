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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.JavaScriptDimFilter;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.lookup.LookupExtractor;
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
import java.util.Map;

@RunWith(Parameterized.class)
public class JavaScriptFilterTest extends BaseFilterTest
{
  public JavaScriptFilterTest(
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
    BaseFilterTest.tearDown(JavaScriptFilterTest.class.getName());
  }

  private final String jsNullFilter = "function(x) { return x === null }";

  private String jsValueFilter(String value)
  {
    String jsFn = "function(x) { return x === '" + value + "' }";
    return jsFn;
  }

  private String jsNumericValueFilter(String value)
  {
    String jsFn = "function(x) { return x === " + value + " }";
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
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("dim1", jsValueFilter("abdef"), null), ImmutableList.of("4"));
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
        "abdef", "HELLO",
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

  @Test
  public void testNumericNull()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("f0", jsNullFilter, null), ImmutableList.of());
      assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("d0", jsNullFilter, null), ImmutableList.of());
      assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("l0", jsNullFilter, null), ImmutableList.of());
    } else {
      assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("f0", jsNullFilter, null), ImmutableList.of("4"));
      assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("d0", jsNullFilter, null), ImmutableList.of("2"));
      assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("l0", jsNullFilter, null), ImmutableList.of("3"));
    }
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("f0", jsNumericValueFilter("5.5"), null), ImmutableList.of("2"));
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("d0", jsNumericValueFilter("120.0245"), null), ImmutableList.of("3"));
    assertFilterMatchesSkipVectorize(newJavaScriptDimFilter("l0", jsNumericValueFilter("9001"), null), ImmutableList.of("4"));
  }

  @Test
  public void testEqualsContract()
  {
    EqualsVerifier.forClass(JavaScriptFilter.class)
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testRequiredColumnRewrite()
  {
    Filter filter = newJavaScriptDimFilter("dim3", jsValueFilter("a"), null).toFilter();
    Assert.assertFalse(filter.supportsRequiredColumnRewrite());

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Required column rewrite is not supported by this filter.");
    filter.rewriteRequiredColumns(ImmutableMap.of("invalidName", "dim1"));
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

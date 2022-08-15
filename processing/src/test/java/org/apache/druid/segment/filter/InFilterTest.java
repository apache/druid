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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class InFilterTest extends BaseFilterTest
{
  private static final List<InputRow> ROWS = ImmutableList.of(
      makeDefaultSchemaRow("a", "", ImmutableList.of("a", "b"), "2017-07-25", 0.0, 0.0f, 0L),
      makeDefaultSchemaRow("b", "10", ImmutableList.of(), "2017-07-25", 10.1, 10.1f, 100L),
      makeDefaultSchemaRow("c", "2", ImmutableList.of(""), "2017-05-25", null, 5.5f, 40L),
      makeDefaultSchemaRow("d", "1", ImmutableList.of("a"), "2020-01-25", 120.0245, 110.0f, null),
      makeDefaultSchemaRow("e", "def", ImmutableList.of("c"), null, 60.0, null, 9001L),
      makeDefaultSchemaRow("f", "abc", null, "2020-01-25", 765.432, 123.45f, 12345L)
  );

  public InFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(testName, ROWS, indexBuilder, finisher, cnf, optimize);
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @AfterClass
  public static void tearDown() throws Exception
  {
    BaseFilterTest.tearDown(InFilterTest.class.getName());
  }

  @Test
  public void testSingleValueStringColumnWithoutNulls()
  {
    assertFilterMatches(
        toInFilter("dim0"),
        ImmutableList.of()
    );

    assertFilterMatches(
        toInFilter("dim0", null),
        ImmutableList.of()
    );

    assertFilterMatches(
        toInFilter("dim0", "", ""),
        ImmutableList.of()
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

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          toInFilter("dim1", null, "10", "abc"),
          ImmutableList.of("a", "b", "f")
      );
    } else {
      assertFilterMatches(
          toInFilter("dim1", null, "10", "abc"),
          ImmutableList.of("b", "f")
      );
    }

    assertFilterMatches(
        toInFilter("dim1", "-1", "ab", "de"),
        ImmutableList.of()
    );
  }

  @Test
  public void testMultiValueStringColumn()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          toInFilter("dim2", "b", "d"),
          ImmutableList.of("a")
      );
      assertFilterMatches(
          toInFilter("dim2", null),
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
          toInFilter("dim2", ""),
          ImmutableList.of("b", "c", "f")
      );
    } else {
      assertFilterMatches(
          toInFilter("dim2", null),
          ImmutableList.of("b", "f")
      );
      assertFilterMatches(
          toInFilter("dim2", null, "a"),
          ImmutableList.of("a", "b", "d", "f")
      );
      assertFilterMatches(
          toInFilter("dim2", null, "b"),
          ImmutableList.of("a", "b", "f")
      );
      assertFilterMatches(
          toInFilter("dim2", ""),
          ImmutableList.of("c")
      );
    }

    assertFilterMatches(
        toInFilter("dim2", "", (String) null),
        ImmutableList.of("b", "c", "f")
    );

    assertFilterMatches(
        toInFilter("dim2", "c"),
        ImmutableList.of("e")
    );

    assertFilterMatches(
        toInFilter("dim2", "d"),
        ImmutableList.of()
    );
  }

  @Test
  public void testMissingColumn()
  {
    assertFilterMatches(
        toInFilter("dim3", null, (String) null),
        ImmutableList.of("a", "b", "c", "d", "e", "f")
    );

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          toInFilter("dim3", ""),
          ImmutableList.of("a", "b", "c", "d", "e", "f")
      );
    } else {
      assertFilterMatches(
          toInFilter("dim3", ""),
          ImmutableList.of()
      );
    }

    assertFilterMatches(
        toInFilter("dim3", null, "a"),
        ImmutableList.of("a", "b", "c", "d", "e", "f")
    );

    assertFilterMatches(
        toInFilter("dim3", "a"),
        ImmutableList.of()
    );

    assertFilterMatches(
        toInFilter("dim3", "b"),
        ImmutableList.of()
    );

    assertFilterMatches(
        toInFilter("dim3", "c"),
        ImmutableList.of()
    );
  }

  @Test
  public void testMatchWithExtractionFn()
  {
    String extractionJsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn superFn = new JavaScriptExtractionFn(extractionJsFn, false, JavaScriptConfig.getEnabledInstance());

    String nullJsFn = "function(str) { if (str === null) { return 'YES'; } else { return 'NO';} }";
    ExtractionFn yesNullFn = new JavaScriptExtractionFn(nullJsFn, false, JavaScriptConfig.getEnabledInstance());

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          toInFilterWithFn("dim2", superFn, "super-null", "super-a", "super-b"),
          ImmutableList.of("a", "b", "c", "d", "f")
      );
      assertFilterMatches(
          toInFilterWithFn("dim1", superFn, "super-null", "super-10", "super-def"),
          ImmutableList.of("a", "b", "e")
      );
      assertFilterMatches(
          toInFilterWithFn("dim2", yesNullFn, "YES"),
          ImmutableList.of("b", "c", "f")
      );
      assertFilterMatches(
          toInFilterWithFn("dim1", yesNullFn, "NO"),
          ImmutableList.of("b", "c", "d", "e", "f")
      );
    } else {
      assertFilterMatches(
          toInFilterWithFn("dim2", superFn, "super-null", "super-a", "super-b"),
          ImmutableList.of("a", "b", "d", "f")
      );
      assertFilterMatches(
          toInFilterWithFn("dim1", superFn, "super-null", "super-10", "super-def"),
          ImmutableList.of("b", "e")
      );
      assertFilterMatches(
          toInFilterWithFn("dim2", yesNullFn, "YES"),
          ImmutableList.of("b", "f")
      );

      assertFilterMatches(
          toInFilterWithFn("dim1", yesNullFn, "NO"),
          ImmutableList.of("a", "b", "c", "d", "e", "f")
      );
    }


    assertFilterMatches(
        toInFilterWithFn("dim3", yesNullFn, "NO"),
        ImmutableList.of()
    );

    assertFilterMatches(
        toInFilterWithFn("dim3", yesNullFn, "YES"),
        ImmutableList.of("a", "b", "c", "d", "e", "f")
    );

  }

  @Test
  public void testMatchWithLookupExtractionFn()
  {
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
    assertFilterMatches(toInFilterWithFn("dim1", lookupFn, "N/A"), ImmutableList.of());
    assertFilterMatches(toInFilterWithFn("dim2", lookupFn, "a"), ImmutableList.of());
    assertFilterMatches(toInFilterWithFn("dim2", lookupFn, "HELLO"), ImmutableList.of("a", "d"));
    assertFilterMatches(
        toInFilterWithFn("dim2", lookupFn, "HELLO", "BYE", "UNKNOWN"),
        ImmutableList.of("a", "b", "c", "d", "e", "f")
    );

    final Map<String, String> stringMap2 = ImmutableMap.of(
        "a", "e"
    );
    LookupExtractor mapExtractor2 = new MapLookupExtractor(stringMap2, false);
    LookupExtractionFn lookupFn2 = new LookupExtractionFn(mapExtractor2, true, null, false, true);

    assertFilterMatches(toInFilterWithFn("dim0", lookupFn2, null, "e"), ImmutableList.of("a", "e"));
    assertFilterMatches(toInFilterWithFn("dim0", lookupFn2, "a"), ImmutableList.of());

    final Map<String, String> stringMap3 = ImmutableMap.of(
        "c", "500",
        "100", "e"
    );
    LookupExtractor mapExtractor3 = new MapLookupExtractor(stringMap3, false);
    LookupExtractionFn lookupFn3 = new LookupExtractionFn(mapExtractor3, false, null, false, true);

    assertFilterMatches(toInFilterWithFn("dim0", lookupFn3, null, "c"), ImmutableList.of("a", "b", "d", "e", "f"));
    assertFilterMatches(toInFilterWithFn("dim0", lookupFn3, "e"), ImmutableList.of());

  }

  @Test
  public void testNumericColumnNullsAndDefaults()
  {
    if (canTestNumericNullsAsDefaultValues) {
      assertFilterMatches(new InDimFilter("f0", Sets.newHashSet("0"), null), ImmutableList.of("a", "e"));
      assertFilterMatches(new InDimFilter("d0", Sets.newHashSet("0"), null), ImmutableList.of("a", "c"));
      assertFilterMatches(new InDimFilter("l0", Sets.newHashSet("0"), null), ImmutableList.of("a", "d"));
      assertFilterMatches(new InDimFilter("f0", Collections.singleton(null), null), ImmutableList.of());
      assertFilterMatches(new InDimFilter("d0", Collections.singleton(null), null), ImmutableList.of());
      assertFilterMatches(new InDimFilter("l0", Collections.singleton(null), null), ImmutableList.of());

      assertFilterMatches(new InDimFilter("f0", Sets.newHashSet("0", "999"), null), ImmutableList.of("a", "e"));
      assertFilterMatches(new InDimFilter("d0", Sets.newHashSet("0", "999"), null), ImmutableList.of("a", "c"));
      assertFilterMatches(new InDimFilter("l0", Sets.newHashSet("0", "999"), null), ImmutableList.of("a", "d"));
      assertFilterMatches(new InDimFilter("f0", Sets.newHashSet(null, "999"), null), ImmutableList.of());
      assertFilterMatches(new InDimFilter("d0", Sets.newHashSet(null, "999"), null), ImmutableList.of());
      assertFilterMatches(new InDimFilter("l0", Sets.newHashSet(null, "999"), null), ImmutableList.of());
    } else {
      assertFilterMatches(new InDimFilter("f0", Sets.newHashSet("0"), null), ImmutableList.of("a"));
      assertFilterMatches(new InDimFilter("d0", Sets.newHashSet("0"), null), ImmutableList.of("a"));
      assertFilterMatches(new InDimFilter("l0", Sets.newHashSet("0"), null), ImmutableList.of("a"));
      assertFilterMatches(new InDimFilter("f0", Collections.singleton(null), null), ImmutableList.of("e"));
      assertFilterMatches(new InDimFilter("d0", Collections.singleton(null), null), ImmutableList.of("c"));
      assertFilterMatches(new InDimFilter("l0", Collections.singleton(null), null), ImmutableList.of("d"));

      assertFilterMatches(new InDimFilter("f0", Sets.newHashSet("0", "999"), null), ImmutableList.of("a"));
      assertFilterMatches(new InDimFilter("d0", Sets.newHashSet("0", "999"), null), ImmutableList.of("a"));
      assertFilterMatches(new InDimFilter("l0", Sets.newHashSet("0", "999"), null), ImmutableList.of("a"));
      assertFilterMatches(new InDimFilter("f0", Sets.newHashSet(null, "999"), null), ImmutableList.of("e"));
      assertFilterMatches(new InDimFilter("d0", Sets.newHashSet(null, "999"), null), ImmutableList.of("c"));
      assertFilterMatches(new InDimFilter("l0", Sets.newHashSet(null, "999"), null), ImmutableList.of("d"));
    }
  }

  @Test
  public void testRequiredColumnRewrite()
  {
    InDimFilter filter = (InDimFilter) toInFilter("dim0", "a", "c").toFilter();
    InDimFilter filter2 = (InDimFilter) toInFilter("dim1", "a", "c").toFilter();

    Assert.assertTrue(filter.supportsRequiredColumnRewrite());
    Assert.assertTrue(filter2.supportsRequiredColumnRewrite());

    Filter rewrittenFilter = filter.rewriteRequiredColumns(ImmutableMap.of("dim0", "dim1"));
    Assert.assertEquals(filter2, rewrittenFilter);

    expectedException.expect(IAE.class);
    expectedException.expectMessage("Received a non-applicable rewrite: {invalidName=dim1}, filter's dimension: dim0");
    filter.rewriteRequiredColumns(ImmutableMap.of("invalidName", "dim1"));
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(InDimFilter.class)
                  .usingGetClass()
                  .withNonnullFields("dimension", "values")
                  .withIgnoredFields("cacheKeySupplier", "predicateFactory", "cachedOptimizedFilter", "valuesUtf8")
                  .verify();
  }

  @Test
  public void test_equals_forInFilterDruidPredicateFactory()
  {
    EqualsVerifier.forClass(InDimFilter.InFilterDruidPredicateFactory.class)
                  .usingGetClass()
                  .withNonnullFields("values")
                  .withIgnoredFields(
                      "longPredicateSupplier",
                      "floatPredicateSupplier",
                      "doublePredicateSupplier",
                      "stringPredicateSupplier"
                  )
                  .verify();
  }

  private DimFilter toInFilter(String dim)
  {
    List<String> emptyList = new ArrayList<>();
    return new InDimFilter(dim, emptyList, null);
  }

  private DimFilter toInFilter(String dim, String value, String... values)
  {
    return new InDimFilter(dim, Lists.asList(value, values), null);
  }

  private DimFilter toInFilterWithFn(String dim, ExtractionFn fn, String value, String... values)
  {
    return new InDimFilter(dim, Lists.asList(value, values), fn);
  }
}

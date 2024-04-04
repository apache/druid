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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class InFilterTests
{
  @RunWith(Parameterized.class)
  public static class InFilterTest extends BaseFilterTest
  {
    private static final List<InputRow> ROWS = ImmutableList.of(
        makeDefaultSchemaRow("a", "", ImmutableList.of("a", "b"), "2017-07-25", "", 0.0, 0.0f, 0L),
        makeDefaultSchemaRow("b", "10", ImmutableList.of(), "2017-07-25", "a", 10.1, 10.1f, 100L),
        makeDefaultSchemaRow("c", "2", ImmutableList.of(""), "2017-05-25", null, null, 5.5f, 40L),
        makeDefaultSchemaRow("d", "1", ImmutableList.of("a"), "2020-01-25", "b", 120.0245, 110.0f, null),
        makeDefaultSchemaRow("e", "def", ImmutableList.of("c"), null, "c", 60.0, null, 9001L),
        makeDefaultSchemaRow("f", "abc", null, "2020-01-25", "a", 765.432, 123.45f, 12345L)
    );

    private final ObjectMapper jsonMapper = new DefaultObjectMapper();

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


    @AfterClass
    public static void tearDown() throws Exception
    {
      BaseFilterTest.tearDown(InFilterTest.class.getName());
    }

    @Test
    public void testSingleValueStringColumnWithoutNulls()
    {
      assertFilterMatches(
          inFilter("dim0", ColumnType.STRING, Collections.emptyList()),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(inFilter("dim0", ColumnType.STRING, Collections.emptyList())),
          ImmutableList.of("a", "b", "c", "d", "e", "f")
      );

      assertFilterMatches(
          inFilter("dim0", ColumnType.STRING, Collections.singletonList(null)),
          ImmutableList.of()
      );

      assertFilterMatches(
          inFilter("dim0", ColumnType.STRING, Arrays.asList("", "")),
          ImmutableList.of()
      );

      assertFilterMatches(
          inFilter("dim0", ColumnType.STRING, Arrays.asList("a", "c")),
          ImmutableList.of("a", "c")
      );

      assertFilterMatches(
          inFilter("dim0", ColumnType.STRING, Arrays.asList("e", "x")),
          ImmutableList.of("e")
      );

      assertFilterMatches(
          NotDimFilter.of(inFilter("dim0", ColumnType.STRING, Arrays.asList("e", "x"))),
          ImmutableList.of("a", "b", "c", "d", "f")
      );
    }
    @Test
    public void testSingleValueStringColumnWithNulls()
    {
      assertFilterMatches(
          inFilter("dim1", ColumnType.STRING, Arrays.asList(null, "")),
          ImmutableList.of("a")
      );

      assertFilterMatches(
          inFilter("dim1", ColumnType.STRING, Collections.singletonList("")),
          ImmutableList.of("a")
      );

      assertFilterMatches(
          inFilter("dim1", ColumnType.STRING, Arrays.asList("-1", "ab", "de")),
          ImmutableList.of()
      );

      assertFilterMatches(
          inFilter("s0", ColumnType.STRING, Arrays.asList("a", "b")),
          ImmutableList.of("b", "d", "f")
      );

      assertFilterMatches(
          inFilter("s0", ColumnType.STRING, Collections.singletonList("noexist")),
          ImmutableList.of()
      );

      if (NullHandling.sqlCompatible()) {
        assertFilterMatches(
            inFilter("dim1", ColumnType.STRING, Arrays.asList(null, "10", "abc")),
            ImmutableList.of("b", "f")
        );
        assertFilterMatches(
            NotDimFilter.of(inFilter("dim1", ColumnType.STRING, Arrays.asList("-1", "ab", "de"))),
            ImmutableList.of("a", "b", "c", "d", "e", "f")
        );
        assertFilterMatches(
            NotDimFilter.of(inFilter("s0", ColumnType.STRING, Arrays.asList("a", "b"))),
            ImmutableList.of("a", "e")
        );
        assertFilterMatches(
            NotDimFilter.of(inFilter("s0", ColumnType.STRING, Collections.singletonList("noexist"))),
            ImmutableList.of("a", "b", "d", "e", "f")
        );
      } else {
        // typed in filter doesn't support default value mode, so use classic filter only
        assertLegacyFilterMatches(
            legacyInFilter("dim1", null, "10", "abc"),
            ImmutableList.of("a", "b", "f")
        );
        assertLegacyFilterMatches(
            legacyInFilter("dim1", null, "10", "abc"),
            ImmutableList.of("a", "b", "f")
        );
        assertLegacyFilterMatches(
            NotDimFilter.of(legacyInFilter("dim1", "-1", "ab", "de")),
            ImmutableList.of("a", "b", "c", "d", "e", "f")
        );
        assertLegacyFilterMatches(
            NotDimFilter.of(legacyInFilter("s0", "a", "b")),
            ImmutableList.of("a", "c", "e")
        );
        assertLegacyFilterMatches(
            NotDimFilter.of(legacyInFilter("s0", "noexist")),
            ImmutableList.of("a", "b", "c", "d", "e", "f")
        );
      }
    }

    @Test
    public void testMultiValueStringColumn()
    {
      Assume.assumeFalse(isAutoSchema());

      if (NullHandling.sqlCompatible()) {
        assertFilterMatches(
            inFilter("dim2", ColumnType.STRING, Collections.singletonList(null)),
            ImmutableList.of("b", "f")
        );
        assertFilterMatches(
            inFilter("dim2", ColumnType.STRING, Arrays.asList(null, "a")),
            ImmutableList.of("a", "b", "d", "f")
        );
        assertFilterMatches(
            inFilter("dim2", ColumnType.STRING, Arrays.asList(null, "b")),
            ImmutableList.of("a", "b", "f")
        );
        assertFilterMatches(
            inFilter("dim2", ColumnType.STRING, Collections.singletonList("")),
            ImmutableList.of("c")
        );
      } else {
        assertLegacyFilterMatches(
            legacyInFilter("dim2", "b", "d"),
            ImmutableList.of("a")
        );
        assertLegacyFilterMatches(
            legacyInFilter("dim2", null),
            ImmutableList.of("b", "c", "f")
        );
        assertLegacyFilterMatches(
            legacyInFilter("dim2", null, "a"),
            ImmutableList.of("a", "b", "c", "d", "f")
        );
        assertLegacyFilterMatches(
            legacyInFilter("dim2", null, "b"),
            ImmutableList.of("a", "b", "c", "f")
        );
        assertLegacyFilterMatches(
            legacyInFilter("dim2", ""),
            ImmutableList.of("b", "c", "f")
        );
      }

      assertFilterMatches(
          inFilter("dim2", ColumnType.STRING, Arrays.asList("", null)),
          ImmutableList.of("b", "c", "f")
      );

      assertFilterMatches(
          inFilter("dim2", ColumnType.STRING, Collections.singletonList("c")),
          ImmutableList.of("e")
      );

      assertFilterMatches(
          inFilter("dim2", ColumnType.STRING, Collections.singletonList("d")),
          ImmutableList.of()
      );
    }

    @Test
    public void testMissingColumn()
    {
      assertFilterMatches(
          inFilter("dim3", ColumnType.STRING, Arrays.asList(null, null)),
          ImmutableList.of("a", "b", "c", "d", "e", "f")
      );
      assertFilterMatches(
          NotDimFilter.of(inFilter("dim3", ColumnType.STRING, Arrays.asList(null, null))),
          ImmutableList.of()
      );

      assertFilterMatches(
          inFilter("dim3", ColumnType.STRING, Arrays.asList(null, "a")),
          ImmutableList.of("a", "b", "c", "d", "e", "f")
      );
      assertFilterMatches(
          inFilter("dim3", ColumnType.STRING, Collections.singletonList("a")),
          ImmutableList.of()
      );
      assertFilterMatches(
          inFilter("dim3", ColumnType.STRING, Collections.singletonList("b")),
          ImmutableList.of()
      );
      assertFilterMatches(
          inFilter("dim3", ColumnType.STRING, Collections.singletonList("c")),
          ImmutableList.of()
      );


      if (NullHandling.sqlCompatible()) {
        assertFilterMatches(
            inFilter("dim3", ColumnType.STRING, Collections.singletonList("")),
            ImmutableList.of()
        );
        assertFilterMatches(
            NotDimFilter.of(inFilter("dim3", ColumnType.STRING, Collections.singletonList(""))),
            ImmutableList.of()
        );
        assertFilterMatches(
            NotDimFilter.of(inFilter("dim3", ColumnType.STRING, Collections.singletonList("a"))),
            ImmutableList.of()
        );
        assertFilterMatches(
            NotDimFilter.of(inFilter("dim3", ColumnType.STRING, Arrays.asList(null, "a"))),
            ImmutableList.of()
        );
      } else {
        assertLegacyFilterMatches(
            legacyInFilter("dim3", ""),
            ImmutableList.of("a", "b", "c", "d", "e", "f")
        );
        assertLegacyFilterMatches(
            NotDimFilter.of(legacyInFilter("dim3", "")),
            ImmutableList.of()
        );
        assertLegacyFilterMatches(
            NotDimFilter.of(legacyInFilter("dim3", "a")),
            ImmutableList.of("a", "b", "c", "d", "e", "f")
        );
        assertLegacyFilterMatches(
            NotDimFilter.of(legacyInFilter("dim3", null, "a")),
            ImmutableList.of()
        );
      }
    }

    @Test
    public void testNumeric()
    {
      Assume.assumeTrue(NullHandling.sqlCompatible());
      assertFilterMatches(
          inFilter("f0", ColumnType.FLOAT, Collections.singletonList(0f)),
          ImmutableList.of("a")
      );
      assertFilterMatches(
          inFilter("d0", ColumnType.DOUBLE, Collections.singletonList(0.0)),
          ImmutableList.of("a")
      );
      assertFilterMatches(inFilter("l0", ColumnType.LONG, Collections.singletonList(0L)), ImmutableList.of("a"));
      assertFilterMatches(
          NotDimFilter.of(inFilter("f0", ColumnType.FLOAT, Collections.singletonList(0f))),
          ImmutableList.of("b", "c", "d", "f")
      );
      assertFilterMatches(
          NotDimFilter.of(inFilter("d0", ColumnType.DOUBLE, Collections.singletonList(0.0))),
          ImmutableList.of("b", "d", "e", "f")
      );
      assertFilterMatches(
          NotDimFilter.of(inFilter("l0", ColumnType.LONG, Collections.singletonList(0L))),
          ImmutableList.of("b", "c", "e", "f")
      );
      assertFilterMatches(inFilter("f0", ColumnType.FLOAT, Collections.singletonList(null)), ImmutableList.of("e"));
      assertFilterMatches(inFilter("d0", ColumnType.DOUBLE, Collections.singletonList(null)), ImmutableList.of("c"));
      assertFilterMatches(inFilter("l0", ColumnType.LONG, Collections.singletonList(null)), ImmutableList.of("d"));
      assertFilterMatches(
          NotDimFilter.of(inFilter("f0", ColumnType.FLOAT, Collections.singletonList(null))),
          ImmutableList.of("a", "b", "c", "d", "f")
      );
      assertFilterMatches(
          NotDimFilter.of(inFilter("d0", ColumnType.DOUBLE, Collections.singletonList(null))),
          ImmutableList.of("a", "b", "d", "e", "f")
      );
      assertFilterMatches(
          NotDimFilter.of(inFilter("l0", ColumnType.LONG, Collections.singletonList(null))),
          ImmutableList.of("a", "b", "c", "e", "f")
      );

      assertFilterMatches(inFilter("f0", ColumnType.FLOAT, Arrays.asList("0", "999")), ImmutableList.of("a"));
      assertFilterMatches(inFilter("d0", ColumnType.DOUBLE, Arrays.asList("0", "999")), ImmutableList.of("a"));
      assertFilterMatches(inFilter("l0", ColumnType.LONG, Arrays.asList("0", "999")), ImmutableList.of("a"));
      assertFilterMatches(inFilter("f0", ColumnType.FLOAT, Arrays.asList(null, "999")), ImmutableList.of("e"));
      assertFilterMatches(inFilter("d0", ColumnType.DOUBLE, Arrays.asList(null, "999")), ImmutableList.of("c"));
      assertFilterMatches(inFilter("l0", ColumnType.LONG, Arrays.asList(null, "999")), ImmutableList.of("d"));

      assertFilterMatches(
          inFilter("l0", ColumnType.LONG, Arrays.asList(100L, 9001L)),
          ImmutableList.of("b", "e")
      );
      assertFilterMatches(
          inFilter("l0", ColumnType.FLOAT, Arrays.asList(100.0f, 110.0f)),
          ImmutableList.of("b")
      );
      assertFilterMatches(
          inFilter("l0", ColumnType.DOUBLE, Arrays.asList(100.0, 110.0)),
          ImmutableList.of("b")
      );

      assertFilterMatches(
          inFilter("d0", ColumnType.DOUBLE, Arrays.asList(10.1, 120.0245)),
          ImmutableList.of("b", "d")
      );

      // auto schema doesn't have float columns, so these get kind of funny
      Assume.assumeFalse(isAutoSchema());
      assertFilterMatches(
          inFilter("f0", ColumnType.FLOAT, Arrays.asList(10.1f, 110.0f)),
          ImmutableList.of("b", "d")
      );
      assertFilterMatches(
          inFilter("f0", ColumnType.DOUBLE, Arrays.asList(10.1, 110.0)),
          ImmutableList.of("b", "d")
      );
    }

    @Test
    public void testLegacyNumericDefaults()
    {
      if (canTestNumericNullsAsDefaultValues) {
        assertLegacyFilterMatches(new InDimFilter("f0", Sets.newHashSet("0"), null), ImmutableList.of("a", "e"));
        assertLegacyFilterMatches(new InDimFilter("d0", Sets.newHashSet("0"), null), ImmutableList.of("a", "c"));
        assertLegacyFilterMatches(new InDimFilter("l0", Sets.newHashSet("0"), null), ImmutableList.of("a", "d"));
        assertLegacyFilterMatches(
            NotDimFilter.of(new InDimFilter("f0", Sets.newHashSet("0"), null)),
            ImmutableList.of("b", "c", "d", "f")
        );
        assertLegacyFilterMatches(
            NotDimFilter.of(new InDimFilter("d0", Sets.newHashSet("0"), null)),
            ImmutableList.of("b", "d", "e", "f")
        );
        assertLegacyFilterMatches(
            NotDimFilter.of(new InDimFilter("l0", Sets.newHashSet("0"), null)),
            ImmutableList.of("b", "c", "e", "f")
        );
        assertLegacyFilterMatches(new InDimFilter("f0", Collections.singleton(null), null), ImmutableList.of());
        assertLegacyFilterMatches(new InDimFilter("d0", Collections.singleton(null), null), ImmutableList.of());
        assertLegacyFilterMatches(new InDimFilter("l0", Collections.singleton(null), null), ImmutableList.of());

        assertLegacyFilterMatches(new InDimFilter("f0", Sets.newHashSet("0", "999"), null), ImmutableList.of("a", "e"));
        assertLegacyFilterMatches(new InDimFilter("d0", Sets.newHashSet("0", "999"), null), ImmutableList.of("a", "c"));
        assertLegacyFilterMatches(new InDimFilter("l0", Sets.newHashSet("0", "999"), null), ImmutableList.of("a", "d"));
        assertLegacyFilterMatches(new InDimFilter("f0", Sets.newHashSet(null, "999"), null), ImmutableList.of());
        assertLegacyFilterMatches(new InDimFilter("d0", Sets.newHashSet(null, "999"), null), ImmutableList.of());
        assertLegacyFilterMatches(new InDimFilter("l0", Sets.newHashSet(null, "999"), null), ImmutableList.of());
      } else {
        assertLegacyFilterMatches(new InDimFilter("f0", Sets.newHashSet("0"), null), ImmutableList.of("a"));
        assertLegacyFilterMatches(new InDimFilter("d0", Sets.newHashSet("0"), null), ImmutableList.of("a"));
        assertLegacyFilterMatches(new InDimFilter("l0", Sets.newHashSet("0"), null), ImmutableList.of("a"));
        assertLegacyFilterMatches(
            NotDimFilter.of(new InDimFilter("f0", Sets.newHashSet("0"), null)),
            NullHandling.sqlCompatible()
            ? ImmutableList.of("b", "c", "d", "f")
            : ImmutableList.of("b", "c", "d", "e", "f")
        );
        assertLegacyFilterMatches(
            NotDimFilter.of(new InDimFilter("d0", Sets.newHashSet("0"), null)),
            NullHandling.sqlCompatible()
            ? ImmutableList.of("b", "d", "e", "f")
            : ImmutableList.of("b", "c", "d", "e", "f")
        );
        assertLegacyFilterMatches(
            NotDimFilter.of(new InDimFilter("l0", Sets.newHashSet("0"), null)),
            NullHandling.sqlCompatible()
            ? ImmutableList.of("b", "c", "e", "f")
            : ImmutableList.of("b", "c", "d", "e", "f")
        );
        assertLegacyFilterMatches(new InDimFilter("f0", Collections.singleton(null), null), ImmutableList.of("e"));
        assertLegacyFilterMatches(new InDimFilter("d0", Collections.singleton(null), null), ImmutableList.of("c"));
        assertLegacyFilterMatches(new InDimFilter("l0", Collections.singleton(null), null), ImmutableList.of("d"));
        assertLegacyFilterMatches(
            NotDimFilter.of(new InDimFilter("f0", Collections.singleton(null), null)),
            ImmutableList.of("a", "b", "c", "d", "f")
        );
        assertLegacyFilterMatches(
            NotDimFilter.of(new InDimFilter("d0", Collections.singleton(null), null)),
            ImmutableList.of("a", "b", "d", "e", "f")
        );
        assertLegacyFilterMatches(
            NotDimFilter.of(new InDimFilter("l0", Collections.singleton(null), null)),
            ImmutableList.of("a", "b", "c", "e", "f")
        );

        assertLegacyFilterMatches(new InDimFilter("f0", Sets.newHashSet("0", "999"), null), ImmutableList.of("a"));
        assertLegacyFilterMatches(new InDimFilter("d0", Sets.newHashSet("0", "999"), null), ImmutableList.of("a"));
        assertLegacyFilterMatches(new InDimFilter("l0", Sets.newHashSet("0", "999"), null), ImmutableList.of("a"));
        assertLegacyFilterMatches(new InDimFilter("f0", Sets.newHashSet(null, "999"), null), ImmutableList.of("e"));
        assertLegacyFilterMatches(new InDimFilter("d0", Sets.newHashSet(null, "999"), null), ImmutableList.of("c"));
        assertLegacyFilterMatches(new InDimFilter("l0", Sets.newHashSet(null, "999"), null), ImmutableList.of("d"));
      }
    }
    @Test
    public void testLegacyMatchWithExtractionFn()
    {
      String extractionJsFn = "function(str) { return 'super-' + str; }";
      ExtractionFn superFn = new JavaScriptExtractionFn(extractionJsFn, false, JavaScriptConfig.getEnabledInstance());

      String nullJsFn = "function(str) { if (str === null) { return 'YES'; } else { return 'NO';} }";
      ExtractionFn yesNullFn = new JavaScriptExtractionFn(nullJsFn, false, JavaScriptConfig.getEnabledInstance());

      if (NullHandling.replaceWithDefault()) {
        assertFilterMatchesSkipArrays(
            legacyInFilterWithFn("dim2", superFn, "super-null", "super-a", "super-b"),
            ImmutableList.of("a", "b", "c", "d", "f")
        );
        assertFilterMatchesSkipArrays(
            NotDimFilter.of(legacyInFilterWithFn("dim2", superFn, "super-null", "super-a", "super-b")),
            ImmutableList.of("e")
        );
        assertFilterMatchesSkipArrays(
            legacyInFilterWithFn("dim2", yesNullFn, "YES"),
            ImmutableList.of("b", "c", "f")
        );
        assertFilterMatchesSkipArrays(
            NotDimFilter.of(legacyInFilterWithFn("dim2", yesNullFn, "YES")),
            ImmutableList.of("a", "d", "e")
        );
        assertLegacyFilterMatches(
            legacyInFilterWithFn("dim1", superFn, "super-null", "super-10", "super-def"),
            ImmutableList.of("a", "b", "e")
        );
        assertLegacyFilterMatches(
            legacyInFilterWithFn("dim1", yesNullFn, "NO"),
            ImmutableList.of("b", "c", "d", "e", "f")
        );
      } else {
        assertFilterMatchesSkipArrays(
            legacyInFilterWithFn("dim2", superFn, "super-null", "super-a", "super-b"),
            ImmutableList.of("a", "b", "d", "f")
        );
        assertFilterMatchesSkipArrays(
            NotDimFilter.of(legacyInFilterWithFn("dim2", superFn, "super-null", "super-a", "super-b")),
            ImmutableList.of("c", "e")
        );
        assertFilterMatchesSkipArrays(
            legacyInFilterWithFn("dim2", yesNullFn, "YES"),
            ImmutableList.of("b", "f")
        );
        assertFilterMatchesSkipArrays(
            NotDimFilter.of(legacyInFilterWithFn("dim2", yesNullFn, "YES")),
            ImmutableList.of("a", "c", "d", "e")
        );
        assertLegacyFilterMatches(
            legacyInFilterWithFn("dim1", superFn, "super-null", "super-10", "super-def"),
            ImmutableList.of("b", "e")
        );

        assertLegacyFilterMatches(
            legacyInFilterWithFn("dim1", yesNullFn, "NO"),
            ImmutableList.of("a", "b", "c", "d", "e", "f")
        );
      }

      assertLegacyFilterMatches(
          legacyInFilterWithFn("dim3", yesNullFn, "NO"),
          ImmutableList.of()
      );
      assertLegacyFilterMatches(
          NotDimFilter.of(legacyInFilterWithFn("dim3", yesNullFn, "NO")),
          ImmutableList.of("a", "b", "c", "d", "e", "f")
      );
      assertLegacyFilterMatches(
          legacyInFilterWithFn("dim3", yesNullFn, "YES"),
          ImmutableList.of("a", "b", "c", "d", "e", "f")
      );

    }

    @Test
    public void testLegacyMatchWithLookupExtractionFn()
    {
      final Map<String, String> stringMap = ImmutableMap.of(
          "a", "HELLO",
          "10", "HELLO",
          "def", "HELLO",
          "c", "BYE"
      );
      LookupExtractor mapExtractor = new MapLookupExtractor(stringMap, false);
      LookupExtractionFn lookupFn = new LookupExtractionFn(mapExtractor, false, "UNKNOWN", false, true);

      assertLegacyFilterMatches(legacyInFilterWithFn("dim0", lookupFn, null, "HELLO"), ImmutableList.of("a"));
      assertLegacyFilterMatches(legacyInFilterWithFn("dim0", lookupFn, "HELLO", "BYE"), ImmutableList.of("a", "c"));
      assertLegacyFilterMatches(legacyInFilterWithFn("dim0", lookupFn, "UNKNOWN"), ImmutableList.of("b", "d", "e", "f"));
      assertLegacyFilterMatches(legacyInFilterWithFn("dim1", lookupFn, "HELLO"), ImmutableList.of("b", "e"));
      assertLegacyFilterMatches(legacyInFilterWithFn("dim1", lookupFn, "N/A"), ImmutableList.of());

      if (optimize) {
        // Arrays don't cause errors when the extractionFn is optimized, because the "IN" filter vanishes completely.
        assertLegacyFilterMatches(legacyInFilterWithFn("dim2", lookupFn, "a"), ImmutableList.of());
      } else {
        assertFilterMatchesSkipArrays(legacyInFilterWithFn("dim2", lookupFn, "a"), ImmutableList.of());
      }

      assertFilterMatchesSkipArrays(legacyInFilterWithFn("dim2", lookupFn, "HELLO"), ImmutableList.of("a", "d"));
      assertFilterMatchesSkipArrays(
          legacyInFilterWithFn("dim2", lookupFn, "HELLO", "BYE", "UNKNOWN"),
          ImmutableList.of("a", "b", "c", "d", "e", "f")
      );

      final Map<String, String> stringMap2 = ImmutableMap.of(
          "a", "e"
      );
      LookupExtractor mapExtractor2 = new MapLookupExtractor(stringMap2, false);
      LookupExtractionFn lookupFn2 = new LookupExtractionFn(mapExtractor2, true, null, false, true);

      assertLegacyFilterMatches(legacyInFilterWithFn("dim0", lookupFn2, null, "e"), ImmutableList.of("a", "e"));
      assertLegacyFilterMatches(legacyInFilterWithFn("dim0", lookupFn2, "a"), ImmutableList.of());

      final Map<String, String> stringMap3 = ImmutableMap.of(
          "c", "500",
          "100", "e"
      );
      LookupExtractor mapExtractor3 = new MapLookupExtractor(stringMap3, false);
      LookupExtractionFn lookupFn3 = new LookupExtractionFn(mapExtractor3, false, null, false, true);

      assertLegacyFilterMatches(legacyInFilterWithFn("dim0", lookupFn3, null, "c"), ImmutableList.of("a", "b", "d", "e", "f"));
      assertLegacyFilterMatches(legacyInFilterWithFn("dim0", lookupFn3, "e"), ImmutableList.of());
    }

    @Override
    protected void assertFilterMatches(DimFilter filter, List<String> expectedRows)
    {
      assertTypedFilterMatches(filter, expectedRows);
      assertLegacyFilterMatches(filter, expectedRows);
    }

    private void assertTypedFilterMatches(DimFilter filter, List<String> expectedRows)
    {
      // this filter only tests in sql compatible mode
      if (NullHandling.sqlCompatible()) {
        super.assertFilterMatches(filter, expectedRows);
        try {
          // make sure round trip json serde is cool
          super.assertFilterMatches(
              jsonMapper.readValue(jsonMapper.writeValueAsString(filter), DimFilter.class),
              expectedRows
          );
        }
        catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      } else {
        Throwable t = Assert.assertThrows(
            DruidException.class,
            () -> super.assertFilterMatches(filter, expectedRows)
        );
        Assert.assertEquals("Invalid IN filter, typed in filter only supports SQL compatible null handling mode, set druid.generic.useDefaultValue=false to use this filter", t.getMessage());
      }
    }

    private void assertLegacyFilterMatches(DimFilter filter, List<String> expectedRows)
    {
      DimFilter newFilter = rewriteToLegacyFilter(filter);
      if (newFilter != null) {
        super.assertFilterMatches(newFilter, expectedRows);
      }
    }

    @Nullable
    private DimFilter rewriteToLegacyFilter(DimFilter filter)
    {
      if (filter instanceof InDimFilter) {
        return filter;
      } else if (filter instanceof TypedInFilter) {
        TypedInFilter theFilter = (TypedInFilter) filter;
        return new InDimFilter(
            theFilter.getColumn(),
            InDimFilter.ValuesSet.copyOf(
                theFilter.getSortedValues()
                         .stream()
                         .map(DimensionHandlerUtils::convertObjectToString)
                         .collect(Collectors.toList())
            ),
            null
        );
      } else if (filter instanceof NotDimFilter) {
        DimFilter rewrite = rewriteToLegacyFilter(((NotDimFilter) filter).getField());
        if (rewrite != null) {
          return NotDimFilter.of(rewrite);
        }
      }
      return null;
    }
  }

  public static class TypedInFilterFilterNonParameterizedTests extends InitializedNullHandlingTest
  {
    @Test
    public void testSerde() throws JsonProcessingException
    {
      Assume.assumeTrue(NullHandling.sqlCompatible());
      ObjectMapper mapper = new DefaultObjectMapper();
      TypedInFilter filter = inFilter("column", ColumnType.STRING, Arrays.asList("a", "b", "c"));
      String s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, TypedInFilter.class));

      filter = inFilter("column", ColumnType.STRING, Arrays.asList("a", "b", "b", null, "c"));
      s = mapper.writeValueAsString(filter);
      TypedInFilter deserialized = mapper.readValue(s, TypedInFilter.class);
      Assert.assertEquals(Arrays.asList(null, "a", "b", "c"), deserialized.getSortedValues());
      Assert.assertEquals(filter, deserialized);

      filter = inFilter("column", ColumnType.LONG, Arrays.asList(1L, 2L, 2L, null, 3L));
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, TypedInFilter.class));

      filter = inFilter("column", ColumnType.DOUBLE, Arrays.asList(1.1, 2.2, 2.3, null, 3.3));
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, TypedInFilter.class));

      filter = inFilter("column", ColumnType.FLOAT, Arrays.asList(1.1f, 2.2f, 2.2f, null, 3.3f));
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, TypedInFilter.class));

      filter = inFilter("column", ColumnType.FLOAT, Arrays.asList(1.1, 2.2, 2.3, null, 3.3));
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, TypedInFilter.class));
    }

    @Test
    public void testGetCacheKey()
    {
      Assume.assumeTrue(NullHandling.sqlCompatible());
      TypedInFilter filterUnsorted = inFilter("column", ColumnType.STRING, Arrays.asList("a", "b", null, "c"));
      TypedInFilter filterDifferent = inFilter("column", ColumnType.STRING, Arrays.asList("a", "c", "b"));
      TypedInFilter filterPresorted = new TypedInFilter(
          "column",
          ColumnType.STRING,
          null,
          Arrays.asList(null, "a", "b", "c"),
          null
      );

      Assert.assertEquals(filterPresorted, filterUnsorted);
      Assert.assertNotEquals(filterDifferent, filterPresorted);
      Assert.assertArrayEquals(filterPresorted.getCacheKey(), filterUnsorted.getCacheKey());
      Assert.assertFalse(Arrays.equals(filterDifferent.getCacheKey(), filterPresorted.getCacheKey()));

      filterUnsorted = inFilter("column", ColumnType.LONG, Arrays.asList(2L, -2L, 1L, null, 3L));
      filterDifferent = inFilter("column", ColumnType.LONG, Arrays.asList(2L, -2L, 1L, 3L));
      filterPresorted = new TypedInFilter(
          "column",
          ColumnType.LONG,
          null,
          Arrays.asList(null, -2L, 1L, 2L, 3L),
          null
      );

      Assert.assertEquals(filterPresorted, filterUnsorted);
      Assert.assertNotEquals(filterDifferent, filterPresorted);
      Assert.assertArrayEquals(filterPresorted.getCacheKey(), filterUnsorted.getCacheKey());
      Assert.assertFalse(Arrays.equals(filterDifferent.getCacheKey(), filterPresorted.getCacheKey()));

      filterUnsorted = inFilter("column", ColumnType.DOUBLE, Arrays.asList(2.2, -2.2, 1.1, null, 3.3));
      filterDifferent = inFilter("column", ColumnType.DOUBLE, Arrays.asList(2.2, -2.2, 1.1, 3.3));
      filterPresorted = new TypedInFilter(
          "column",
          ColumnType.DOUBLE,
          null,
          Arrays.asList(null, -2.2, 1.1, 2.2, 3.3),
          null
      );

      Assert.assertEquals(filterPresorted, filterUnsorted);
      Assert.assertNotEquals(filterDifferent, filterPresorted);
      Assert.assertArrayEquals(filterPresorted.getCacheKey(), filterUnsorted.getCacheKey());
      Assert.assertFalse(Arrays.equals(filterDifferent.getCacheKey(), filterPresorted.getCacheKey()));

      filterUnsorted = inFilter("column", ColumnType.FLOAT, Arrays.asList(2.2f, -2.2f, 1.1f, null, 3.3f));
      filterDifferent = inFilter("column", ColumnType.FLOAT, Arrays.asList(2.2f, -2.2f, 1.1f, 3.3f));
      filterPresorted = new TypedInFilter(
          "column",
          ColumnType.FLOAT,
          null,
          Arrays.asList(null, -2.2f, 1.1f, 2.2f, 3.3f),
          null
      );

      Assert.assertEquals(filterPresorted, filterUnsorted);
      Assert.assertNotEquals(filterDifferent, filterPresorted);
      Assert.assertArrayEquals(filterPresorted.getCacheKey(), filterUnsorted.getCacheKey());
      Assert.assertFalse(Arrays.equals(filterDifferent.getCacheKey(), filterPresorted.getCacheKey()));
    }

    @Test
    public void testInvalidParameters()
    {
      if (NullHandling.replaceWithDefault()) {
        Throwable t = Assert.assertThrows(
            DruidException.class,
            () -> new TypedInFilter("column", ColumnType.STRING, Collections.emptyList(), null, null).toFilter()
        );
        Assert.assertEquals("Invalid IN filter, typed in filter only supports SQL compatible null handling mode, set druid.generic.useDefaultValue=false to use this filter", t.getMessage());
      }

      Assume.assumeTrue(NullHandling.sqlCompatible());
      Throwable t = Assert.assertThrows(
          DruidException.class,
          () -> new TypedInFilter(null, ColumnType.STRING, null, null, null)
      );
      Assert.assertEquals("Invalid IN filter, column cannot be null", t.getMessage());
      t = Assert.assertThrows(
          DruidException.class,
          () -> new TypedInFilter("dim0", null, null, null, null)
      );
      Assert.assertEquals("Invalid IN filter on column [dim0], matchValueType cannot be null", t.getMessage());
      t = Assert.assertThrows(
          DruidException.class,
          () -> new TypedInFilter("dim0", ColumnType.STRING, null, null, null)
      );
      Assert.assertEquals(
          "Invalid IN filter on column [dim0], exactly one of values or sortedValues must be non-null",
          t.getMessage()
      );
    }

    @Test
    public void testGetDimensionRangeSet()
    {
      Assume.assumeTrue(NullHandling.sqlCompatible());
      TypedInFilter filter = inFilter("x", ColumnType.STRING, Arrays.asList(null, "a", "b", "c"));
      TypedInFilter filter2 = inFilter("x", ColumnType.STRING, Arrays.asList("a", "b", null, "c"));

      Assert.assertEquals(filter.getDimensionRangeSet("x"), filter2.getDimensionRangeSet("x"));
      RangeSet<String> range = filter.getDimensionRangeSet("x");
      Assert.assertTrue(range.contains("b"));

      filter = inFilter("x", ColumnType.LONG, Arrays.asList(null, 1L, 2L, 3L));
      filter2 = inFilter("x", ColumnType.LONG, Arrays.asList(3L, 1L, null, 2L));
      Assert.assertEquals(filter.getDimensionRangeSet("x"), filter2.getDimensionRangeSet("x"));
      range = filter.getDimensionRangeSet("x");
      Assert.assertTrue(range.contains("2"));

      filter = inFilter("x", ColumnType.DOUBLE, Arrays.asList(null, 1.1, 2.2, 3.3));
      filter2 = inFilter("x", ColumnType.DOUBLE, Arrays.asList(3.3, 1.1, null, 2.2));
      range = filter.getDimensionRangeSet("x");
      Assert.assertEquals(filter.getDimensionRangeSet("x"), filter2.getDimensionRangeSet("x"));
      Assert.assertTrue(range.contains("2.2"));

      filter = inFilter("x", ColumnType.FLOAT, Arrays.asList(null, 1.1f, 2.2f, 3.3f));
      filter2 = inFilter("x", ColumnType.FLOAT, Arrays.asList(3.3f, 1.1f, null, 2.2f));
      range = filter.getDimensionRangeSet("x");
      Assert.assertEquals(filter.getDimensionRangeSet("x"), filter2.getDimensionRangeSet("x"));
      Assert.assertTrue(range.contains("2.2"));
    }

    @Test
    public void testRequiredColumnRewrite()
    {
      Assume.assumeTrue(NullHandling.sqlCompatible());
      TypedInFilter filter = inFilter("dim0", ColumnType.STRING, Arrays.asList("a", "c"));
      TypedInFilter filter2 = inFilter("dim1", ColumnType.STRING, Arrays.asList("a", "c"));

      Assert.assertTrue(filter.supportsRequiredColumnRewrite());
      Assert.assertTrue(filter2.supportsRequiredColumnRewrite());

      Filter rewrittenFilter = filter.rewriteRequiredColumns(ImmutableMap.of("dim0", "dim1"));
      Assert.assertEquals(filter2, rewrittenFilter);

      Throwable t = Assert.assertThrows(
          IAE.class,
          () -> filter.rewriteRequiredColumns(ImmutableMap.of("invalidName", "dim1"))
      );
      Assert.assertEquals(
          "Received a non-applicable rewrite: {invalidName=dim1}, filter's dimension: dim0",
          t.getMessage()
      );
    }

    @Test
    public void testEquals()
    {
      Assume.assumeTrue(NullHandling.sqlCompatible());
      EqualsVerifier.forClass(TypedInFilter.class).usingGetClass()
                    .withNonnullFields(
                        "column",
                        "matchValueType",
                        "unsortedValues",
                        "sortedMatchValues",
                        "optimizedFilterIncludeUnknown",
                        "optimizedFilterNoIncludeUnknown"
                    )
                    .withPrefabValues(ColumnType.class, ColumnType.STRING, ColumnType.DOUBLE)
                    .withPrefabValues(
                        Supplier.class,
                        Suppliers.ofInstance(ImmutableList.of("a", "b")),
                        Suppliers.ofInstance(ImmutableList.of("b", "c"))
                    )
                    .withIgnoredFields(
                        "unsortedValues",
                        "sortedUtf8MatchValueBytes",
                        "predicateFactorySupplier",
                        "cacheKeySupplier",
                        "optimizedFilterIncludeUnknown",
                        "optimizedFilterNoIncludeUnknown"
                    )
                    .verify();
    }
  }

  public static class LegacyInDimFilterNonParameterizedTests extends InitializedNullHandlingTest
  {
    @Test
    public void testRequiredColumnRewrite()
    {
      InDimFilter filter = (InDimFilter) legacyInFilter("dim0", "a", "c").toFilter();
      InDimFilter filter2 = (InDimFilter) legacyInFilter("dim1", "a", "c").toFilter();

      Assert.assertTrue(filter.supportsRequiredColumnRewrite());
      Assert.assertTrue(filter2.supportsRequiredColumnRewrite());

      Filter rewrittenFilter = filter.rewriteRequiredColumns(ImmutableMap.of("dim0", "dim1"));
      Assert.assertEquals(filter2, rewrittenFilter);

      Throwable t = Assert.assertThrows(
          IAE.class,
          () -> filter.rewriteRequiredColumns(ImmutableMap.of("invalidName", "dim1"))
      );
      Assert.assertEquals(
          "Received a non-applicable rewrite: {invalidName=dim1}, filter's dimension: dim0",
          t.getMessage()
      );
    }

    @Test
    public void testEuals()
    {
      EqualsVerifier.forClass(InDimFilter.class)
                    .usingGetClass()
                    .withNonnullFields("dimension", "values")
                    .withIgnoredFields(
                        "cacheKeySupplier",
                        "predicateFactory",
                        "optimizedFilterIncludeUnknown",
                        "optimizedFilterNoIncludeUnknown",
                        "valuesUtf8"
                    )
                    .verify();
    }

    @Test
    public void testEqualsForInFilterDruidPredicateFactory()
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
  }

  private static TypedInFilter inFilter(String columnName, ColumnType matchValueType, List<?> values)
  {
    return new TypedInFilter(
        columnName,
        matchValueType,
        values,
        null,
        null
    );
  }

  private static InDimFilter legacyInFilter(String dim, String value, String... values)
  {
    return new InDimFilter(dim, Lists.asList(value, values), null);
  }

  private static InDimFilter legacyInFilterWithFn(String dim, ExtractionFn fn, String value, String... values)
  {
    return new InDimFilter(dim, Lists.asList(value, values), fn);
  }
}

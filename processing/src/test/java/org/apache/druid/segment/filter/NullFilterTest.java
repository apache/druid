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
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.Map;

@RunWith(Parameterized.class)
public class NullFilterTest extends BaseFilterTest
{
  public NullFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(testName, DEFAULT_ROWS, indexBuilder, finisher, cnf, optimize);
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    BaseFilterTest.tearDown(NullFilterTest.class.getName());
  }

  @Test
  public void testSingleValueStringColumnWithoutNulls()
  {
    assertFilterMatches(NullFilter.forColumn("dim0"), ImmutableList.of());
  }

  @Test
  public void testSingleValueVirtualStringColumnWithoutNulls()
  {
    assertFilterMatches(NullFilter.forColumn("vdim0"), ImmutableList.of());
  }

  @Test
  public void testListFilteredVirtualColumn()
  {
    assertFilterMatchesSkipVectorize(NullFilter.forColumn("allow-dim0"), ImmutableList.of("0", "1", "2", "5"));
    assertFilterMatchesSkipVectorize(NullFilter.forColumn("deny-dim0"), ImmutableList.of("3", "4"));
    if (isAutoSchema()) {
      return;
    }
    assertFilterMatchesSkipVectorize(NullFilter.forColumn("allow-dim2"), ImmutableList.of("1", "2", "4", "5"));
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatchesSkipVectorize(
          NullFilter.forColumn("deny-dim2"),
          ImmutableList.of("1", "2", "3", "5")
      );
    } else {
      assertFilterMatchesSkipVectorize(
          NullFilter.forColumn("deny-dim2"),
          ImmutableList.of("1", "3", "5")
      );
    }
  }

  @Test
  public void testSingleValueStringColumnWithNulls()
  {
    // testSingleValueStringColumnWithoutNulls but with virtual column selector
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(NullFilter.forColumn("dim1"), ImmutableList.of("0"));
    } else {
      assertFilterMatches(NullFilter.forColumn("dim1"), ImmutableList.of());
    }
  }

  @Test
  public void testSingleValueVirtualStringColumnWithNulls()
  {
    // testSingleValueStringColumnWithNulls but with virtual column selector
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(NullFilter.forColumn("vdim1"), ImmutableList.of("0"));
    } else {
      assertFilterMatches(NullFilter.forColumn("vdim1"), ImmutableList.of());
    }
  }

  @Test
  public void testMultiValueStringColumn()
  {
    if (NullHandling.replaceWithDefault()) {
      if (isAutoSchema()) {
        assertFilterMatches(NullFilter.forColumn("dim2"), ImmutableList.of("5"));
      } else {
        assertFilterMatches(NullFilter.forColumn("dim2"), ImmutableList.of("1", "2", "5"));
      }
    } else {
      // only one array row is totally null
      if (isAutoSchema()) {
        assertFilterMatches(NullFilter.forColumn("dim2"), ImmutableList.of("5"));
      } else {
        assertFilterMatches(NullFilter.forColumn("dim2"), ImmutableList.of("1", "5"));
      }
    }
  }

  @Test
  public void testMissingColumnSpecifiedInDimensionList()
  {
    assertFilterMatches(NullFilter.forColumn("dim3"), ImmutableList.of("0", "1", "2", "3", "4", "5"));
  }

  @Test
  public void testMissingColumnNotSpecifiedInDimensionList()
  {
    assertFilterMatches(NullFilter.forColumn("dim4"), ImmutableList.of("0", "1", "2", "3", "4", "5"));
  }


  @Test
  public void testVirtualNumericColumnNullsAndDefaults()
  {
    if (canTestNumericNullsAsDefaultValues) {
      assertFilterMatches(NullFilter.forColumn("vf0"), ImmutableList.of());
      assertFilterMatches(NullFilter.forColumn("vd0"), ImmutableList.of());
      assertFilterMatches(NullFilter.forColumn("vl0"), ImmutableList.of());
    } else {
      assertFilterMatches(NullFilter.forColumn("vf0"), ImmutableList.of("4"));
      assertFilterMatches(NullFilter.forColumn("vd0"), ImmutableList.of("2"));
      assertFilterMatches(NullFilter.forColumn("vl0"), ImmutableList.of("3"));
    }
  }

  @Test
  public void testNumericColumnNullsAndDefaults()
  {
    if (canTestNumericNullsAsDefaultValues) {
      assertFilterMatches(NullFilter.forColumn("f0"), ImmutableList.of());
      assertFilterMatches(NullFilter.forColumn("d0"), ImmutableList.of());
      assertFilterMatches(NullFilter.forColumn("l0"), ImmutableList.of());
    } else {
      assertFilterMatches(NullFilter.forColumn("f0"), ImmutableList.of("4"));
      assertFilterMatches(NullFilter.forColumn("d0"), ImmutableList.of("2"));
      assertFilterMatches(NullFilter.forColumn("l0"), ImmutableList.of("3"));
    }
  }

  @Test
  public void testSelectorWithLookupExtractionFn()
  {
    /*
      static final List<InputRow> DEFAULT_ROWS = ImmutableList.of(
      makeDefaultSchemaRow("0", "", ImmutableList.of("a", "b"), "2017-07-25", 0.0, 0.0f, 0L),
      makeDefaultSchemaRow("1", "10", ImmutableList.of(), "2017-07-25", 10.1, 10.1f, 100L),
      makeDefaultSchemaRow("2", "2", ImmutableList.of(""), "2017-05-25", null, 5.5f, 40L),
      makeDefaultSchemaRow("3", "1", ImmutableList.of("a"), "2020-01-25", 120.0245, 110.0f, null),
      makeDefaultSchemaRow("4", "abdef", ImmutableList.of("c"), null, 60.0, null, 9001L),
      makeDefaultSchemaRow("5", "abc", null, "2020-01-25", 765.432, 123.45f, 12345L)
  );
     */
    final Map<String, String> stringMap = ImmutableMap.of(
        "1", "HELLO",
        "a", "HELLO",
        "abdef", "HELLO",
        "abc", "UNKNOWN"
    );
    LookupExtractor mapExtractor = new MapLookupExtractor(stringMap, false);
    LookupExtractionFn lookupFn = new LookupExtractionFn(mapExtractor, false, null, false, true);
    LookupExtractionFn lookupFnRetain = new LookupExtractionFn(mapExtractor, true, null, false, true);
    LookupExtractionFn lookupFnReplace = new LookupExtractionFn(mapExtractor, false, "UNKNOWN", false, true);

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new NullFilter("dim0", lookupFn, null), ImmutableList.of("0", "2", "3", "4", "5"));
      assertFilterMatches(new NullFilter("dim0", lookupFnRetain, null), ImmutableList.of());
    } else {
      assertFilterMatches(new NullFilter("dim0", lookupFn, null), ImmutableList.of("0", "2", "3", "4", "5"));
      assertFilterMatches(new NullFilter("dim0", lookupFnRetain, null), ImmutableList.of());
    }

    assertFilterMatches(new NullFilter("dim0", lookupFnReplace, null), ImmutableList.of());


    final Map<String, String> stringMapEmpty = ImmutableMap.of(
        "1", ""
    );
    LookupExtractor mapExtractoryEmpty = new MapLookupExtractor(stringMapEmpty, false);
    LookupExtractionFn lookupFnEmpty = new LookupExtractionFn(mapExtractoryEmpty, false, null, false, true);
    if (NullHandling.replaceWithDefault()) {
      // Nulls and empty strings are considered equivalent
      assertFilterMatches(
          new NullFilter("dim0", lookupFnEmpty, null),
          ImmutableList.of("0", "1", "2", "3", "4", "5")
      );
    } else {
      assertFilterMatches(
          new NullFilter("dim0", lookupFnEmpty, null),
          ImmutableList.of("0", "2", "3", "4", "5")
      );
    }
  }

  @Test
  public void testArrays()
  {
    if (isAutoSchema()) {
      // only auto schema ingests arrays
    /*
        dim0 .. arrayString               arrayLong             arrayDouble
        "0", .. ["a", "b", "c"],          [1L, 2L, 3L],         [1.1, 2.2, 3.3]
        "1", .. [],                       [],                   [1.1, 2.2, 3.3]
        "2", .. null,                     [1L, 2L, 3L],         [null]
        "3", .. ["a", "b", "c"],          null,                 []
        "4", .. ["c", "d"],               [null],               [-1.1, -333.3]
        "5", .. [null],                   [123L, 345L],         null
     */
      assertFilterMatches(
          new NullFilter("arrayString", null, null),
          ImmutableList.of("2")
      );
      assertFilterMatches(
          new NullFilter("arrayLong", null, null),
          ImmutableList.of("3")
      );
      assertFilterMatches(
          new NullFilter("arrayDouble", null, null),
          ImmutableList.of("5")
      );
    }
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(NullFilter.class).usingGetClass()
                  .withNonnullFields("column")
                  .withIgnoredFields("cachedOptimizedFilter")
                  .verify();
  }
}

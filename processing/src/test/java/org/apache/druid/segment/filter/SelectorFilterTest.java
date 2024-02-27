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
import org.apache.druid.query.extraction.TimeDimExtractionFn;
import org.apache.druid.query.filter.ExtractionDimFilter;
import org.apache.druid.query.filter.FalseDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Map;

@RunWith(Parameterized.class)
public class SelectorFilterTest extends BaseFilterTest
{
  public SelectorFilterTest(
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
    BaseFilterTest.tearDown(SelectorFilterTest.class.getName());
  }

  @Test
  public void testWithTimeExtractionFnNull()
  {
    assertFilterMatches(
        new SelectorDimFilter("dim0", null, new TimeDimExtractionFn("yyyy-MM-dd", "yyyy-MM", true)),
        ImmutableList.of()
    );
    assertFilterMatches(
        new SelectorDimFilter("vdim0", null, new TimeDimExtractionFn("yyyy-MM-dd", "yyyy-MM", true)),
        ImmutableList.of()
    );
    assertFilterMatches(
        new SelectorDimFilter("timeDim", null, new TimeDimExtractionFn("yyyy-MM-dd", "yyyy-MM", true)),
        ImmutableList.of("4")
    );
    assertFilterMatches(new SelectorDimFilter(
        "timeDim",
        "2017-07",
        new TimeDimExtractionFn("yyyy-MM-dd", "yyyy-MM", true)
    ), ImmutableList.of("0", "1"));
    assertFilterMatches(new SelectorDimFilter(
        "timeDim",
        "2017-05",
        new TimeDimExtractionFn("yyyy-MM-dd", "yyyy-MM", true)
    ), ImmutableList.of("2"));

    assertFilterMatches(new SelectorDimFilter(
        "timeDim",
        "2020-01",
        new TimeDimExtractionFn("yyyy-MM-dd", "yyyy-MM", true)
    ), ImmutableList.of("3", "5"));
  }

  @Test
  public void testSingleValueStringColumnWithoutNulls()
  {
    assertFilterMatches(new SelectorDimFilter("dim0", null, null), ImmutableList.of());
    assertFilterMatches(new SelectorDimFilter("dim0", "", null), ImmutableList.of());
    assertFilterMatches(new SelectorDimFilter("dim0", "0", null), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("dim0", "1", null), ImmutableList.of("1"));
  }

  @Test
  public void testSingleValueVirtualStringColumnWithoutNulls()
  {
    assertFilterMatches(new SelectorDimFilter("vdim0", null, null), ImmutableList.of());
    assertFilterMatches(new SelectorDimFilter("vdim0", "", null), ImmutableList.of());
    assertFilterMatches(new SelectorDimFilter("vdim0", "0", null), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("vdim0", "1", null), ImmutableList.of("1"));
  }

  @Test
  public void testListFilteredVirtualColumn()
  {
    assertFilterMatchesSkipVectorize(new SelectorDimFilter("allow-dim0", "1", null), ImmutableList.of());
    assertFilterMatchesSkipVectorize(new SelectorDimFilter("allow-dim0", "4", null), ImmutableList.of("4"));
    assertFilterMatchesSkipVectorize(
        new SelectorDimFilter("allow-dim0", null, null),
        ImmutableList.of("0", "1", "2", "5")
    );
    assertFilterMatchesSkipVectorize(new SelectorDimFilter("deny-dim0", "0", null), ImmutableList.of("0"));
    assertFilterMatchesSkipVectorize(new SelectorDimFilter("deny-dim0", "4", null), ImmutableList.of());
    assertFilterMatchesSkipVectorize(new SelectorDimFilter("deny-dim0", null, null), ImmutableList.of("3", "4"));
    if (isAutoSchema()) {
      return;
    }
    assertFilterMatchesSkipVectorize(new SelectorDimFilter("allow-dim2", "b", null), ImmutableList.of());
    assertFilterMatchesSkipVectorize(new SelectorDimFilter("allow-dim2", "a", null), ImmutableList.of("0", "3"));
    assertFilterMatchesSkipVectorize(
        new SelectorDimFilter("allow-dim2", null, null),
        ImmutableList.of("1", "2", "4", "5")
    );
    assertFilterMatchesSkipVectorize(new SelectorDimFilter("deny-dim2", "b", null), ImmutableList.of("0"));
    assertFilterMatchesSkipVectorize(new SelectorDimFilter("deny-dim2", "a", null), ImmutableList.of());
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatchesSkipVectorize(
          new SelectorDimFilter("deny-dim2", null, null),
          ImmutableList.of("1", "2", "3", "5")
      );
    } else {
      assertFilterMatchesSkipVectorize(
          new SelectorDimFilter("deny-dim2", null, null),
          ImmutableList.of("1", "3", "5")
      );
    }
  }

  @Test
  public void testSingleValueStringColumnWithNulls()
  {
    // testSingleValueStringColumnWithoutNulls but with virtual column selector
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new SelectorDimFilter("dim1", null, null), ImmutableList.of("0"));
    } else {
      assertFilterMatches(new SelectorDimFilter("dim1", null, null), ImmutableList.of());
    }
    assertFilterMatches(new SelectorDimFilter("dim1", "", null), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("dim1", "10", null), ImmutableList.of("1"));
    assertFilterMatches(new SelectorDimFilter("dim1", "2", null), ImmutableList.of("2"));
    assertFilterMatches(new SelectorDimFilter("dim1", "1", null), ImmutableList.of("3"));
    assertFilterMatches(new SelectorDimFilter("dim1", "abdef", null), ImmutableList.of("4"));
    assertFilterMatches(new SelectorDimFilter("dim1", "abc", null), ImmutableList.of("5"));
    assertFilterMatches(new SelectorDimFilter("dim1", "ab", null), ImmutableList.of());
  }

  @Test
  public void testSingleValueVirtualStringColumnWithNulls()
  {
    // testSingleValueStringColumnWithNulls but with virtual column selector
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new SelectorDimFilter("vdim1", null, null), ImmutableList.of("0"));
    } else {
      assertFilterMatches(new SelectorDimFilter("vdim1", null, null), ImmutableList.of());
    }
    assertFilterMatches(new SelectorDimFilter("vdim1", "", null), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("vdim1", "10", null), ImmutableList.of("1"));
    assertFilterMatches(new SelectorDimFilter("vdim1", "2", null), ImmutableList.of("2"));
    assertFilterMatches(new SelectorDimFilter("vdim1", "1", null), ImmutableList.of("3"));
    assertFilterMatches(new SelectorDimFilter("vdim1", "abdef", null), ImmutableList.of("4"));
    assertFilterMatches(new SelectorDimFilter("vdim1", "abc", null), ImmutableList.of("5"));
    assertFilterMatches(new SelectorDimFilter("vdim1", "ab", null), ImmutableList.of());
  }

  @Test
  public void testMultiValueStringColumn()
  {
    if (isAutoSchema()) {
      return;
    }
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new SelectorDimFilter("dim2", null, null), ImmutableList.of("1", "2", "5"));
      assertFilterMatches(new SelectorDimFilter("dim2", "", null), ImmutableList.of("1", "2", "5"));
    } else {
      assertFilterMatches(new SelectorDimFilter("dim2", null, null), ImmutableList.of("1", "5"));
      assertFilterMatches(new SelectorDimFilter("dim2", "", null), ImmutableList.of("2"));
    }
    assertFilterMatches(new SelectorDimFilter("dim2", "a", null), ImmutableList.of("0", "3"));
    assertFilterMatches(new SelectorDimFilter("dim2", "b", null), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("dim2", "c", null), ImmutableList.of("4"));
    assertFilterMatches(new SelectorDimFilter("dim2", "d", null), ImmutableList.of());
  }

  @Test
  public void testMissingColumnSpecifiedInDimensionList()
  {
    assertFilterMatches(new SelectorDimFilter("dim3", null, null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new SelectorDimFilter("dim3", "", null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    } else {
      assertFilterMatches(new SelectorDimFilter("dim3", "", null), ImmutableList.of());
    }
    assertFilterMatches(new SelectorDimFilter("dim3", "a", null), ImmutableList.of());
    assertFilterMatches(new SelectorDimFilter("dim3", "b", null), ImmutableList.of());
    assertFilterMatches(new SelectorDimFilter("dim3", "c", null), ImmutableList.of());
  }

  @Test
  public void testMissingColumnNotSpecifiedInDimensionList()
  {
    assertFilterMatches(new SelectorDimFilter("dim4", null, null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new SelectorDimFilter("dim4", "", null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    } else {
      assertFilterMatches(new SelectorDimFilter("dim4", "", null), ImmutableList.of());
    }
    assertFilterMatches(new SelectorDimFilter("dim4", "a", null), ImmutableList.of());
    assertFilterMatches(new SelectorDimFilter("dim4", "b", null), ImmutableList.of());
    assertFilterMatches(new SelectorDimFilter("dim4", "c", null), ImmutableList.of());
  }

  @Test
  public void testExpressionVirtualColumn()
  {
    assertFilterMatches(
        new SelectorDimFilter("expr", "1.1", null),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
    assertFilterMatches(new SelectorDimFilter("expr", "1.2", null), ImmutableList.of());
  }

  @Test
  public void testSelectorWithLookupExtractionFn()
  {
    final Map<String, String> stringMap = ImmutableMap.of(
        "1", "HELLO",
        "a", "HELLO",
        "abdef", "HELLO",
        "abc", "UNKNOWN"
    );
    LookupExtractor mapExtractor = new MapLookupExtractor(stringMap, false);
    LookupExtractionFn lookupFn = new LookupExtractionFn(mapExtractor, false, "UNKNOWN", false, true);

    assertFilterMatches(new SelectorDimFilter("dim0", "HELLO", lookupFn), ImmutableList.of("1"));
    assertFilterMatches(new SelectorDimFilter("dim0", "UNKNOWN", lookupFn), ImmutableList.of("0", "2", "3", "4", "5"));

    assertFilterMatches(new SelectorDimFilter("dim1", "HELLO", lookupFn), ImmutableList.of("3", "4"));
    assertFilterMatches(new SelectorDimFilter("dim1", "UNKNOWN", lookupFn), ImmutableList.of("0", "1", "2", "5"));

    assertFilterMatchesSkipArrays(new SelectorDimFilter("dim2", "HELLO", lookupFn), ImmutableList.of("0", "3"));
    assertFilterMatchesSkipArrays(
        new SelectorDimFilter("dim2", "UNKNOWN", lookupFn),
        ImmutableList.of("0", "1", "2", "4", "5")
    );

    assertFilterMatches(new SelectorDimFilter("dim3", "HELLO", lookupFn), ImmutableList.of());
    assertFilterMatches(
        new SelectorDimFilter("dim3", "UNKNOWN", lookupFn),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );

    assertFilterMatches(new SelectorDimFilter("dim4", "HELLO", lookupFn), ImmutableList.of());
    assertFilterMatches(
        new SelectorDimFilter("dim4", "UNKNOWN", lookupFn),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );

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
    if (NullHandling.replaceWithDefault()) {
      // Nulls and empty strings are considered equivalent
      assertFilterMatches(
          new SelectorDimFilter("dim0", null, lookupFn3),
          ImmutableList.of("0", "1", "2", "3", "4", "5")
      );
    } else {
      assertFilterMatches(
          new SelectorDimFilter("dim0", null, lookupFn3),
          ImmutableList.of("0", "2", "3", "4", "5")
      );
      assertFilterMatches(
          new SelectorDimFilter("dim0", "", lookupFn3),
          ImmutableList.of("1")
      );
    }


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

    Assert.assertEquals(optFilter1, optFilter1.optimize(false));
    Assert.assertEquals(optFilter2Optimized, optFilter2.optimize(false));
    Assert.assertEquals(optFilter3, optFilter3.optimize(false));
    Assert.assertEquals(optFilter4Optimized, optFilter4.optimize(false));
    Assert.assertEquals(FalseDimFilter.instance(), optFilter5.optimize(false));
    Assert.assertEquals(optFilter6Optimized, optFilter6.optimize(false));

    assertFilterMatches(optFilter1, ImmutableList.of("0", "1", "2", "5"));
    assertFilterMatches(optFilter2, ImmutableList.of("2", "5"));
    if (NullHandling.replaceWithDefault()) {
      // Null and Empty strings are same
      assertFilterMatches(optFilter3, ImmutableList.of("0", "1", "2", "3", "4", "5"));
    } else {
      assertFilterMatches(optFilter3, ImmutableList.of("0", "2", "3", "4", "5"));
    }
    assertFilterMatches(optFilter4, ImmutableList.of("5"));
    assertFilterMatches(optFilter5, ImmutableList.of());
    assertFilterMatches(optFilter6, ImmutableList.of("5"));

    // tests that ExtractionDimFilter (identical to SelectorDimFilter now) optimize() with lookup works
    // remove these when ExtractionDimFilter is removed.
    assertFilterMatches(
        new ExtractionDimFilter("dim1", "UNKNOWN", lookupFn, null),
        ImmutableList.of("0", "1", "2", "5")
    );
    assertFilterMatches(new ExtractionDimFilter("dim0", "5", lookupFn2, null), ImmutableList.of("2", "5"));
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new ExtractionDimFilter("dim0", null, lookupFn3, null),
          ImmutableList.of("0", "1", "2", "3", "4", "5")
      );
    } else {
      assertFilterMatches(
          new ExtractionDimFilter("dim0", null, lookupFn3, null),
          ImmutableList.of("0", "2", "3", "4", "5")
      );
      assertFilterMatches(
          new ExtractionDimFilter("dim0", "", lookupFn3, null),
          ImmutableList.of("1")
      );
    }
  }

  @Test
  public void testNumericColumnNullsAndDefaults()
  {
    if (canTestNumericNullsAsDefaultValues) {
      assertFilterMatches(new SelectorDimFilter("f0", "0", null), ImmutableList.of("0", "4"));
      assertFilterMatches(new SelectorDimFilter("d0", "0", null), ImmutableList.of("0", "2"));
      assertFilterMatches(new SelectorDimFilter("l0", "0", null), ImmutableList.of("0", "3"));
      assertFilterMatches(new SelectorDimFilter("f0", null, null), ImmutableList.of());
      assertFilterMatches(new SelectorDimFilter("d0", null, null), ImmutableList.of());
      assertFilterMatches(new SelectorDimFilter("l0", null, null), ImmutableList.of());
    } else {
      assertFilterMatches(new SelectorDimFilter("f0", "0", null), ImmutableList.of("0"));
      assertFilterMatches(new SelectorDimFilter("d0", "0", null), ImmutableList.of("0"));
      assertFilterMatches(new SelectorDimFilter("l0", "0", null), ImmutableList.of("0"));
      assertFilterMatches(new SelectorDimFilter("f0", null, null), ImmutableList.of("4"));
      assertFilterMatches(new SelectorDimFilter("d0", null, null), ImmutableList.of("2"));
      assertFilterMatches(new SelectorDimFilter("l0", null, null), ImmutableList.of("3"));
    }
  }

  @Test
  public void testVirtualNumericColumnNullsAndDefaults()
  {
    if (canTestNumericNullsAsDefaultValues) {
      assertFilterMatches(new SelectorDimFilter("vf0", "0", null), ImmutableList.of("0", "4"));
      assertFilterMatches(new SelectorDimFilter("vd0", "0", null), ImmutableList.of("0", "2"));
      assertFilterMatches(new SelectorDimFilter("vl0", "0", null), ImmutableList.of("0", "3"));
      assertFilterMatches(new SelectorDimFilter("vf0", null, null), ImmutableList.of());
      assertFilterMatches(new SelectorDimFilter("vd0", null, null), ImmutableList.of());
      assertFilterMatches(new SelectorDimFilter("vl0", null, null), ImmutableList.of());
    } else {
      assertFilterMatches(new SelectorDimFilter("vf0", "0", null), ImmutableList.of("0"));
      assertFilterMatches(new SelectorDimFilter("vd0", "0", null), ImmutableList.of("0"));
      assertFilterMatches(new SelectorDimFilter("vl0", "0", null), ImmutableList.of("0"));
      assertFilterMatches(new SelectorDimFilter("vf0", null, null), ImmutableList.of("4"));
      assertFilterMatches(new SelectorDimFilter("vd0", null, null), ImmutableList.of("2"));
      assertFilterMatches(new SelectorDimFilter("vl0", null, null), ImmutableList.of("3"));
    }
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(SelectorFilter.class).usingGetClass().withNonnullFields("dimension").verify();
  }
}

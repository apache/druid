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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;

@RunWith(Enclosed.class)
public class NullFilterTests
{
  @RunWith(Parameterized.class)
  public static class NullFilterTest extends BaseFilterTest
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
      assertFilterMatches(
          NotDimFilter.of(NullFilter.forColumn("dim0")),
          ImmutableList.of("0", "1", "2", "3", "4", "5")
      );
    }

    @Test
    public void testSingleValueVirtualStringColumnWithoutNulls()
    {
      assertFilterMatches(NullFilter.forColumn("vdim0"), ImmutableList.of());
      assertFilterMatches(
          NotDimFilter.of(NullFilter.forColumn("vdim0")),
          ImmutableList.of("0", "1", "2", "3", "4", "5")
      );
    }

    @Test
    public void testListFilteredVirtualColumn()
    {
      assertFilterMatchesSkipVectorize(NullFilter.forColumn("allow-dim0"), ImmutableList.of("0", "1", "2", "5"));
      assertFilterMatchesSkipVectorize(NotDimFilter.of(NullFilter.forColumn("allow-dim0")), ImmutableList.of("3", "4"));
      assertFilterMatchesSkipVectorize(NullFilter.forColumn("deny-dim0"), ImmutableList.of("3", "4"));
      assertFilterMatchesSkipVectorize(
          NotDimFilter.of(NullFilter.forColumn("deny-dim0")),
          ImmutableList.of("0", "1", "2", "5")
      );
      if (isAutoSchema()) {
        return;
      }
      assertFilterMatchesSkipVectorize(NullFilter.forColumn("allow-dim2"), ImmutableList.of("1", "2", "4", "5"));
      assertFilterMatchesSkipVectorize(
          NotDimFilter.of(NullFilter.forColumn("allow-dim2")),
          ImmutableList.of("0", "3")
      );
      if (NullHandling.replaceWithDefault()) {
        assertFilterMatchesSkipVectorize(
            NullFilter.forColumn("deny-dim2"),
            ImmutableList.of("1", "2", "3", "5")
        );
        assertFilterMatchesSkipVectorize(
            NotDimFilter.of(NullFilter.forColumn("deny-dim2")),
            ImmutableList.of("0", "4")
        );
      } else {
        assertFilterMatchesSkipVectorize(
            NullFilter.forColumn("deny-dim2"),
            ImmutableList.of("1", "3", "5")
        );
        assertFilterMatchesSkipVectorize(
            NotDimFilter.of(NullFilter.forColumn("deny-dim2")),
            ImmutableList.of("0", "2", "4")
        );
      }
    }

    @Test
    public void testSingleValueStringColumnWithNulls()
    {
      // testSingleValueStringColumnWithoutNulls but with virtual column selector
      if (NullHandling.replaceWithDefault()) {
        assertFilterMatches(NullFilter.forColumn("dim1"), ImmutableList.of("0"));
        assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("dim1")), ImmutableList.of("1", "2", "3", "4", "5"));

        assertFilterMatches(NullFilter.forColumn("s0"), ImmutableList.of("0", "3"));
        assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("s0")), ImmutableList.of("1", "2", "4", "5"));
      } else {
        assertFilterMatches(NullFilter.forColumn("dim1"), ImmutableList.of());
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("dim1")),
            ImmutableList.of("0", "1", "2", "3", "4", "5")
        );

        assertFilterMatches(NullFilter.forColumn("s0"), ImmutableList.of("3"));
        assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("s0")), ImmutableList.of("0", "1", "2", "4", "5"));
      }
    }

    @Test
    public void testSingleValueVirtualStringColumnWithNulls()
    {
      // testSingleValueStringColumnWithNulls but with virtual column selector
      if (NullHandling.replaceWithDefault()) {
        assertFilterMatches(NullFilter.forColumn("vdim1"), ImmutableList.of("0"));
        assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("vdim1")), ImmutableList.of("1", "2", "3", "4", "5"));

        assertFilterMatches(NullFilter.forColumn("vs0"), ImmutableList.of("0", "3"));
        assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("vs0")), ImmutableList.of("1", "2", "4", "5"));
      } else {
        assertFilterMatches(NullFilter.forColumn("vdim1"), ImmutableList.of());
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("vdim1")),
            ImmutableList.of("0", "1", "2", "3", "4", "5")
        );

        assertFilterMatches(NullFilter.forColumn("vs0"), ImmutableList.of("3"));
        assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("vs0")), ImmutableList.of("0", "1", "2", "4", "5"));
      }
    }

    @Test
    public void testMultiValueStringColumn()
    {
      if (NullHandling.replaceWithDefault()) {
        if (isAutoSchema()) {
          assertFilterMatches(NullFilter.forColumn("dim2"), ImmutableList.of("5"));
          assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("dim2")), ImmutableList.of("0", "1", "2", "3", "4"));
        } else {
          assertFilterMatches(NullFilter.forColumn("dim2"), ImmutableList.of("1", "2", "5"));
          assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("dim2")), ImmutableList.of("0", "3", "4"));
        }
      } else {
        // only one array row is totally null
        if (isAutoSchema()) {
          assertFilterMatches(NullFilter.forColumn("dim2"), ImmutableList.of("5"));
          assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("dim2")), ImmutableList.of("0", "1", "2", "3", "4"));
        } else {
          assertFilterMatches(NullFilter.forColumn("dim2"), ImmutableList.of("1", "5"));
          assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("dim2")), ImmutableList.of("0", "2", "3", "4"));
        }
      }
    }

    @Test
    public void testMissingColumnSpecifiedInDimensionList()
    {
      assertFilterMatches(NullFilter.forColumn("dim3"), ImmutableList.of("0", "1", "2", "3", "4", "5"));
      assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("dim3")), ImmutableList.of());
    }

    @Test
    public void testMissingColumnNotSpecifiedInDimensionList()
    {
      assertFilterMatches(NullFilter.forColumn("dim4"), ImmutableList.of("0", "1", "2", "3", "4", "5"));
      assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("dim4")), ImmutableList.of());
    }


    @Test
    public void testVirtualNumericColumnNullsAndDefaults()
    {
      if (canTestNumericNullsAsDefaultValues) {
        assertFilterMatches(NullFilter.forColumn("vf0"), ImmutableList.of());
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("vf0")),
            ImmutableList.of("0", "1", "2", "3", "4", "5")
        );

        assertFilterMatches(NullFilter.forColumn("vd0"), ImmutableList.of());
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("vd0")),
            ImmutableList.of("0", "1", "2", "3", "4", "5")
        );

        assertFilterMatches(NullFilter.forColumn("vl0"), ImmutableList.of());
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("vl0")),
            ImmutableList.of("0", "1", "2", "3", "4", "5")
        );

        assertFilterMatches(NullFilter.forColumn("vf0-add-sub"), ImmutableList.of());
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("vf0-add-sub")),
            ImmutableList.of("0", "1", "2", "3", "4", "5")
        );

        assertFilterMatches(NullFilter.forColumn("vd0-add-sub"), ImmutableList.of());
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("vd0-add-sub")),
            ImmutableList.of("0", "1", "2", "3", "4", "5")
        );

        assertFilterMatches(NullFilter.forColumn("vl0-add-sub"), ImmutableList.of());
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("vl0-add-sub")),
            ImmutableList.of("0", "1", "2", "3", "4", "5")
        );

        assertFilterMatches(NullFilter.forColumn("double-vf0-add-sub"), ImmutableList.of());
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("double-vf0-add-sub")),
            ImmutableList.of("0", "1", "2", "3", "4", "5")
        );

        assertFilterMatches(NullFilter.forColumn("double-vd0-add-sub"), ImmutableList.of());
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("double-vd0-add-sub")),
            ImmutableList.of("0", "1", "2", "3", "4", "5")
        );

        assertFilterMatches(NullFilter.forColumn("double-vl0-add-sub"), ImmutableList.of());
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("double-vl0-add-sub")),
            ImmutableList.of("0", "1", "2", "3", "4", "5")
        );
      } else {
        assertFilterMatches(NullFilter.forColumn("vf0"), ImmutableList.of("4"));
        assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("vf0")), ImmutableList.of("0", "1", "2", "3", "5"));

        assertFilterMatches(NullFilter.forColumn("vd0"), ImmutableList.of("2"));
        assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("vd0")), ImmutableList.of("0", "1", "3", "4", "5"));

        assertFilterMatches(NullFilter.forColumn("vl0"), ImmutableList.of("3"));
        assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("vl0")), ImmutableList.of("0", "1", "2", "4", "5"));

        if (NullHandling.sqlCompatible()) {
          // these fail in default value mode that cannot be tested as numeric default values becuase of type
          // mismatch for subtract operation
          assertFilterMatches(NullFilter.forColumn("vf0-add-sub"), ImmutableList.of("4"));
          assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("vf0-add-sub")), ImmutableList.of("0", "1", "2", "3", "5"));

          assertFilterMatches(NullFilter.forColumn("vd0-add-sub"), ImmutableList.of("2"));
          assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("vd0-add-sub")), ImmutableList.of("0", "1", "3", "4", "5"));

          assertFilterMatches(NullFilter.forColumn("vl0-add-sub"), ImmutableList.of("3"));
          assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("vl0-add-sub")), ImmutableList.of("0", "1", "2", "4", "5"));

          assertFilterMatches(NullFilter.forColumn("double-vf0-add-sub"), ImmutableList.of("4"));
          assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("double-vf0-add-sub")), ImmutableList.of("0", "1", "2", "3", "5"));

          assertFilterMatches(NullFilter.forColumn("double-vd0-add-sub"), ImmutableList.of("2"));
          assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("double-vd0-add-sub")), ImmutableList.of("0", "1", "3", "4", "5"));

          assertFilterMatches(NullFilter.forColumn("vl0-add-sub"), ImmutableList.of("3"));
          assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("double-vl0-add-sub")), ImmutableList.of("0", "1", "2", "4", "5"));
        }
      }
    }

    @Test
    public void testNumericColumnNullsAndDefaults()
    {
      if (canTestNumericNullsAsDefaultValues) {
        assertFilterMatches(NullFilter.forColumn("f0"), ImmutableList.of());
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("f0")),
            ImmutableList.of("0", "1", "2", "3", "4", "5")
        );

        assertFilterMatches(NullFilter.forColumn("d0"), ImmutableList.of());
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("d0")),
            ImmutableList.of("0", "1", "2", "3", "4", "5")
        );

        assertFilterMatches(NullFilter.forColumn("l0"), ImmutableList.of());
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("l0")),
            ImmutableList.of("0", "1", "2", "3", "4", "5")
        );

      } else {
        assertFilterMatches(NullFilter.forColumn("f0"), ImmutableList.of("4"));
        assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("f0")), ImmutableList.of("0", "1", "2", "3", "5"));

        assertFilterMatches(NullFilter.forColumn("d0"), ImmutableList.of("2"));
        assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("d0")), ImmutableList.of("0", "1", "3", "4", "5"));

        assertFilterMatches(NullFilter.forColumn("l0"), ImmutableList.of("3"));
        assertFilterMatches(NotDimFilter.of(NullFilter.forColumn("l0")), ImmutableList.of("0", "1", "2", "4", "5"));
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
        assertFilterMatches(NullFilter.forColumn("arrayString"), ImmutableList.of("2"));
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("arrayString")),
            ImmutableList.of("0", "1", "3", "4", "5")
        );

        assertFilterMatches(NullFilter.forColumn("arrayLong"), ImmutableList.of("3"));
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("arrayLong")),
            ImmutableList.of("0", "1", "2", "4", "5")
        );

        assertFilterMatches(NullFilter.forColumn("arrayDouble"), ImmutableList.of("5"));
        assertFilterMatches(
            NotDimFilter.of(NullFilter.forColumn("arrayDouble")),
            ImmutableList.of("0", "1", "2", "3", "4")
        );
      }
    }
  }

  public static class NullFilterNonParameterizedTest
  {
    @Test
    public void testGetDimensionRangeSet()
    {
      final NullFilter filter = new NullFilter("x", null);

      Assert.assertEquals(
          TreeRangeSet.create(Collections.singleton(Range.lessThan(""))),
          filter.getDimensionRangeSet("x")
      );

      Assert.assertNull(filter.getDimensionRangeSet("y"));
    }

    @Test
    public void testSerde() throws JsonProcessingException
    {
      ObjectMapper mapper = new DefaultObjectMapper();
      NullFilter filter = new NullFilter("x", null);
      String s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, NullFilter.class));
    }

    @Test
    public void testGetCacheKey()
    {
      NullFilter f1 = new NullFilter("x", null);
      NullFilter f1_2 = new NullFilter("x", null);
      NullFilter f2 = new NullFilter("y", null);
      NullFilter f3 = new NullFilter("x", new FilterTuning(true, 1234, null));
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());
    }

    @Test
    public void test_equals()
    {
      EqualsVerifier.forClass(NullFilter.class).usingGetClass()
                    .withNonnullFields("column")
                    .withIgnoredFields("optimizedFilterIncludeUnknown", "optimizedFilterNoIncludeUnknown")
                    .verify();
    }
  }
}

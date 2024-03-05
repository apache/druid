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
import com.google.common.collect.RangeSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@RunWith(Enclosed.class)
public class TypedInFilterTests
{
  @RunWith(Parameterized.class)
  public static class TypedInFilterTest extends BaseFilterTest
  {
    private final ObjectMapper jsonMapper = new DefaultObjectMapper();

    public TypedInFilterTest(
        String testName,
        IndexBuilder indexBuilder,
        Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
        boolean cnf,
        boolean optimize
    )
    {
      super(testName, InFilterTest.ROWS, indexBuilder, finisher, cnf, optimize);
    }


    @AfterClass
    public static void tearDown() throws Exception
    {
      BaseFilterTest.tearDown(TypedInFilterTest.class.getName());
    }

    @Test
    public void testSingleValueStringColumnWithNulls()
    {
      Assume.assumeTrue(NullHandling.sqlCompatible());

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
    }

    @Test
    public void testMultiValueStringColumn()
    {
      Assume.assumeTrue(NullHandling.sqlCompatible());
      Assume.assumeFalse(isAutoSchema());

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
      Assume.assumeTrue(NullHandling.sqlCompatible());
      assertFilterMatches(
          inFilter("dim3", ColumnType.STRING, Arrays.asList(null, null)),
          ImmutableList.of("a", "b", "c", "d", "e", "f")
      );
      assertFilterMatches(
          NotDimFilter.of(inFilter("dim3", ColumnType.STRING, Arrays.asList(null, null))),
          ImmutableList.of()
      );

      assertFilterMatches(
          inFilter("dim3", ColumnType.STRING, Collections.singletonList("")),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(inFilter("dim3", ColumnType.STRING, Collections.singletonList(""))),
          ImmutableList.of()
      );

      assertFilterMatches(
          inFilter("dim3", ColumnType.STRING, Arrays.asList(null, "a")),
          ImmutableList.of("a", "b", "c", "d", "e", "f")
      );
      assertFilterMatches(
          NotDimFilter.of(inFilter("dim3", ColumnType.STRING, Arrays.asList(null, "a"))),
          ImmutableList.of()
      );

      assertFilterMatches(
          inFilter("dim3", ColumnType.STRING, Collections.singletonList("a")),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(inFilter("dim3", ColumnType.STRING, Collections.singletonList("a"))),
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
    }

    @Override
    protected void assertFilterMatches(DimFilter filter, List<String> expectedRows)
    {
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
      Assert.assertEquals(filter, mapper.readValue(s, TypedInFilter.class));

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
      TypedInFilter filterUnsorted = inFilter("column", ColumnType.STRING, Arrays.asList("a", "b", "b", null, "c"));
      TypedInFilter filterDifferent = inFilter("column", ColumnType.STRING, Arrays.asList("a", "b", "b", "c"));
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

      filterUnsorted = inFilter("column", ColumnType.LONG, Arrays.asList(2L, -2L, 1L, null, 2L, 3L));
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

      filterUnsorted = inFilter("column", ColumnType.DOUBLE, Arrays.asList(2.2, -2.2, 1.1, null, 2.2, 3.3));
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

      filterUnsorted = inFilter("column", ColumnType.FLOAT, Arrays.asList(2.2f, -2.2f, 1.1f, null, 2.2f, 3.3f));
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
            () -> new TypedInFilter(null, ColumnType.STRING, null, null, null)
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
    public void test_equals()
    {
      Assume.assumeTrue(NullHandling.sqlCompatible());
      EqualsVerifier.forClass(TypedInFilter.class).usingGetClass()
                    .withNonnullFields(
                        "column",
                        "matchValueType",
                        "unsortedValues",
                        "lazyMatchValues",
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
                        "lazyMatchValueBytes",
                        "predicateFactorySupplier",
                        "cacheKeySupplier",
                        "optimizedFilterIncludeUnknown",
                        "optimizedFilterNoIncludeUnknown"
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
}

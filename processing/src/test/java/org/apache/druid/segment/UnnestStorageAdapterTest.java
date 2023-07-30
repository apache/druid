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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
<<<<<<< HEAD
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
=======
>>>>>>> 27981e5e51 (Refactor logic for unnest filters/query filters and add tests)
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.BoundFilter;
import org.apache.druid.segment.filter.ColumnComparisonFilter;
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.filter.LikeFilter;
import org.apache.druid.segment.filter.NotFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.filter.TrueFilter;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.join.PostJoinCursor;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.utils.CloseableUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.joda.time.Interval;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

import static org.apache.druid.segment.filter.FilterTestUtils.sdf;
import static org.apache.druid.segment.filter.FilterTestUtils.sdfd;
import static org.apache.druid.segment.filter.Filters.and;
import static org.apache.druid.segment.filter.Filters.or;

public class UnnestStorageAdapterTest extends InitializedNullHandlingTest
{
  private static Closer CLOSER;
  private static IncrementalIndex INCREMENTAL_INDEX;
  private static IncrementalIndexStorageAdapter INCREMENTAL_INDEX_STORAGE_ADAPTER;
  private static UnnestStorageAdapter UNNEST_STORAGE_ADAPTER;
  private static UnnestStorageAdapter UNNEST_STORAGE_ADAPTER1;
  private static List<StorageAdapter> ADAPTERS;
  private static String COLUMNNAME = "multi-string1";
  private static String OUTPUT_COLUMN_NAME = "unnested-multi-string1";
  private static String OUTPUT_COLUMN_NAME1 = "unnested-multi-string1-again";

  @BeforeClass
  public static void setup()
  {
    CLOSER = Closer.create();
    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("expression-testbench");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();
    final SegmentGenerator segmentGenerator = CLOSER.register(new SegmentGenerator());

    final int numRows = 2;
    INCREMENTAL_INDEX = CLOSER.register(
        segmentGenerator.generateIncrementalIndex(dataSegment, schemaInfo, Granularities.HOUR, numRows)
    );
    INCREMENTAL_INDEX_STORAGE_ADAPTER = new IncrementalIndexStorageAdapter(INCREMENTAL_INDEX);
    UNNEST_STORAGE_ADAPTER = new UnnestStorageAdapter(
        INCREMENTAL_INDEX_STORAGE_ADAPTER,
        new ExpressionVirtualColumn(OUTPUT_COLUMN_NAME, "\"" + COLUMNNAME + "\"", null, ExprMacroTable.nil()),
        null
    );

    UNNEST_STORAGE_ADAPTER1 = new UnnestStorageAdapter(
        UNNEST_STORAGE_ADAPTER,
        new ExpressionVirtualColumn(OUTPUT_COLUMN_NAME1, "\"" + COLUMNNAME + "\"", null, ExprMacroTable.nil()),
        null
    );

    ADAPTERS = ImmutableList.of(
        UNNEST_STORAGE_ADAPTER,
        UNNEST_STORAGE_ADAPTER1
    );
  }

  @AfterClass
  public static void teardown()
  {
    CloseableUtils.closeAndSuppressExceptions(CLOSER, throwable -> {
    });
  }

  @Test
  public void test_group_of_unnest_adapters_methods()
  {
    String colName = "multi-string1";
    for (StorageAdapter adapter : ADAPTERS) {
      Assert.assertEquals(
          DateTimes.of("2000-01-01T23:00:00.000Z"),
          adapter.getMaxTime()
      );
      Assert.assertEquals(
          DateTimes.of("2000-01-01T12:00:00.000Z"),
          adapter.getMinTime()
      );
      adapter.getColumnCapabilities(colName);
      Assert.assertEquals(adapter.getNumRows(), 0);
      Assert.assertNotNull(adapter.getMetadata());
      Assert.assertEquals(
          DateTimes.of("2000-01-01T23:59:59.999Z"),
          adapter.getMaxIngestedEventTime()
      );
      Assert.assertEquals(
          adapter.getColumnCapabilities(colName).toColumnType(),
          INCREMENTAL_INDEX_STORAGE_ADAPTER.getColumnCapabilities(colName).toColumnType()
      );

      assertColumnReadsIdentifier(((UnnestStorageAdapter) adapter).getUnnestColumn(), colName);
    }
  }

  @Test
  public void test_group_of_unnest_adapters_column_capabilities()
  {
    String colName = "multi-string1";
    List<String> columnsInTable = Arrays.asList(
        "string1",
        "long1",
        "double1",
        "float1",
        "multi-string1",
        OUTPUT_COLUMN_NAME
    );
    List<ValueType> valueTypes = Arrays.asList(
        ValueType.STRING,
        ValueType.LONG,
        ValueType.DOUBLE,
        ValueType.FLOAT,
        ValueType.STRING,
        ValueType.STRING
    );
    UnnestStorageAdapter adapter = UNNEST_STORAGE_ADAPTER;

    for (int i = 0; i < columnsInTable.size(); i++) {
      ColumnCapabilities capabilities = adapter.getColumnCapabilities(columnsInTable.get(i));
      Assert.assertEquals(capabilities.getType(), valueTypes.get(i));
    }
    assertColumnReadsIdentifier(adapter.getUnnestColumn(), colName);

  }

  @Test
  public void test_unnest_adapters_basic()
  {

    Sequence<Cursor> cursorSequence = UNNEST_STORAGE_ADAPTER.makeCursors(
        null,
        UNNEST_STORAGE_ADAPTER.getInterval(),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    cursorSequence.accumulate(null, (accumulated, cursor) -> {
      ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();

      DimensionSelector dimSelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(OUTPUT_COLUMN_NAME));
      int count = 0;
      while (!cursor.isDone()) {
        Object dimSelectorVal = dimSelector.getObject();
        if (dimSelectorVal == null) {
          Assert.assertNull(dimSelectorVal);
        }
        cursor.advance();
        count++;
      }
        /*
      each row has 8 entries.
      unnest 2 rows -> 16 rows after unnest
       */
      Assert.assertEquals(count, 16);
      return null;
    });

  }

  @Test
  public void test_two_levels_of_unnest_adapters()
  {
    Sequence<Cursor> cursorSequence = UNNEST_STORAGE_ADAPTER1.makeCursors(
        null,
        UNNEST_STORAGE_ADAPTER1.getInterval(),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );


    cursorSequence.accumulate(null, (accumulated, cursor) -> {
      ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();

      DimensionSelector dimSelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(OUTPUT_COLUMN_NAME1));
      ColumnValueSelector valueSelector = factory.makeColumnValueSelector(OUTPUT_COLUMN_NAME1);

      int count = 0;
      while (!cursor.isDone()) {
        Object dimSelectorVal = dimSelector.getObject();
        Object valueSelectorVal = valueSelector.getObject();
        if (dimSelectorVal == null) {
          Assert.assertNull(dimSelectorVal);
        } else if (valueSelectorVal == null) {
          Assert.assertNull(valueSelectorVal);
        }
        cursor.advance();
        count++;
      }
      /*
      each row has 8 entries.
      unnest 2 rows -> 16 entries also the value cardinality
      unnest of unnest -> 16*8 = 128 rows
       */
      Assert.assertEquals(count, 128);
      Assert.assertEquals(dimSelector.getValueCardinality(), 16);
      return null;
    });
  }

  private static void assertColumnReadsIdentifier(final VirtualColumn column, final String identifier)
  {
    MatcherAssert.assertThat(column, CoreMatchers.instanceOf(ExpressionVirtualColumn.class));
    Assert.assertEquals("\"" + identifier + "\"", ((ExpressionVirtualColumn) column).getExpression());
  }

  @Test
  public void test_pushdown_or_filters_unnested_and_original_dimension_with_unnest_adapters()
  {
    final UnnestStorageAdapter unnestStorageAdapter = new UnnestStorageAdapter(
        new TestStorageAdapter(INCREMENTAL_INDEX),
        new ExpressionVirtualColumn(OUTPUT_COLUMN_NAME, "\"" + COLUMNNAME + "\"", null, ExprMacroTable.nil()),
        null
    );

    final VirtualColumn vc = unnestStorageAdapter.getUnnestColumn();

    final String inputColumn = unnestStorageAdapter.getUnnestInputIfDirectAccess(vc);

    final OrFilter baseFilter = new OrFilter(ImmutableList.of(
        sdf(OUTPUT_COLUMN_NAME, "1", null),
        sdf(inputColumn, "2", null)
    ));

    final OrFilter expectedPushDownFilter = new OrFilter(ImmutableList.of(
        sdf(inputColumn, "1", null),
        sdf(inputColumn, "2", null)
    ));

    final Sequence<Cursor> cursorSequence = unnestStorageAdapter.makeCursors(
        baseFilter,
        unnestStorageAdapter.getInterval(),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    final TestStorageAdapter base = (TestStorageAdapter) unnestStorageAdapter.getBaseAdapter();
    final Filter pushDownFilter = base.getPushDownFilter();

    Assert.assertEquals(expectedPushDownFilter, pushDownFilter);
    cursorSequence.accumulate(null, (accumulated, cursor) -> {
      Assert.assertEquals(cursor.getClass(), PostJoinCursor.class);
      final Filter postFilter = ((PostJoinCursor) cursor).getPostJoinFilter();
      // OR-case so base filter should match the postJoinFilter
      Assert.assertEquals(baseFilter, postFilter);
      return null;
    });
  }
  @Test
  public void test_nested_filters_unnested_and_original_dimension_with_unnest_adapters()
  {
    final UnnestStorageAdapter unnestStorageAdapter = new UnnestStorageAdapter(
        new TestStorageAdapter(INCREMENTAL_INDEX),
        new ExpressionVirtualColumn(OUTPUT_COLUMN_NAME, "\"" + COLUMNNAME + "\"", null, ExprMacroTable.nil()),
        null
    );

    final VirtualColumn vc = unnestStorageAdapter.getUnnestColumn();

    final String inputColumn = unnestStorageAdapter.getUnnestInputIfDirectAccess(vc);

    final OrFilter baseFilter = new OrFilter(ImmutableList.of(
        sdf(OUTPUT_COLUMN_NAME, "1", null),
        new AndFilter(ImmutableList.of(
            sdf(inputColumn, "2", null),
            sdf(OUTPUT_COLUMN_NAME, "10", null)
        ))
    ));

    final OrFilter expectedPushDownFilter = new OrFilter(ImmutableList.of(
        sdf(inputColumn, "1", null),
        new AndFilter(ImmutableList.of(
            sdf(inputColumn, "2", null),
            sdf(inputColumn, "10", null)
        ))
    ));

    final Sequence<Cursor> cursorSequence = unnestStorageAdapter.makeCursors(
        baseFilter,
        unnestStorageAdapter.getInterval(),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    final TestStorageAdapter base = (TestStorageAdapter) unnestStorageAdapter.getBaseAdapter();
    final Filter pushDownFilter = base.getPushDownFilter();

    Assert.assertEquals(expectedPushDownFilter, pushDownFilter);
    cursorSequence.accumulate(null, (accumulated, cursor) -> {
      Assert.assertEquals(cursor.getClass(), PostJoinCursor.class);
      final Filter postFilter = ((PostJoinCursor) cursor).getPostJoinFilter();
      // OR-case so base filter should match the postJoinFilter
      Assert.assertEquals(baseFilter, postFilter);
      return null;
    });
  }
  @Test
  public void test_nested_filters_unnested_and_topLevel1And3filtersInOR()
  {
    final Filter testQueryFilter = and(ImmutableList.of(
        sdf(OUTPUT_COLUMN_NAME, "3", null),
        or(ImmutableList.of(
            sdf("newcol", "2", null),
            sdf(COLUMNNAME, "2", null),
            sdf(OUTPUT_COLUMN_NAME, "1", null)
        ))
    ));
    testComputeBaseAndPostUnnestFilters(
        testQueryFilter,
        "unnested-multi-string1 = 3",
        "(unnested-multi-string1 = 3 && (newcol = 2 || multi-string1 = 2 || unnested-multi-string1 = 1))"
    );
  }

  @Test
  public void test_nested_filters_unnested_and_topLevelORAnd3filtersInOR()
  {
    final Filter testQueryFilter = or(ImmutableList.of(
        sdf(OUTPUT_COLUMN_NAME, "3", null),
        and(ImmutableList.of(
            sdf("newcol", "2", null),
            sdf(COLUMNNAME, "2", null),
            sdf(OUTPUT_COLUMN_NAME, "1", null)
        ))
    ));
    testComputeBaseAndPostUnnestFilters(
        testQueryFilter,
        "",
        "(unnested-multi-string1 = 3 || (newcol = 2 && multi-string1 = 2 && unnested-multi-string1 = 1))"
    );
  }

  @Test
  public void test_nested_filters_unnested_and_topLevelAND3filtersInORWithNestedOrs()
  {
    final Filter testQueryFilter = and(ImmutableList.of(
        sdf(OUTPUT_COLUMN_NAME, "3", null),
        or(ImmutableList.of(
            sdf("newcol", "2", null),
            sdf(COLUMNNAME, "2", null)
        )),
        or(ImmutableList.of(
            sdf("newcol", "4", null),
            sdf(COLUMNNAME, "8", null),
            sdf(OUTPUT_COLUMN_NAME, "6", null)
        ))
    ));
    testComputeBaseAndPostUnnestFilters(
        testQueryFilter,
        "unnested-multi-string1 = 3",
        "(unnested-multi-string1 = 3 && (newcol = 2 || multi-string1 = 2) && (newcol = 4 || multi-string1 = 8 || unnested-multi-string1 = 6))"
    );
  }

  @Test
  public void test_nested_filters_unnested_and_topLevelAND2sdf()
  {
    final Filter testQueryFilter = and(ImmutableList.of(
        sdf(OUTPUT_COLUMN_NAME, "3", null),
        sdf(COLUMNNAME, "2", null)
    ));
    testComputeBaseAndPostUnnestFilters(
        testQueryFilter,
        "unnested-multi-string1 = 3",
        "(unnested-multi-string1 = 3 && multi-string1 = 2)"
    );
  }
  @Test
  public void test_pushdown_filters_unnested_dimension_with_unnest_adapters()
  {
    final UnnestStorageAdapter unnestStorageAdapter = new UnnestStorageAdapter(
        new TestStorageAdapter(INCREMENTAL_INDEX),
        new ExpressionVirtualColumn(OUTPUT_COLUMN_NAME, "\"" + COLUMNNAME + "\"", null, ExprMacroTable.nil()),
         sdfd(OUTPUT_COLUMN_NAME, "1", null)
    );

    final VirtualColumn vc = unnestStorageAdapter.getUnnestColumn();

    final String inputColumn = unnestStorageAdapter.getUnnestInputIfDirectAccess(vc);

    final Filter expectedPushDownFilter =
        sdf(inputColumn, "1", null);


    final Sequence<Cursor> cursorSequence = unnestStorageAdapter.makeCursors(
        null,
        unnestStorageAdapter.getInterval(),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    final TestStorageAdapter base = (TestStorageAdapter) unnestStorageAdapter.getBaseAdapter();
    final Filter pushDownFilter = base.getPushDownFilter();

    Assert.assertEquals(expectedPushDownFilter, pushDownFilter);
    cursorSequence.accumulate(null, (accumulated, cursor) -> {
      Assert.assertEquals(cursor.getClass(), PostJoinCursor.class);
      final Filter postFilter = ((PostJoinCursor) cursor).getPostJoinFilter();
      Assert.assertEquals(unnestStorageAdapter.getUnnestFilter(), postFilter);

      int count = 0;
      while (!cursor.isDone()) {
        cursor.advance();
        count++;
      }
      Assert.assertEquals(1, count);
      return null;
    });
  }


  @Test
  public void test_pushdown_filters_unnested_dimension_outside()
  {
    final UnnestStorageAdapter unnestStorageAdapter = new UnnestStorageAdapter(
        new TestStorageAdapter(INCREMENTAL_INDEX),
        new ExpressionVirtualColumn(OUTPUT_COLUMN_NAME, "\"" + COLUMNNAME + "\"", null, ExprMacroTable.nil()),
        null
    );

    final VirtualColumn vc = unnestStorageAdapter.getUnnestColumn();

    final String inputColumn = unnestStorageAdapter.getUnnestInputIfDirectAccess(vc);

    final Filter expectedPushDownFilter =
        sdf(inputColumn, "1", null);

    final Filter queryFilter = sdf(OUTPUT_COLUMN_NAME, "1", null);
    final Sequence<Cursor> cursorSequence = unnestStorageAdapter.makeCursors(
        queryFilter,
        unnestStorageAdapter.getInterval(),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    final TestStorageAdapter base = (TestStorageAdapter) unnestStorageAdapter.getBaseAdapter();
    final Filter pushDownFilter = base.getPushDownFilter();

    Assert.assertEquals(expectedPushDownFilter, pushDownFilter);
    cursorSequence.accumulate(null, (accumulated, cursor) -> {
      Assert.assertEquals(cursor.getClass(), PostJoinCursor.class);
      final Filter postFilter = ((PostJoinCursor) cursor).getPostJoinFilter();
      Assert.assertEquals(queryFilter, postFilter);

      int count = 0;
      while (!cursor.isDone()) {
        cursor.advance();
        count++;
      }
      Assert.assertEquals(1, count);
      return null;
    });
  }

<<<<<<< HEAD
  @Test
  public void testAllowedFiltersForPushdown()
  {
    Filter[] allowed = new Filter[] {
        new SelectorFilter("column", "value"),
        new InDimFilter("column", ImmutableSet.of("a", "b")),
        new LikeFilter("column", null, LikeDimFilter.LikeMatcher.from("hello%", null), null),
        new BoundFilter(
            new BoundDimFilter("column", "a", "b", true, true, null, null, null)
        ),
        NullFilter.forColumn("column"),
        new EqualityFilter("column", ColumnType.LONG, 1234L, null),
        new RangeFilter("column", ColumnType.LONG, 0L, 1234L, true, false, null)
    };
    // not exhaustive
    Filter[] notAllowed = new Filter[] {
        TrueFilter.instance(),
        FalseFilter.instance(),
        new ColumnComparisonFilter(ImmutableList.of(DefaultDimensionSpec.of("col1"), DefaultDimensionSpec.of("col2")))
    };

    for (Filter f : allowed) {
      Assert.assertTrue(UnnestStorageAdapter.filterMapsOverMultiValueStrings(f));
    }
    for (Filter f : notAllowed) {
      Assert.assertFalse(UnnestStorageAdapter.filterMapsOverMultiValueStrings(f));
    }

    Filter notAnd = new NotFilter(
        new AndFilter(
            Arrays.asList(allowed)
        )
    );

    Assert.assertTrue(UnnestStorageAdapter.filterMapsOverMultiValueStrings(notAnd));
    Assert.assertTrue(UnnestStorageAdapter.filterMapsOverMultiValueStrings(new OrFilter(Arrays.asList(allowed))));
    Assert.assertTrue(UnnestStorageAdapter.filterMapsOverMultiValueStrings(new NotFilter(notAnd)));
    Assert.assertFalse(
        UnnestStorageAdapter.filterMapsOverMultiValueStrings(new NotFilter(new OrFilter(Arrays.asList(notAllowed))))
=======
  public void testComputeBaseAndPostUnnestFilters(
      Filter testQueryFilter,
      String expectedBasePushDown,
      String expectedPostUnnest
  )
  {
    final String inputColumn = UNNEST_STORAGE_ADAPTER.getUnnestInputIfDirectAccess(UNNEST_STORAGE_ADAPTER.getUnnestColumn());
    final VirtualColumn vc = UNNEST_STORAGE_ADAPTER.getUnnestColumn();
    Pair<Filter, Filter> filterPair = UNNEST_STORAGE_ADAPTER.computeBaseAndPostUnnestFilters(
        testQueryFilter,
        null,
        VirtualColumns.EMPTY,
        inputColumn,
        vc.capabilities(inputColumn)
    );
    Filter actualPushDownFilter = filterPair.lhs;
    Filter actualPostUnnestFilter = filterPair.rhs;
    Assert.assertEquals(
        "Expects only top level child of And Filter to push down to base",
        expectedBasePushDown,
        actualPushDownFilter == null ? "" : actualPushDownFilter.toString()
    );
    Assert.assertEquals(
        "Should have post unnest filter",
        expectedPostUnnest,
        actualPostUnnestFilter == null ? "" : actualPostUnnestFilter.toString()
>>>>>>> 27981e5e51 (Refactor logic for unnest filters/query filters and add tests)
    );
  }
}

/**
 * Class to test the flow of pushing down filters into the base cursor
 * while using the UnnestStorageAdapter. This class keeps a reference of the filter
 * which is pushed down to the cursor which serves as a checkpoint to validate
 * if the right filter is being pushed down
 */
class TestStorageAdapter extends IncrementalIndexStorageAdapter
{

  private Filter pushDownFilter;

  public TestStorageAdapter(IncrementalIndex index)
  {
    super(index);
  }

  public Filter getPushDownFilter()
  {
    return pushDownFilter;
  }

  @Override
  public Sequence<Cursor> makeCursors(
      @Nullable final Filter filter,
      final Interval interval,
      final VirtualColumns virtualColumns,
      final Granularity gran,
      final boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    this.pushDownFilter = filter;
    return super.makeCursors(filter, interval, virtualColumns, gran, descending, queryMetrics);
  }
}

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
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.ResourceInputSource;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.join.PostJoinCursor;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.utils.CloseableUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static org.apache.druid.segment.filter.FilterTestUtils.not;
import static org.apache.druid.segment.filter.FilterTestUtils.selector;
import static org.apache.druid.segment.filter.Filters.and;
import static org.apache.druid.segment.filter.Filters.or;

public class UnnestStorageAdapterTest extends InitializedNullHandlingTest
{
  @ClassRule
  public static TemporaryFolder tmp = new TemporaryFolder();
  private static Closer CLOSER;
  private static IncrementalIndex INCREMENTAL_INDEX;
  private static IncrementalIndexStorageAdapter INCREMENTAL_INDEX_STORAGE_ADAPTER;
  private static QueryableIndex QUERYABLE_INDEX;
  private static UnnestStorageAdapter UNNEST_STORAGE_ADAPTER;
  private static UnnestStorageAdapter UNNEST_STORAGE_ADAPTER1;
  private static UnnestStorageAdapter UNNEST_ARRAYS;
  private static List<StorageAdapter> ADAPTERS;
  private static String INPUT_COLUMN_NAME = "multi-string1";
  private static String OUTPUT_COLUMN_NAME = "unnested-multi-string1";
  private static String OUTPUT_COLUMN_NAME1 = "unnested-multi-string1-again";


  @BeforeClass
  public static void setup() throws IOException
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
        new ExpressionVirtualColumn(OUTPUT_COLUMN_NAME, "\"" + INPUT_COLUMN_NAME + "\"", null, ExprMacroTable.nil()),
        null
    );

    UNNEST_STORAGE_ADAPTER1 = new UnnestStorageAdapter(
        UNNEST_STORAGE_ADAPTER,
        new ExpressionVirtualColumn(OUTPUT_COLUMN_NAME1, "\"" + INPUT_COLUMN_NAME + "\"", null, ExprMacroTable.nil()),
        null
    );

    final InputSource inputSource = ResourceInputSource.of(
        UnnestStorageAdapterTest.class.getClassLoader(),
        NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE
    );
    IndexBuilder bob = IndexBuilder.create()
                                   .tmpDir(tmp.newFolder())
                                   .schema(
                                       IncrementalIndexSchema.builder()
                                                             .withTimestampSpec(NestedDataTestUtils.TIMESTAMP_SPEC)
                                                             .withDimensionsSpec(NestedDataTestUtils.AUTO_DISCOVERY)
                                                             .withQueryGranularity(Granularities.DAY)
                                                             .withRollup(false)
                                                             .withMinTimestamp(0)
                                                             .build()
                                   )
                                   .indexSpec(IndexSpec.DEFAULT)
                                   .inputSource(inputSource)
                                   .inputFormat(NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT)
                                   .transform(TransformSpec.NONE)
                                   .inputTmpDir(tmp.newFolder());
    QUERYABLE_INDEX = CLOSER.register(bob.buildMMappedIndex());
    UNNEST_ARRAYS = new UnnestStorageAdapter(
        new QueryableIndexStorageAdapter(QUERYABLE_INDEX),
        new ExpressionVirtualColumn("u", "\"arrayLongNulls\"", ColumnType.LONG, ExprMacroTable.nil()),
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
      adapter.getColumnCapabilities(colName);
      Assert.assertEquals(adapter.getNumRows(), 0);
      Assert.assertNotNull(adapter.getMetadata());
      Assert.assertEquals(
          adapter.getColumnCapabilities(colName).toColumnType(),
          INCREMENTAL_INDEX_STORAGE_ADAPTER.getColumnCapabilities(colName).toColumnType()
      );

      assertColumnReadsIdentifier(((UnnestStorageAdapter) adapter).getUnnestColumn(), colName);
    }
  }

  @Test
  public void test_unnest_adapter_column_capabilities()
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
    Assert.assertEquals(
        adapter.getColumnCapabilities(OUTPUT_COLUMN_NAME).isDictionaryEncoded(),
        ColumnCapabilities.Capable.TRUE // passed through from dict-encoded input
    );
    Assert.assertEquals(
        adapter.getColumnCapabilities(OUTPUT_COLUMN_NAME).hasMultipleValues(),
        ColumnCapabilities.Capable.FALSE
    );
  }

  @Test
  public void test_unnest_adapters_basic()
  {
    try (final CursorHolder cursorHolder = UNNEST_STORAGE_ADAPTER.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      Cursor cursor = cursorHolder.asCursor();

      ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();

      DimensionSelector dimSelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(OUTPUT_COLUMN_NAME));
      int count = 0;

      List<Object> rows = new ArrayList<>();
      // test cursor reset
      while (!cursor.isDone()) {
        cursor.advance();
      }
      cursor.reset();

      while (!cursor.isDone()) {
        Object dimSelectorVal = dimSelector.getObject();
        rows.add(dimSelectorVal);
        cursor.advance();
        count++;
      }
        /*
      each row has 8 entries.
      unnest 2 rows -> 16 rows after unnest
       */
      Assert.assertEquals(count, 16);
      Assert.assertEquals(
          Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "10", "11", "12", "13", "14", "15", "8", "9"),
          rows
      );
    }
  }

  @Test
  public void test_unnest_adapters_basic_array_column()
  {
    try (final CursorHolder cursorHolder = UNNEST_ARRAYS.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      Cursor cursor = cursorHolder.asCursor();

      ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();

      ColumnValueSelector dimSelector = factory.makeColumnValueSelector("u");
      int count = 0;

      List<Object> rows = new ArrayList<>();
      // test cursor reset
      while (!cursor.isDone()) {
        cursor.advance();
      }
      cursor.reset();

      while (!cursor.isDone()) {
        Object dimSelectorVal = dimSelector.getObject();
        rows.add(dimSelectorVal);
        cursor.advance();
        count++;
      }
      Assert.assertEquals(count, 12);
      Assert.assertEquals(
          Arrays.asList(2L, 3L, 1L, null, 3L, 1L, null, 2L, 9L, 1L, 2L, 3L),
          rows
      );
    }
  }

  @Test
  public void test_unnest_adapters_basic_row_based_array_column()
  {
    StorageAdapter adapter = new UnnestStorageAdapter(
        new RowBasedStorageAdapter<>(
            Sequences.simple(
                Arrays.asList(
                    new Object[]{1L, new Object[]{1L, 2L}},
                    new Object[]{1L, new Object[]{3L, 4L, 5L}},
                    new Object[]{2L, new Object[]{6L, null, 7L}},
                    new Object[]{2L, null},
                    new Object[]{3L, new Object[]{8L, 9L, 10L}}
                )
            ),
            new RowAdapter<Object[]>()
            {
              @Override
              public ToLongFunction<Object[]> timestampFunction()
              {
                return value -> (long) value[0];
              }

              @Override
              public Function<Object[], Object> columnFunction(String columnName)
              {
                if ("a".equals(columnName)) {
                  return objects -> objects[1];
                }
                return null;
              }
            },
            RowSignature.builder().add("arrayLongNulls", ColumnType.LONG_ARRAY).build()
        ),
        new ExpressionVirtualColumn("u", "\"a\"", ColumnType.LONG, ExprMacroTable.nil()),
        null
    );
    try (final CursorHolder cursorHolder = adapter.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      Cursor cursor = cursorHolder.asCursor();

      ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();

      ColumnValueSelector dimSelector = factory.makeColumnValueSelector("u");
      int count = 0;

      List<Object> rows = new ArrayList<>();
      // test cursor reset
      while (!cursor.isDone()) {
        cursor.advance();
      }
      cursor.reset();

      while (!cursor.isDone()) {
        Object dimSelectorVal = dimSelector.getObject();
        rows.add(dimSelectorVal);
        cursor.advance();
        count++;
      }
      Assert.assertEquals(count, 11);
      Assert.assertEquals(
          Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, null, 7L, 8L, 9L, 10L),
          rows
      );
    }
  }

  @Test
  public void test_two_levels_of_unnest_adapters()
  {
    try (final CursorHolder cursorHolder = UNNEST_STORAGE_ADAPTER1.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      Cursor cursor = cursorHolder.asCursor();
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
      unnest 2 rows -> 16 entries also the value cardinality, but null is not present in the dictionary and so is
                       fabricated so cardinality is 17
      unnest of unnest -> 16*8 = 128 rows
       */
      Assert.assertEquals(count, 128);
      Assert.assertEquals(dimSelector.getValueCardinality(), 17);
    }
  }

  @Test
  public void test_pushdown_or_filters_unnested_and_original_dimension_with_unnest_adapters()
  {
    final UnnestStorageAdapter unnestStorageAdapter = new UnnestStorageAdapter(
        new TestStorageAdapter(INCREMENTAL_INDEX),
        new ExpressionVirtualColumn(OUTPUT_COLUMN_NAME, "\"" + INPUT_COLUMN_NAME + "\"", null, ExprMacroTable.nil()),
        null
    );

    final VirtualColumn vc = unnestStorageAdapter.getUnnestColumn();

    final String inputColumn = unnestStorageAdapter.getUnnestInputIfDirectAccess(vc);

    final OrFilter baseFilter = new OrFilter(ImmutableList.of(
        selector(OUTPUT_COLUMN_NAME, "1"),
        selector(inputColumn, "2")
    ));

    final OrFilter expectedPushDownFilter = new OrFilter(ImmutableList.of(
        selector(inputColumn, "1"),
        selector(inputColumn, "2")
    ));

    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setFilter(baseFilter)
                                                     .build();
    try (final CursorHolder cursorHolder = unnestStorageAdapter.makeCursorHolder(buildSpec)) {
      Cursor cursor = cursorHolder.asCursor();
      final TestStorageAdapter base = (TestStorageAdapter) unnestStorageAdapter.getBaseAdapter();
      final Filter pushDownFilter = base.getPushDownFilter();

      Assert.assertEquals(expectedPushDownFilter, pushDownFilter);
      Assert.assertEquals(cursor.getClass(), PostJoinCursor.class);
      final Filter postFilter = ((PostJoinCursor) cursor).getPostJoinFilter();
      // OR-case so base filter should match the postJoinFilter
      Assert.assertEquals(baseFilter, postFilter);
    }
  }

  @Test
  public void test_nested_filters_unnested_and_original_dimension_with_unnest_adapters()
  {
    final UnnestStorageAdapter unnestStorageAdapter = new UnnestStorageAdapter(
        new TestStorageAdapter(INCREMENTAL_INDEX),
        new ExpressionVirtualColumn(OUTPUT_COLUMN_NAME, "\"" + INPUT_COLUMN_NAME + "\"", null, ExprMacroTable.nil()),
        null
    );

    final VirtualColumn vc = unnestStorageAdapter.getUnnestColumn();

    final String inputColumn = unnestStorageAdapter.getUnnestInputIfDirectAccess(vc);

    final OrFilter baseFilter = new OrFilter(ImmutableList.of(
        selector(OUTPUT_COLUMN_NAME, "1"),
        new AndFilter(ImmutableList.of(
            selector(inputColumn, "2"),
            selector(OUTPUT_COLUMN_NAME, "10")
        ))
    ));

    final OrFilter expectedPushDownFilter = new OrFilter(ImmutableList.of(
        selector(inputColumn, "1"),
        new AndFilter(ImmutableList.of(
            selector(inputColumn, "2"),
            selector(inputColumn, "10")
        ))
    ));

    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setFilter(baseFilter)
                                                     .build();
    try (final CursorHolder cursorHolder = unnestStorageAdapter.makeCursorHolder(buildSpec)) {
      Cursor cursor = cursorHolder.asCursor();
      final TestStorageAdapter base = (TestStorageAdapter) unnestStorageAdapter.getBaseAdapter();
      final Filter pushDownFilter = base.getPushDownFilter();

      Assert.assertEquals(expectedPushDownFilter, pushDownFilter);
      Assert.assertEquals(cursor.getClass(), PostJoinCursor.class);
      final Filter postFilter = ((PostJoinCursor) cursor).getPostJoinFilter();
      // OR-case so base filter should match the postJoinFilter
      Assert.assertEquals(baseFilter, postFilter);
    }
  }

  @Test
  public void test_nested_filters_unnested_and_topLevel1And3filtersInOR()
  {
    final Filter testQueryFilter = and(ImmutableList.of(
        selector(OUTPUT_COLUMN_NAME, "3"),
        or(ImmutableList.of(
            selector("newcol", "2"),
            selector(INPUT_COLUMN_NAME, "2"),
            selector(OUTPUT_COLUMN_NAME, "1")
        ))
    ));
    testComputeBaseAndPostUnnestFilters(
        testQueryFilter,
        "(multi-string1 = 3 && (newcol = 2 || multi-string1 = 2 || multi-string1 = 1))",
        "(unnested-multi-string1 = 3 && (newcol = 2 || multi-string1 = 2 || unnested-multi-string1 = 1))"
    );
  }

  @Test
  public void test_nested_multiLevel_filters_unnested()
  {
    final Filter testQueryFilter = and(ImmutableList.of(
        selector(OUTPUT_COLUMN_NAME, "3"),
        or(ImmutableList.of(
            or(ImmutableList.of(
                selector("newcol", "2"),
                selector(INPUT_COLUMN_NAME, "2"),
                and(ImmutableList.of(
                    selector("newcol", "3"),
                    selector(INPUT_COLUMN_NAME, "7")
                ))
            )),
            selector(OUTPUT_COLUMN_NAME, "1")
        ))
    ));
    testComputeBaseAndPostUnnestFilters(
        testQueryFilter,
        "(multi-string1 = 3 && (newcol = 2 || multi-string1 = 2 || (newcol = 3 && multi-string1 = 7) || multi-string1 = 1))",
        "(unnested-multi-string1 = 3 && (newcol = 2 || multi-string1 = 2 || (newcol = 3 && multi-string1 = 7) || unnested-multi-string1 = 1))"
    );
  }

  @Test
  public void test_nested_multiLevel_filters_unnested5Level()
  {
    final Filter testQueryFilter = or(ImmutableList.of(
        selector(OUTPUT_COLUMN_NAME, "3"),
        or(ImmutableList.of(
            or(ImmutableList.of(
                selector("newcol", "2"),
                selector(INPUT_COLUMN_NAME, "2"),
                and(ImmutableList.of(
                    selector("newcol", "3"),
                    and(ImmutableList.of(
                        selector(INPUT_COLUMN_NAME, "7"),
                        selector("newcol_1", "10")
                    ))
                ))
            )),
            selector(OUTPUT_COLUMN_NAME, "1")
        ))
    ));
    testComputeBaseAndPostUnnestFilters(
        testQueryFilter,
        "(multi-string1 = 3 || newcol = 2 || multi-string1 = 2 || (newcol = 3 && multi-string1 = 7 && newcol_1 = 10) || multi-string1 = 1)",
        "(unnested-multi-string1 = 3 || newcol = 2 || multi-string1 = 2 || (newcol = 3 && multi-string1 = 7 && newcol_1 = 10) || unnested-multi-string1 = 1)"
    );
  }

  @Test
  public void test_nested_filters_unnested_and_topLevelORAnd3filtersInOR()
  {
    final Filter testQueryFilter = or(ImmutableList.of(
        selector(OUTPUT_COLUMN_NAME, "3"),
        and(ImmutableList.of(
            selector("newcol", "2"),
            selector(INPUT_COLUMN_NAME, "2"),
            selector(OUTPUT_COLUMN_NAME, "1")
        ))
    ));
    testComputeBaseAndPostUnnestFilters(
        testQueryFilter,
        "(multi-string1 = 3 || (newcol = 2 && multi-string1 = 2 && multi-string1 = 1))",
        "(unnested-multi-string1 = 3 || (newcol = 2 && multi-string1 = 2 && unnested-multi-string1 = 1))"
    );
  }

  @Test
  public void test_nested_filters_unnested_and_topLevelAND3filtersInORWithNestedOrs()
  {
    final Filter testQueryFilter = and(ImmutableList.of(
        selector(OUTPUT_COLUMN_NAME, "3"),
        or(ImmutableList.of(
            selector("newcol", "2"),
            selector(INPUT_COLUMN_NAME, "2")
        )),
        or(ImmutableList.of(
            selector("newcol", "4"),
            selector(INPUT_COLUMN_NAME, "8"),
            selector(OUTPUT_COLUMN_NAME, "6")
        ))
    ));
    testComputeBaseAndPostUnnestFilters(
        testQueryFilter,
        "(multi-string1 = 3 && (newcol = 2 || multi-string1 = 2) && (newcol = 4 || multi-string1 = 8 || multi-string1 = 6))",
        "(unnested-multi-string1 = 3 && (newcol = 2 || multi-string1 = 2) && (newcol = 4 || multi-string1 = 8 || unnested-multi-string1 = 6))"
    );
  }

  @Test
  public void test_nested_filters_unnested_and_topLevelAND2sdf()
  {
    final Filter testQueryFilter = and(ImmutableList.of(
        not(selector(OUTPUT_COLUMN_NAME, "3")),
        selector(INPUT_COLUMN_NAME, "2")
    ));
    testComputeBaseAndPostUnnestFilters(
        testQueryFilter,
        "multi-string1 = 2",
        "(~(unnested-multi-string1 = 3) && multi-string1 = 2)"
    );
  }

  @Test
  public void test_nested_filters_unnested_and_topLevelOR2sdf()
  {
    final Filter testQueryFilter = or(ImmutableList.of(
        not(selector(OUTPUT_COLUMN_NAME, "3")),
        selector(INPUT_COLUMN_NAME, "2")
    ));
    testComputeBaseAndPostUnnestFilters(
        testQueryFilter,
        "(multi-string1 = 2)",
        "(~(unnested-multi-string1 = 3) || multi-string1 = 2)"
    );
  }

  @Test
  public void test_not_pushdown_not_filter()
  {
    final Filter testQueryFilter = and(ImmutableList.of(
        not(selector(OUTPUT_COLUMN_NAME, "3")),
        or(ImmutableList.of(
            or(ImmutableList.of(
                selector("newcol", "2"),
                selector(INPUT_COLUMN_NAME, "2"),
                and(ImmutableList.of(
                    selector("newcol", "3"),
                    selector(INPUT_COLUMN_NAME, "7")
                ))
            )),
            selector(OUTPUT_COLUMN_NAME, "1")
        ))
    ));
    testComputeBaseAndPostUnnestFilters(
        testQueryFilter,
        "(newcol = 2 || multi-string1 = 2 || (newcol = 3 && multi-string1 = 7) || multi-string1 = 1)",
        "(~(unnested-multi-string1 = 3) && (newcol = 2 || multi-string1 = 2 || (newcol = 3 && multi-string1 = 7) || unnested-multi-string1 = 1))"
    );
  }

  @Test
  public void testPartialArrayPushdown()
  {
    final Filter testQueryFilter = and(
        ImmutableList.of(
            new EqualityFilter("u", ColumnType.LONG, 1L, null),
            new EqualityFilter("str", ColumnType.STRING, "a", null),
            new EqualityFilter("long", ColumnType.LONG, 1L, null)
        )
    );
    testComputeBaseAndPostUnnestFilters(
        UNNEST_ARRAYS,
        testQueryFilter,
        "(str = a && long = 1 (LONG))",
        "(u = 1 (LONG) && str = a && long = 1 (LONG))"
    );
  }

  @Test
  public void testPartialArrayPushdownNested()
  {
    final Filter testQueryFilter = and(
        ImmutableList.of(
            and(
                ImmutableList.of(
                    new EqualityFilter("u", ColumnType.LONG, 1L, null),
                    new EqualityFilter("str", ColumnType.STRING, "a", null)
                )
            ),
            new EqualityFilter("long", ColumnType.LONG, 1L, null)
        )
    );
    // this seems wrong since we should be able to push down str = a and long = 1
    testComputeBaseAndPostUnnestFilters(
        UNNEST_ARRAYS,
        testQueryFilter,
        "(str = a && long = 1 (LONG))",
        "(u = 1 (LONG) && str = a && long = 1 (LONG))"
    );
  }

  @Test
  public void testPartialArrayPushdown2()
  {
    final Filter testQueryFilter = and(
        ImmutableList.of(
            or(
                ImmutableList.of(
                    new EqualityFilter("u", ColumnType.LONG, 1L, null),
                    new EqualityFilter("str", ColumnType.STRING, "a", null)
                )
            ),
            new EqualityFilter("long", ColumnType.LONG, 1L, null)
        )
    );
    testComputeBaseAndPostUnnestFilters(
        UNNEST_ARRAYS,
        testQueryFilter,
        "long = 1 (LONG)",
        "((u = 1 (LONG) || str = a) && long = 1 (LONG))"
    );
  }

  @Test
  public void testArrayCannotPushdown2()
  {
    final Filter testQueryFilter = or(
        ImmutableList.of(
            or(
                ImmutableList.of(
                    new EqualityFilter("u", ColumnType.LONG, 1L, null),
                    new EqualityFilter("str", ColumnType.STRING, "a", null)
                )
            ),
            new EqualityFilter("long", ColumnType.LONG, 1L, null)
        )
    );
    testComputeBaseAndPostUnnestFilters(
        UNNEST_ARRAYS,
        testQueryFilter,
        "",
        "(u = 1 (LONG) || str = a || long = 1 (LONG))"
    );
  }

  @Test
  public void test_pushdown_filters_unnested_dimension_with_unnest_adapters()
  {
    final UnnestStorageAdapter unnestStorageAdapter = new UnnestStorageAdapter(
        new TestStorageAdapter(INCREMENTAL_INDEX),
        new ExpressionVirtualColumn(OUTPUT_COLUMN_NAME, "\"" + INPUT_COLUMN_NAME + "\"", null, ExprMacroTable.nil()),
        new SelectorDimFilter(OUTPUT_COLUMN_NAME, "1", null)
    );

    final VirtualColumn vc = unnestStorageAdapter.getUnnestColumn();

    final String inputColumn = unnestStorageAdapter.getUnnestInputIfDirectAccess(vc);

    final Filter expectedPushDownFilter =
        selector(inputColumn, "1");

    try (final CursorHolder cursorHolder = unnestStorageAdapter.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      Cursor cursor = cursorHolder.asCursor();
      final TestStorageAdapter base = (TestStorageAdapter) unnestStorageAdapter.getBaseAdapter();
      final Filter pushDownFilter = base.getPushDownFilter();

      Assert.assertEquals(expectedPushDownFilter, pushDownFilter);
      Assert.assertEquals(cursor.getClass(), PostJoinCursor.class);
      final Filter postFilter = ((PostJoinCursor) cursor).getPostJoinFilter();
      Assert.assertEquals(unnestStorageAdapter.getUnnestFilter(), postFilter);

      int count = 0;
      while (!cursor.isDone()) {
        cursor.advance();
        count++;
      }
      Assert.assertEquals(1, count);
    }
  }


  @Test
  public void test_pushdown_filters_unnested_dimension_outside()
  {
    final UnnestStorageAdapter unnestStorageAdapter = new UnnestStorageAdapter(
        new TestStorageAdapter(INCREMENTAL_INDEX),
        new ExpressionVirtualColumn(OUTPUT_COLUMN_NAME, "\"" + INPUT_COLUMN_NAME + "\"", null, ExprMacroTable.nil()),
        null
    );

    final VirtualColumn vc = unnestStorageAdapter.getUnnestColumn();

    final String inputColumn = unnestStorageAdapter.getUnnestInputIfDirectAccess(vc);

    final Filter expectedPushDownFilter =
        selector(inputColumn, "1");

    final Filter queryFilter = new SelectorFilter(OUTPUT_COLUMN_NAME, "1", null);
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setFilter(queryFilter)
                                                     .build();

    try (final CursorHolder cursorHolder = unnestStorageAdapter.makeCursorHolder(buildSpec)) {
      Cursor cursor = cursorHolder.asCursor();
      final TestStorageAdapter base = (TestStorageAdapter) unnestStorageAdapter.getBaseAdapter();
      final Filter pushDownFilter = base.getPushDownFilter();

      Assert.assertEquals(expectedPushDownFilter, pushDownFilter);
      Assert.assertEquals(cursor.getClass(), PostJoinCursor.class);
      final Filter postFilter = ((PostJoinCursor) cursor).getPostJoinFilter();
      Assert.assertEquals(queryFilter, postFilter);

      int count = 0;
      while (!cursor.isDone()) {
        cursor.advance();
        count++;
      }
      Assert.assertEquals(1, count);
    }
  }

  @Test
  public void testUnnestValueMatcherValueDoesntExist()
  {
    final String inputColumn = "multi-string5";
    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("expression-testbench");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();
    final SegmentGenerator segmentGenerator = CLOSER.register(new SegmentGenerator());

    IncrementalIndex index = CLOSER.register(
        segmentGenerator.generateIncrementalIndex(dataSegment, schemaInfo, Granularities.HOUR, 100)
    );
    IncrementalIndexStorageAdapter adapter = new IncrementalIndexStorageAdapter(index);
    UnnestStorageAdapter withNullsStorageAdapter = new UnnestStorageAdapter(
        adapter,
        new ExpressionVirtualColumn(OUTPUT_COLUMN_NAME, "\"" + inputColumn + "\"", null, ExprMacroTable.nil()),
        null
    );

    try (final CursorHolder cursorHolder = withNullsStorageAdapter.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      Cursor cursor = cursorHolder.asCursor();
      ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();

      DimensionSelector dimSelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(OUTPUT_COLUMN_NAME));
      // wont match anything
      ValueMatcher matcher = dimSelector.makeValueMatcher("x");
      int count = 0;
      while (!cursor.isDone()) {
        Object dimSelectorVal = dimSelector.getObject();
        if (dimSelectorVal == null) {
          Assert.assertNull(dimSelectorVal);
          Assert.assertTrue(matcher.matches(true));
        }
        Assert.assertFalse(matcher.matches(false));
        cursor.advance();
        count++;
      }
      Assert.assertEquals(count, 618);
    }
  }

  public void testComputeBaseAndPostUnnestFilters(
      Filter testQueryFilter,
      String expectedBasePushDown,
      String expectedPostUnnest
  )
  {
    testComputeBaseAndPostUnnestFilters(
        UNNEST_STORAGE_ADAPTER,
        testQueryFilter,
        expectedBasePushDown,
        expectedPostUnnest
    );
  }

  public void testComputeBaseAndPostUnnestFilters(
      UnnestStorageAdapter adapter,
      Filter testQueryFilter,
      String expectedBasePushDown,
      String expectedPostUnnest
  )
  {
    final String inputColumn = adapter.getUnnestInputIfDirectAccess(adapter.getUnnestColumn());
    final VirtualColumn vc = adapter.getUnnestColumn();
    Pair<Filter, Filter> filterPair = adapter.computeBaseAndPostUnnestFilters(
        testQueryFilter,
        null,
        VirtualColumns.EMPTY,
        inputColumn,
        vc.capabilities(adapter, inputColumn)
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
    );
  }

  private static void assertColumnReadsIdentifier(final VirtualColumn column, final String identifier)
  {
    MatcherAssert.assertThat(column, CoreMatchers.instanceOf(ExpressionVirtualColumn.class));
    Assert.assertEquals("\"" + identifier + "\"", ((ExpressionVirtualColumn) column).getExpression());
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
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    this.pushDownFilter = spec.getFilter();
    return super.makeCursorHolder(spec);
  }
}

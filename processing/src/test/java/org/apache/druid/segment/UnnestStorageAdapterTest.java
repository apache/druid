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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
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
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class UnnestStorageAdapterTest extends InitializedNullHandlingTest
{
  private static Closer CLOSER;
  private static IncrementalIndex INCREMENTAL_INDEX;
  private static IncrementalIndexStorageAdapter INCREMENTAL_INDEX_STORAGE_ADAPTER;
  private static UnnestStorageAdapter UNNEST_STORAGE_ADAPTER;
  private static UnnestStorageAdapter UNNEST_STORAGE_ADAPTER1;
  private static UnnestStorageAdapter UNNEST_STORAGE_ADAPTER2;
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

    UNNEST_STORAGE_ADAPTER2 = new UnnestStorageAdapter(
        INCREMENTAL_INDEX_STORAGE_ADAPTER,
        new ExpressionVirtualColumn(OUTPUT_COLUMN_NAME, "\"" + COLUMNNAME + "\"", null, ExprMacroTable.nil()),
        new SelectorDimFilter(OUTPUT_COLUMN_NAME, "1", null)
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
  public void test_unnest_adapters_with_no_base_filter_active_unnest_filter()
  {

    Sequence<Cursor> cursorSequence = UNNEST_STORAGE_ADAPTER2.makeCursors(
        null,
        UNNEST_STORAGE_ADAPTER2.getInterval(),
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
      Assert.assertEquals(1, count);
      Filter unnestFilter = new SelectorDimFilter(OUTPUT_COLUMN_NAME, "1", null).toFilter();
      VirtualColumn vc = new ExpressionVirtualColumn(
          OUTPUT_COLUMN_NAME,
          "\"" + COLUMNNAME + "\"",
          null,
          ExprMacroTable.nil()
      );
      final String inputColumn = UNNEST_STORAGE_ADAPTER2.getUnnestInputIfDirectAccess(vc);
      Pair<Filter, Filter> filterPair = UNNEST_STORAGE_ADAPTER2.computeBaseAndPostUnnestFilters(
          null,
          unnestFilter,
          VirtualColumns.EMPTY,
          inputColumn,
          INCREMENTAL_INDEX_STORAGE_ADAPTER.getColumnCapabilities(inputColumn)
      );
      SelectorFilter left = ((SelectorFilter) filterPair.lhs);
      SelectorFilter right = ((SelectorFilter) filterPair.rhs);
      Assert.assertEquals(inputColumn, left.getDimension());
      Assert.assertEquals(OUTPUT_COLUMN_NAME, right.getDimension());
      Assert.assertEquals(right.getValue(), left.getValue());
      return null;
    });
  }

  @Test
  public void test_unnest_adapters_with_base_or_filter_no_unnest_filter()
  {
    VirtualColumn vc = new ExpressionVirtualColumn(
        OUTPUT_COLUMN_NAME,
        "\"" + COLUMNNAME + "\"",
        null,
        ExprMacroTable.nil()
    );
    final String inputColumn = UNNEST_STORAGE_ADAPTER.getUnnestInputIfDirectAccess(vc);
    final OrFilter baseFilter = new OrFilter(ImmutableList.of(
        new SelectorDimFilter(OUTPUT_COLUMN_NAME, "1", null).toFilter(),
        new SelectorDimFilter(inputColumn, "2", null).toFilter()
    ));
    Sequence<Cursor> cursorSequence = UNNEST_STORAGE_ADAPTER.makeCursors(
        baseFilter,
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

      Pair<Filter, Filter> filterPair = UNNEST_STORAGE_ADAPTER2.computeBaseAndPostUnnestFilters(
          baseFilter,
          null,
          VirtualColumns.EMPTY,
          inputColumn,
          INCREMENTAL_INDEX_STORAGE_ADAPTER.getColumnCapabilities(inputColumn)
      );
      OrFilter left = ((OrFilter) filterPair.lhs);
      OrFilter right = ((OrFilter) filterPair.rhs);
      Assert.assertEquals("(multi-string1 = 1 || multi-string1 = 2)", left.toString());
      Assert.assertEquals("(unnested-multi-string1 = 1 || multi-string1 = 2)", right.toString());
      return null;
    });
  }
}

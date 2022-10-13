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

package org.apache.druid.segment.virtual;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseSingleValueDimensionSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestObjectColumnSelector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.utils.CloseableUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExpressionSelectorsTest extends InitializedNullHandlingTest
{
  private static Closer CLOSER;
  private static QueryableIndex QUERYABLE_INDEX;
  private static QueryableIndexStorageAdapter QUERYABLE_INDEX_STORAGE_ADAPTER;
  private static IncrementalIndex INCREMENTAL_INDEX;
  private static IncrementalIndexStorageAdapter INCREMENTAL_INDEX_STORAGE_ADAPTER;
  private static List<StorageAdapter> ADAPTERS;

  private static final ColumnCapabilities SINGLE_VALUE = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                                                     .setDictionaryEncoded(true)
                                                                                     .setDictionaryValuesUnique(true)
                                                                                     .setDictionaryValuesSorted(true)
                                                                                     .setHasMultipleValues(false)
                                                                                     .setHasNulls(true);
  private static final ColumnCapabilities MULTI_VAL = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                                                  .setDictionaryEncoded(true)
                                                                                  .setDictionaryValuesUnique(true)
                                                                                  .setDictionaryValuesSorted(true)
                                                                                  .setHasMultipleValues(true)
                                                                                  .setHasNulls(true);

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

    final int numRows = 10_000;
    INCREMENTAL_INDEX = CLOSER.register(
        segmentGenerator.generateIncrementalIndex(dataSegment, schemaInfo, Granularities.HOUR, numRows)
    );
    INCREMENTAL_INDEX_STORAGE_ADAPTER = new IncrementalIndexStorageAdapter(INCREMENTAL_INDEX);

    QUERYABLE_INDEX = CLOSER.register(
        segmentGenerator.generate(dataSegment, schemaInfo, Granularities.HOUR, numRows)
    );
    QUERYABLE_INDEX_STORAGE_ADAPTER = new QueryableIndexStorageAdapter(QUERYABLE_INDEX);

    ADAPTERS = ImmutableList.of(INCREMENTAL_INDEX_STORAGE_ADAPTER, QUERYABLE_INDEX_STORAGE_ADAPTER);
  }

  @AfterClass
  public static void teardown()
  {
    CloseableUtils.closeAndSuppressExceptions(CLOSER, throwable -> {
    });
  }

  @Test
  public void test_single_value_string_bindings()
  {
    final String columnName = "string3";
    for (StorageAdapter adapter : ADAPTERS) {
      Sequence<Cursor> cursorSequence = adapter.makeCursors(
          null,
          adapter.getInterval(),
          VirtualColumns.EMPTY,
          Granularities.ALL,
          false,
          null
      );


      cursorSequence.accumulate(null, (accumulated, cursor) -> {
        ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();
        ExpressionPlan plan = ExpressionPlanner.plan(
            adapter,
            Parser.parse("\"string3\"", TestExprMacroTable.INSTANCE)
        );
        ExpressionPlan plan2 = ExpressionPlanner.plan(
            adapter,
            Parser.parse(
                "concat(\"string3\", 'foo')",
                TestExprMacroTable.INSTANCE
            )
        );

        Expr.ObjectBinding bindings = ExpressionSelectors.createBindings(factory, plan);
        Expr.ObjectBinding bindings2 = ExpressionSelectors.createBindings(factory, plan2);

        DimensionSelector dimSelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(columnName));
        ColumnValueSelector valueSelector = factory.makeColumnValueSelector(columnName);

        // realtime index needs to handle as multi-value in case any new values are added during processing
        final boolean isMultiVal = factory.getColumnCapabilities(columnName) == null ||
                                   factory.getColumnCapabilities(columnName).hasMultipleValues().isMaybeTrue();
        while (!cursor.isDone()) {
          Object dimSelectorVal = dimSelector.getObject();
          Object valueSelectorVal = valueSelector.getObject();
          Object bindingVal = bindings.get(columnName);
          Object bindingVal2 = bindings2.get(columnName);
          if (dimSelectorVal == null) {
            Assert.assertNull(dimSelectorVal);
            Assert.assertNull(valueSelectorVal);
            Assert.assertNull(bindingVal);
            if (isMultiVal) {
              Assert.assertNull(((Object[]) bindingVal2)[0]);
            } else {
              Assert.assertNull(bindingVal2);
            }

          } else {
            if (isMultiVal) {
              Assert.assertEquals(dimSelectorVal, ((Object[]) bindingVal)[0]);
              Assert.assertEquals(valueSelectorVal, ((Object[]) bindingVal)[0]);
              Assert.assertEquals(dimSelectorVal, ((Object[]) bindingVal2)[0]);
              Assert.assertEquals(valueSelectorVal, ((Object[]) bindingVal2)[0]);
            } else {
              Assert.assertEquals(dimSelectorVal, bindingVal);
              Assert.assertEquals(valueSelectorVal, bindingVal);
              Assert.assertEquals(dimSelectorVal, bindingVal2);
              Assert.assertEquals(valueSelectorVal, bindingVal2);
            }
          }

          cursor.advance();
        }

        return null;
      });
    }
  }

  @Test
  public void test_multi_value_string_bindings()
  {
    final String columnName = "multi-string3";
    for (StorageAdapter adapter : ADAPTERS) {
      Sequence<Cursor> cursorSequence = adapter.makeCursors(
          null,
          adapter.getInterval(),
          VirtualColumns.EMPTY,
          Granularities.ALL,
          false,
          null
      );

      cursorSequence.accumulate(null, (ignored, cursor) -> {
        ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();

        // identifier, uses dimension selector supplier supplier, no null coercion
        ExpressionPlan plan = ExpressionPlanner.plan(
            adapter,
            Parser.parse("\"multi-string3\"", TestExprMacroTable.INSTANCE)
        );
        // array output, uses object selector supplier, no null coercion
        ExpressionPlan plan2 = ExpressionPlanner.plan(
            adapter,
            Parser.parse(
                "array_append(\"multi-string3\", 'foo')",
                TestExprMacroTable.INSTANCE
            )
        );
        // array input, uses dimension selector supplier, no null coercion
        ExpressionPlan plan3 = ExpressionPlanner.plan(
            adapter,
            Parser.parse(
                "array_length(\"multi-string3\")",
                TestExprMacroTable.INSTANCE
            )
        );
        // used as scalar, has null coercion
        ExpressionPlan plan4 = ExpressionPlanner.plan(
            adapter,
            Parser.parse(
                "concat(\"multi-string3\", 'foo')",
                TestExprMacroTable.INSTANCE
            )
        );
        Expr.ObjectBinding bindings = ExpressionSelectors.createBindings(factory, plan);
        Expr.ObjectBinding bindings2 = ExpressionSelectors.createBindings(factory, plan2);
        Expr.ObjectBinding bindings3 = ExpressionSelectors.createBindings(factory, plan3);
        Expr.ObjectBinding bindings4 = ExpressionSelectors.createBindings(factory, plan4);

        DimensionSelector dimSelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(columnName));
        ColumnValueSelector valueSelector = factory.makeColumnValueSelector(columnName);

        while (!cursor.isDone()) {
          Object dimSelectorVal = dimSelector.getObject();
          Object valueSelectorVal = valueSelector.getObject();
          Object bindingVal = bindings.get(columnName);
          Object bindingVal2 = bindings2.get(columnName);
          Object bindingVal3 = bindings3.get(columnName);
          Object bindingVal4 = bindings4.get(columnName);

          if (dimSelectorVal == null) {
            Assert.assertNull(dimSelectorVal);
            Assert.assertNull(valueSelectorVal);
            Assert.assertNull(bindingVal);
            Assert.assertNull(bindingVal2);
            Assert.assertNull(bindingVal3);
            // binding4 has null coercion
            Assert.assertArrayEquals(new Object[]{null}, (Object[]) bindingVal4);
          } else {
            Assert.assertArrayEquals(((List) dimSelectorVal).toArray(), (Object[]) bindingVal);
            Assert.assertArrayEquals(((List) valueSelectorVal).toArray(), (Object[]) bindingVal);
            Assert.assertArrayEquals(((List) dimSelectorVal).toArray(), (Object[]) bindingVal2);
            Assert.assertArrayEquals(((List) valueSelectorVal).toArray(), (Object[]) bindingVal2);
            Assert.assertArrayEquals(((List) dimSelectorVal).toArray(), (Object[]) bindingVal3);
            Assert.assertArrayEquals(((List) valueSelectorVal).toArray(), (Object[]) bindingVal3);
          }

          cursor.advance();
        }
        return ignored;
      });
    }
  }

  @Test
  public void test_long_bindings()
  {
    final String columnName = "long3";
    for (StorageAdapter adapter : ADAPTERS) {
      Sequence<Cursor> cursorSequence = adapter.makeCursors(
          null,
          adapter.getInterval(),
          VirtualColumns.EMPTY,
          Granularities.ALL,
          false,
          null
      );

      cursorSequence.accumulate(null, (accumulated, cursor) -> {
        ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();
        // an assortment of plans
        ExpressionPlan plan = ExpressionPlanner.plan(
            adapter,
            Parser.parse("\"long3\"", TestExprMacroTable.INSTANCE)
        );
        ExpressionPlan plan2 = ExpressionPlanner.plan(
            adapter,
            Parser.parse(
                "\"long3\" + 3",
                TestExprMacroTable.INSTANCE
            )
        );

        Expr.ObjectBinding bindings = ExpressionSelectors.createBindings(factory, plan);
        Expr.ObjectBinding bindings2 = ExpressionSelectors.createBindings(factory, plan2);

        ColumnValueSelector valueSelector = factory.makeColumnValueSelector(columnName);

        while (!cursor.isDone()) {
          Object bindingVal = bindings.get(columnName);
          Object bindingVal2 = bindings2.get(columnName);
          if (valueSelector.isNull()) {
            Assert.assertNull(valueSelector.getObject());
            Assert.assertNull(bindingVal);
            Assert.assertNull(bindingVal2);
          } else {
            Assert.assertEquals(valueSelector.getObject(), bindingVal);
            Assert.assertEquals(valueSelector.getLong(), bindingVal);
            Assert.assertEquals(valueSelector.getObject(), bindingVal2);
            Assert.assertEquals(valueSelector.getLong(), bindingVal2);
          }
          cursor.advance();
        }

        return null;
      });
    }
  }

  @Test
  public void test_double_bindings()
  {
    final String columnName = "double3";
    for (StorageAdapter adapter : ADAPTERS) {
      Sequence<Cursor> cursorSequence = adapter.makeCursors(
          null,
          adapter.getInterval(),
          VirtualColumns.EMPTY,
          Granularities.ALL,
          false,
          null
      );


      cursorSequence.accumulate(null, (accumulated, cursor) -> {
        ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();
        // an assortment of plans
        ExpressionPlan plan = ExpressionPlanner.plan(
            adapter,
            Parser.parse("\"double3\"", TestExprMacroTable.INSTANCE)
        );
        ExpressionPlan plan2 = ExpressionPlanner.plan(
            adapter,
            Parser.parse(
                "\"double3\" + 3.0",
                TestExprMacroTable.INSTANCE
            )
        );

        Expr.ObjectBinding bindings = ExpressionSelectors.createBindings(factory, plan);
        Expr.ObjectBinding bindings2 = ExpressionSelectors.createBindings(factory, plan2);

        ColumnValueSelector valueSelector = factory.makeColumnValueSelector(columnName);

        while (!cursor.isDone()) {
          Object bindingVal = bindings.get(columnName);
          Object bindingVal2 = bindings2.get(columnName);
          if (valueSelector.isNull()) {
            Assert.assertNull(valueSelector.getObject());
            Assert.assertNull(bindingVal);
            Assert.assertNull(bindingVal2);
          } else {
            Assert.assertEquals(valueSelector.getObject(), bindingVal);
            Assert.assertEquals(valueSelector.getDouble(), bindingVal);
            Assert.assertEquals(valueSelector.getObject(), bindingVal2);
            Assert.assertEquals(valueSelector.getDouble(), bindingVal2);
          }
          cursor.advance();
        }

        return null;
      });
    }
  }

  @Test
  public void test_canMapOverDictionary_oneSingleValueInput()
  {
    Assert.assertTrue(
        ExpressionSelectors.canMapOverDictionary(
            Parser.parse("dim1 == 2", ExprMacroTable.nil()).analyzeInputs(),
            SINGLE_VALUE
        )
    );
  }

  @Test
  public void test_canMapOverDictionary_oneSingleValueInputSpecifiedTwice()
  {
    Assert.assertTrue(
        ExpressionSelectors.canMapOverDictionary(
            Parser.parse("concat(dim1, dim1) == 2", ExprMacroTable.nil()).analyzeInputs(),
            SINGLE_VALUE
        )
    );
  }

  @Test
  public void test_canMapOverDictionary_oneMultiValueInput()
  {
    Assert.assertTrue(
        ExpressionSelectors.canMapOverDictionary(
            Parser.parse("dim1 == 2", ExprMacroTable.nil()).analyzeInputs(),
            MULTI_VAL
        )
    );
  }

  @Test
  public void test_canMapOverDictionary_oneUnknownInput()
  {
    Assert.assertFalse(
        ExpressionSelectors.canMapOverDictionary(
            Parser.parse("dim1 == 2", ExprMacroTable.nil()).analyzeInputs(),
            new ColumnCapabilitiesImpl()
        )
    );
  }

  @Test
  public void test_canMapOverDictionary_oneSingleValueInputInArrayContext()
  {
    Assert.assertFalse(
        ExpressionSelectors.canMapOverDictionary(
            Parser.parse("array_contains(dim1, 2)", ExprMacroTable.nil()).analyzeInputs(),
            ColumnCapabilitiesImpl.createDefault().setType(ColumnType.STRING_ARRAY)
        )
    );
  }

  @Test
  public void test_canMapOverDictionary_oneMultiValueInputInArrayContext()
  {
    Assert.assertFalse(
        ExpressionSelectors.canMapOverDictionary(
            Parser.parse("array_contains(dim1, 2)", ExprMacroTable.nil()).analyzeInputs(),
            MULTI_VAL
        )
    );
  }

  @Test
  public void test_canMapOverDictionary_oneUnknownInputInArrayContext()
  {
    Assert.assertFalse(
        ExpressionSelectors.canMapOverDictionary(
            Parser.parse("array_contains(dim1, 2)", ExprMacroTable.nil()).analyzeInputs(),
            new ColumnCapabilitiesImpl()
        )
    );
  }

  @Test
  public void test_canMapOverDictionary()
  {
    Assert.assertTrue(
        ExpressionSelectors.canMapOverDictionary(
            Parser.parse("dim1 == 2", ExprMacroTable.nil()).analyzeInputs(),
            SINGLE_VALUE
        )
    );
  }

  @Test
  public void test_supplierFromDimensionSelector()
  {
    final SettableSupplier<String> settableSupplier = new SettableSupplier<>();
    final Supplier<Object> supplier = ExpressionSelectors.supplierFromDimensionSelector(
        dimensionSelectorFromSupplier(settableSupplier),
        false,
        false
    );

    Assert.assertNotNull(supplier);
    Assert.assertEquals(null, supplier.get());

    settableSupplier.set(null);
    Assert.assertEquals(null, supplier.get());

    settableSupplier.set("1234");
    Assert.assertEquals("1234", supplier.get());
  }

  @Test
  public void test_supplierFromObjectSelector_onObject()
  {
    final SettableSupplier<Object> settableSupplier = new SettableSupplier<>();
    final Supplier<Object> supplier = ExpressionSelectors.supplierFromObjectSelector(
        objectSelectorFromSupplier(settableSupplier, Object.class),
        true
    );

    Assert.assertNotNull(supplier);
    Assert.assertEquals(null, supplier.get());

    settableSupplier.set(1.1f);
    Assert.assertEquals(1.1f, supplier.get());

    settableSupplier.set(1L);
    Assert.assertEquals(1L, supplier.get());

    settableSupplier.set("1234");
    Assert.assertEquals("1234", supplier.get());

    settableSupplier.set("1.234");
    Assert.assertEquals("1.234", supplier.get());
  }

  @Test
  public void test_supplierFromObjectSelector_onNumber()
  {
    final SettableSupplier<Number> settableSupplier = new SettableSupplier<>();
    final Supplier<Object> supplier = ExpressionSelectors.supplierFromObjectSelector(
        objectSelectorFromSupplier(settableSupplier, Number.class),
        true
    );


    Assert.assertNotNull(supplier);
    Assert.assertEquals(null, supplier.get());

    settableSupplier.set(1.1f);
    Assert.assertEquals(1.1f, supplier.get());

    settableSupplier.set(1L);
    Assert.assertEquals(1L, supplier.get());
  }

  @Test
  public void test_supplierFromObjectSelector_onString()
  {
    final SettableSupplier<String> settableSupplier = new SettableSupplier<>();
    final Supplier<Object> supplier = ExpressionSelectors.supplierFromObjectSelector(
        objectSelectorFromSupplier(settableSupplier, String.class),
        true
    );

    Assert.assertNotNull(supplier);
    Assert.assertEquals(null, supplier.get());

    settableSupplier.set("1.1");
    Assert.assertEquals("1.1", supplier.get());

    settableSupplier.set("1");
    Assert.assertEquals("1", supplier.get());
  }

  @Test
  public void test_supplierFromObjectSelector_onList()
  {
    final SettableSupplier<List> settableSupplier = new SettableSupplier<>();
    final Supplier<Object> supplier = ExpressionSelectors.supplierFromObjectSelector(
        objectSelectorFromSupplier(settableSupplier, List.class),
        true
    );

    Assert.assertNotNull(supplier);
    Assert.assertEquals(null, supplier.get());

    settableSupplier.set(ImmutableList.of("1", "2", "3"));
    Assert.assertArrayEquals(new String[]{"1", "2", "3"}, (Object[]) supplier.get());

  }

  @Test
  public void test_coerceEvalToSelectorObject()
  {
    Assert.assertEquals(
        ImmutableList.of(1L, 2L, 3L),
        ExpressionSelectors.coerceEvalToObjectOrList(ExprEval.ofLongArray(new Long[]{1L, 2L, 3L}))
    );

    Assert.assertEquals(
        ImmutableList.of(1.0, 2.0, 3.0),
        ExpressionSelectors.coerceEvalToObjectOrList(ExprEval.ofDoubleArray(new Double[]{1.0, 2.0, 3.0}))
    );

    Assert.assertEquals(
        ImmutableList.of("a", "b", "c"),
        ExpressionSelectors.coerceEvalToObjectOrList(ExprEval.ofStringArray(new String[]{"a", "b", "c"}))
    );

    List<String> withNulls = new ArrayList<>();
    withNulls.add("a");
    withNulls.add(null);
    withNulls.add("c");
    Assert.assertEquals(
        withNulls,
        ExpressionSelectors.coerceEvalToObjectOrList(ExprEval.ofStringArray(new String[]{"a", null, "c"}))
    );

    Assert.assertNull(
        ExpressionSelectors.coerceEvalToObjectOrList(ExprEval.ofLongArray(null))
    );
    Assert.assertEquals(
        1L,
        ExpressionSelectors.coerceEvalToObjectOrList(ExprEval.ofLongArray(new Long[]{1L}))
    );
    Assert.assertNull(
        ExpressionSelectors.coerceEvalToObjectOrList(ExprEval.ofLongArray(new Long[]{null}))
    );
  }

  @Test
  public void test_incrementalIndexStringSelector() throws IndexSizeExceededException
  {
    // This test covers a regression caused by ColumnCapabilites.isDictionaryEncoded not matching the value of
    // DimensionSelector.nameLookupPossibleInAdvance in the indexers of an IncrementalIndex, which resulted in an
    // exception trying to make an optimized string expression selector that was not appropriate to use for the
    // underlying dimension selector.
    // This occurred during schemaless ingestion with spare dimension values and no explicit null rows, so the
    // conditions are replicated by this test. See https://github.com/apache/druid/pull/10248 for details
    IncrementalIndexSchema schema = new IncrementalIndexSchema(
        0,
        new TimestampSpec("time", "millis", DateTimes.nowUtc()),
        Granularities.NONE,
        VirtualColumns.EMPTY,
        DimensionsSpec.EMPTY,
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        true
    );

    IncrementalIndex index = new OnheapIncrementalIndex.Builder().setMaxRowCount(100).setIndexSchema(schema).build();
    index.add(
        new MapBasedInputRow(
            DateTimes.nowUtc().getMillis(),
            ImmutableList.of("x"),
            ImmutableMap.of("x", "foo")
        )
    );
    index.add(
        new MapBasedInputRow(
            DateTimes.nowUtc().plusMillis(1000).getMillis(),
            ImmutableList.of("y"),
            ImmutableMap.of("y", "foo")
        )
    );

    IncrementalIndexStorageAdapter adapter = new IncrementalIndexStorageAdapter(index);

    Sequence<Cursor> cursors = adapter.makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );
    int rowsProcessed = cursors.map(cursor -> {
      DimensionSelector xExprSelector = ExpressionSelectors.makeDimensionSelector(
          cursor.getColumnSelectorFactory(),
          Parser.parse("concat(x, 'foo')", ExprMacroTable.nil()),
          null
      );
      DimensionSelector yExprSelector = ExpressionSelectors.makeDimensionSelector(
          cursor.getColumnSelectorFactory(),
          Parser.parse("concat(y, 'foo')", ExprMacroTable.nil()),
          null
      );
      int rowCount = 0;
      while (!cursor.isDone()) {
        Object x = xExprSelector.getObject();
        Object y = yExprSelector.getObject();
        List<String> expectedFoo = Collections.singletonList("foofoo");
        List<String> expectedNull = NullHandling.replaceWithDefault()
                                    ? Collections.singletonList("foo")
                                    : Collections.singletonList(null);
        if (rowCount == 0) {
          Assert.assertEquals(expectedFoo, x);
          Assert.assertEquals(expectedNull, y);
        } else {
          Assert.assertEquals(expectedNull, x);
          Assert.assertEquals(expectedFoo, y);
        }
        rowCount++;
        cursor.advance();
      }
      return rowCount;
    }).accumulate(0, (in, acc) -> in + acc);

    Assert.assertEquals(2, rowsProcessed);
  }

  private static DimensionSelector dimensionSelectorFromSupplier(
      final Supplier<String> supplier
  )
  {
    return new BaseSingleValueDimensionSelector()
    {
      @Override
      protected String getValue()
      {
        return supplier.get();
      }

      @Override
      public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
      {
        inspector.visit("supplier", supplier);
      }
    };
  }

  private static <T> ColumnValueSelector<T> objectSelectorFromSupplier(
      final Supplier<T> supplier,
      final Class<T> clazz
  )
  {
    return new TestObjectColumnSelector<T>()
    {
      @Override
      public Class<T> classOfObject()
      {
        return clazz;
      }

      @Override
      public T getObject()
      {
        return supplier.get();
      }
    };
  }
}

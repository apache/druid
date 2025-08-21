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
import com.google.common.collect.Lists;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.segment.FrameSegment;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequences;
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
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.TestObjectColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExpressionSelectorsTest extends InitializedNullHandlingTest
{
  private static Closer CLOSER;
  private static QueryableIndex QUERYABLE_INDEX;
  private static QueryableIndexCursorFactory QUERYABLE_INDEX_CURSOR_FACTORY;
  private static IncrementalIndex INCREMENTAL_INDEX;
  private static IncrementalIndexCursorFactory INCREMENTAL_INDEX_CURSOR_FACTORY;
  private static List<CursorFactory> CURSOR_FACTORIES;

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
    INCREMENTAL_INDEX_CURSOR_FACTORY = new IncrementalIndexCursorFactory(INCREMENTAL_INDEX);

    QUERYABLE_INDEX = CLOSER.register(
        segmentGenerator.generate(dataSegment, schemaInfo, Granularities.HOUR, numRows)
    );
    QUERYABLE_INDEX_CURSOR_FACTORY = new QueryableIndexCursorFactory(QUERYABLE_INDEX);

    CURSOR_FACTORIES = ImmutableList.of(
        INCREMENTAL_INDEX_CURSOR_FACTORY,
        QUERYABLE_INDEX_CURSOR_FACTORY
    );
  }

  @AfterClass
  public static void teardown()
  {
    CloseableUtils.closeAndSuppressExceptions(CLOSER, throwable -> {
    });
  }

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();


  @Test
  public void test_single_value_string_bindings()
  {
    final String columnName = "string3";
    for (CursorFactory adapter : CURSOR_FACTORIES) {
      try (final CursorHolder cursorHolder = adapter.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
        Cursor cursor = cursorHolder.asCursor();

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
      }
    }
  }

  @Test
  public void test_multi_value_string_bindings()
  {
    final String columnName = "multi-string3";
    for (CursorFactory adapter : CURSOR_FACTORIES) {
      try (final CursorHolder cursorHolder = adapter.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
        Cursor cursor = cursorHolder.asCursor();
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
      }
    }
  }

  @Test
  public void test_long_bindings()
  {
    final String columnName = "long3";
    for (CursorFactory adapter : CURSOR_FACTORIES) {
      try (final CursorHolder cursorHolder = adapter.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
        Cursor cursor = cursorHolder.asCursor();
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
      }
    }
  }

  @Test
  public void test_double_bindings()
  {
    final String columnName = "double3";
    for (CursorFactory adapter : CURSOR_FACTORIES) {
      try (final CursorHolder cursorHolder = adapter.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
        Cursor cursor = cursorHolder.asCursor();
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
      }
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
  public void test_supplierFromObjectSelector_onArray()
  {
    final SettableSupplier<Object[]> settableSupplier = new SettableSupplier<>();
    final Supplier<Object> supplier = ExpressionSelectors.supplierFromObjectSelector(
        objectSelectorFromSupplier(settableSupplier, Object[].class),
        true
    );

    Assert.assertNotNull(supplier);
    Assert.assertEquals(null, supplier.get());

    settableSupplier.set(new String[]{"1", "2", "3"});
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
  public void test_incrementalIndexStringSelector()
  {
    // This test covers a regression caused by ColumnCapabilites.isDictionaryEncoded not matching the value of
    // DimensionSelector.nameLookupPossibleInAdvance in the indexers of an IncrementalIndex, which resulted in an
    // exception trying to make an optimized string expression selector that was not appropriate to use for the
    // underlying dimension selector.
    // This occurred during schemaless ingestion with sparse dimension values and no explicit null rows, so the
    // conditions are replicated by this test. See https://github.com/apache/druid/pull/10248 for details
    IncrementalIndexSchema schema = IncrementalIndexSchema.builder()
                                                          .withTimestampSpec(new TimestampSpec("time", "millis", DateTimes.nowUtc()))
                                                          .withMetrics(new AggregatorFactory[]{new CountAggregatorFactory("count")})
                                                          .build();

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

    IncrementalIndexCursorFactory cursorFactory = new IncrementalIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      Cursor cursor = cursorHolder.asCursor();
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
        String expectedFoo = "foofoo";
        if (rowCount == 0) {
          Assert.assertEquals(expectedFoo, x);
          Assert.assertNull(y);
        } else {
          Assert.assertNull(x);
          Assert.assertEquals(expectedFoo, y);
        }
        rowCount++;
        cursor.advance();
      }

      Assert.assertEquals(2, rowCount);
    }
  }

  @Test
  public void test_incrementalIndexStringSelectorCast()
  {
    IncrementalIndexSchema schema = IncrementalIndexSchema.builder()
                                                          .withTimestampSpec(new TimestampSpec("time", "millis", DateTimes.nowUtc()))
                                                          .withMetrics(new AggregatorFactory[]{new CountAggregatorFactory("count")})
                                                          .build();

    IncrementalIndex index = new OnheapIncrementalIndex.Builder().setMaxRowCount(100).setIndexSchema(schema).build();
    index.add(
        new MapBasedInputRow(
            DateTimes.nowUtc().getMillis(),
            ImmutableList.of("x"),
            ImmutableMap.of("x", "1.1")
        )
    );

    IncrementalIndexCursorFactory cursorFactory = new IncrementalIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      Cursor cursor = cursorHolder.asCursor();
      ColumnValueSelector<?> xExprSelector = ExpressionSelectors.makeColumnValueSelector(
          cursor.getColumnSelectorFactory(),
          Parser.parse("cast(x, 'DOUBLE')", ExprMacroTable.nil())
      );
      int rowCount = 0;
      while (!cursor.isDone()) {
        Object x = xExprSelector.getObject();
        double expectedFoo = 1.1;
        Assert.assertEquals(expectedFoo, x);
        rowCount++;
        cursor.advance();
      }

      Assert.assertEquals(1, rowCount);
    }
  }

  @Test
  public void testCastSelectors() throws IOException
  {
    final RowSignature rowSignature = RowSignature.builder()
                                                  .add("string", ColumnType.STRING)
                                                  .add("multiString", ColumnType.STRING)
                                                  .add("long", ColumnType.LONG)
                                                  .add("double", ColumnType.DOUBLE)
                                                  .add("stringArray", ColumnType.STRING_ARRAY)
                                                  .add("longArray", ColumnType.LONG_ARRAY)
                                                  .add("doubleArray", ColumnType.DOUBLE_ARRAY)
                                                  .build();
    final DateTime start = DateTimes.nowUtc();
    List<InputRow> rows = List.of(
        new ListBasedInputRow(
            rowSignature,
            start,
            rowSignature.getColumnNames(),
            List.of(
                "a",
                List.of("a1", "a2"),
                1L,
                1.1,
                new Object[]{"a1", "a2"},
                new Object[]{1L, 1L},
                new Object[]{1.1, 1.1}
            )
        ),
        new ListBasedInputRow(
            rowSignature,
            start.plusMinutes(1),
            rowSignature.getColumnNames(),
            List.of(
                "b",
                List.of("b1"),
                2L,
                2.2,
                new Object[]{"2.2"},
                new Object[]{2L},
                new Object[]{2.2}
            )
        ),
        new ListBasedInputRow(
            rowSignature,
            start.plusMinutes(2),
            rowSignature.getColumnNames(),
            Lists.newArrayList(
                null,
                List.of(),
                null,
                null,
                null,
                null,
                null
            )
        ),
        new ListBasedInputRow(
            rowSignature,
            start.plusMinutes(3),
            rowSignature.getColumnNames(),
            List.of(
                "4",
                Lists.newArrayList(null, null, "4.4"),
                4L,
                4.4,
                new Object[0],
                new Object[0],
                new Object[0]
            )
        )
    );

    final IncrementalIndexSchema schema =
        IncrementalIndexSchema.builder()
                              .withDimensionsSpec(
                                  DimensionsSpec.builder()
                                                .setDimensions(
                                                    List.of(
                                                        new StringDimensionSchema("string"),
                                                        new StringDimensionSchema("multiString"),
                                                        new LongDimensionSchema("long"),
                                                        new DoubleDimensionSchema("double")
                                                    )
                                                )
                                                .useSchemaDiscovery(true)
                                                .build()
                              )
                              .build();

    IndexBuilder bob = IndexBuilder.create()
                                   .schema(schema)
                                   .tmpDir(temporaryFolder.newFolder())
                                   .rows(rows);

    try (final Closer closer = Closer.create()) {
      final List<Segment> segments = List.of(
          new IncrementalIndexSegment(bob.buildIncrementalIndex(), SegmentId.dummy("test")),
          new QueryableIndexSegment(bob.buildMMappedIndex(), SegmentId.dummy("test")),
          new RowBasedSegment<>(Sequences.simple(rows), RowAdapters.standardRow(), RowSignature.empty()),
          new RowBasedSegment<>(Sequences.simple(rows), RowAdapters.standardRow(), rowSignature),
          FrameTestUtil.cursorFactoryToFrameSegment(
              new QueryableIndexCursorFactory(bob.buildMMappedIndex()),
              FrameType.latestRowBased()
          ),
          FrameTestUtil.cursorFactoryToFrameSegment(
              new QueryableIndexCursorFactory(bob.buildMMappedIndex()),
              FrameType.latestColumnar()
          )
      );

      for (Segment segment : segments) {
        final CursorFactory cursorFactory = segment.as(CursorFactory.class);
        Assert.assertNotNull(cursorFactory);
        final CursorHolder holder = closer.register(cursorFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN));

        final Cursor cursor = holder.asCursor();
        final Offset offset = new SimpleAscendingOffset(rows.size());

        ColumnValueSelector baseStringSelector = cursor.getColumnSelectorFactory().makeColumnValueSelector("string");
        ColumnValueSelector baseMultiStringSelector = cursor.getColumnSelectorFactory().makeColumnValueSelector("multiString");
        ColumnValueSelector baseLongSelector = cursor.getColumnSelectorFactory().makeColumnValueSelector("long");
        ColumnValueSelector baseDoubleSelector = cursor.getColumnSelectorFactory().makeColumnValueSelector("double");
        ColumnValueSelector baseStringArraySelector = cursor.getColumnSelectorFactory().makeColumnValueSelector("stringArray");
        ColumnValueSelector baseLongArraySelector = cursor.getColumnSelectorFactory().makeColumnValueSelector("longArray");
        ColumnValueSelector baseDoubleArraySelector = cursor.getColumnSelectorFactory().makeColumnValueSelector("doubleArray");

        ColumnValueSelector stringSelectorToLong = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseStringSelector,
            ColumnType.STRING,
            ColumnType.LONG
        );
        ColumnValueSelector stringSelectorToDouble = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseStringSelector,
            ColumnType.STRING,
            ColumnType.DOUBLE
        );
        ColumnValueSelector stringToArraySelector = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseStringSelector,
            ColumnType.STRING,
            ColumnType.STRING_ARRAY
        );

        ColumnValueSelector multiStringToStringSelector = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseMultiStringSelector,
            ColumnType.STRING,
            ColumnType.STRING
        );

        ColumnValueSelector multiStringToStringArraySelector = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseMultiStringSelector,
            ColumnType.STRING,
            ColumnType.STRING_ARRAY
        );

        ColumnValueSelector longToStringSelector = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseLongSelector,
            ColumnType.LONG,
            ColumnType.STRING
        );

        ColumnValueSelector longToDoubleArraySelector = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseLongSelector,
            ColumnType.LONG,
            ColumnType.DOUBLE_ARRAY
        );

        ColumnValueSelector doubleToStringSelector = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseDoubleSelector,
            ColumnType.DOUBLE,
            ColumnType.STRING
        );

        ColumnValueSelector doubleToLongArraySelector = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseDoubleSelector,
            ColumnType.DOUBLE,
            ColumnType.LONG_ARRAY
        );

        ColumnValueSelector stringArrayToStringSelector = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseStringArraySelector,
            ColumnType.STRING_ARRAY,
            ColumnType.STRING
        );

        ColumnValueSelector stringArrayToLongArraySelector = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseStringArraySelector,
            ColumnType.STRING_ARRAY,
            ColumnType.LONG_ARRAY
        );

        ColumnValueSelector stringArrayToDoubleSelector = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseStringArraySelector,
            ColumnType.STRING_ARRAY,
            ColumnType.DOUBLE
        );

        ColumnValueSelector longArrayToStringSelector = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseLongArraySelector,
            ColumnType.LONG_ARRAY,
            ColumnType.STRING
        );

        ColumnValueSelector longArrayToDoubleArraySelector = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseLongArraySelector,
            ColumnType.LONG_ARRAY,
            ColumnType.DOUBLE_ARRAY
        );

        ColumnValueSelector doubleArrayToStringArraySelector = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseDoubleArraySelector,
            ColumnType.DOUBLE_ARRAY,
            ColumnType.STRING_ARRAY
        );

        ColumnValueSelector doubleArrayToLongSelector = ExpressionSelectors.castColumnValueSelector(
            offset::getOffset,
            baseDoubleArraySelector,
            ColumnType.DOUBLE_ARRAY,
            ColumnType.LONG
        );

        // first row
        // "a"
        Assert.assertNull(stringSelectorToLong.getObject());
        Assert.assertNull(stringSelectorToDouble.getObject());
        Assert.assertArrayEquals(new Object[]{"a"}, (Object[]) stringToArraySelector.getObject());

        // ["a1", "a2"]
        Assert.assertNull(multiStringToStringSelector.getObject());
        Assert.assertArrayEquals(new Object[]{"a1", "a2"}, (Object[]) multiStringToStringArraySelector.getObject());

        // 1
        Assert.assertEquals("1", longToStringSelector.getObject());
        Assert.assertArrayEquals(new Object[]{1.0}, (Object[]) longToDoubleArraySelector.getObject());

        // 1.1
        Assert.assertEquals("1.1", doubleToStringSelector.getObject());
        Assert.assertArrayEquals(new Object[]{1L}, (Object[]) doubleToLongArraySelector.getObject());

        // ["a1", "a2"]
        Assert.assertNull(stringArrayToStringSelector.getObject());
        Assert.assertArrayEquals(new Object[]{null, null}, (Object[]) stringArrayToLongArraySelector.getObject());
        Assert.assertNull(stringArrayToDoubleSelector.getObject());

        // [1, 1]
        Assert.assertNull(longArrayToStringSelector.getObject());
        Assert.assertArrayEquals(new Object[]{1.0, 1.0}, (Object[]) longArrayToDoubleArraySelector.getObject());

        // [1.1, 1.1]
        Assert.assertArrayEquals(new Object[]{"1.1", "1.1"}, (Object[]) doubleArrayToStringArraySelector.getObject());
        Assert.assertNull(doubleArrayToLongSelector.getObject());

        cursor.advance();
        offset.increment();

        // first row
        // "b"
        Assert.assertNull(stringSelectorToLong.getObject());
        Assert.assertNull(stringSelectorToDouble.getObject());
        Assert.assertArrayEquals(new Object[]{"b"}, (Object[]) stringToArraySelector.getObject());

        // ["b1"]
        Assert.assertEquals("b1", multiStringToStringSelector.getObject());
        Assert.assertArrayEquals(new Object[]{"b1"}, (Object[]) multiStringToStringArraySelector.getObject());

        // 2
        Assert.assertEquals("2", longToStringSelector.getObject());
        Assert.assertArrayEquals(new Object[]{2.0}, (Object[]) longToDoubleArraySelector.getObject());

        // 2.2
        Assert.assertEquals("2.2", doubleToStringSelector.getObject());
        Assert.assertArrayEquals(new Object[]{2L}, (Object[]) doubleToLongArraySelector.getObject());

        // ["2.2"]
        Assert.assertEquals("2.2", stringArrayToStringSelector.getObject());
        Assert.assertArrayEquals(new Object[]{2L}, (Object[]) stringArrayToLongArraySelector.getObject());
        Assert.assertEquals(2.2, stringArrayToDoubleSelector.getObject());

        // [2]
        Assert.assertEquals("2", longArrayToStringSelector.getObject());
        Assert.assertArrayEquals(new Object[]{2.0}, (Object[]) longArrayToDoubleArraySelector.getObject());

        // [2.2]
        Assert.assertArrayEquals(new Object[]{"2.2"}, (Object[]) doubleArrayToStringArraySelector.getObject());
        Assert.assertEquals(2L, doubleArrayToLongSelector.getObject());

        cursor.advance();
        offset.increment();


        // third row
        // null
        Assert.assertNull(stringSelectorToLong.getObject());
        Assert.assertNull(stringSelectorToDouble.getObject());
        Assert.assertNull(stringToArraySelector.getObject());

        // []
        Assert.assertNull(multiStringToStringSelector.getObject());
        if (segment instanceof IncrementalIndexSegment || segment instanceof QueryableIndexSegment || segment instanceof FrameSegment) {
          Assert.assertNull(multiStringToStringSelector.getObject());
        } else {
          // this one is kind of weird, the row based segment selector does not convert the empty list into null like
          // the others do
          Assert.assertArrayEquals(new Object[0], (Object[]) multiStringToStringArraySelector.getObject());
        }

        // null
        Assert.assertNull(longToStringSelector.getObject());
        Assert.assertNull(longToDoubleArraySelector.getObject());

        // null
        Assert.assertNull(doubleToStringSelector.getObject());
        Assert.assertNull(doubleToLongArraySelector.getObject());

        // null
        Assert.assertNull(stringArrayToStringSelector.getObject());
        Assert.assertNull(stringArrayToLongArraySelector.getObject());
        Assert.assertNull(stringArrayToDoubleSelector.getObject());

        // null
        Assert.assertNull(longArrayToStringSelector.getObject());
        Assert.assertNull(longArrayToDoubleArraySelector.getObject());

        // null
        Assert.assertNull(doubleArrayToStringArraySelector.getObject());
        Assert.assertNull(doubleArrayToLongSelector.getObject());

        cursor.advance();
        offset.increment();

        // fourth row
        // "4"
        Assert.assertEquals(4L, stringSelectorToLong.getObject());
        Assert.assertEquals(4.0, stringSelectorToDouble.getObject());
        Assert.assertArrayEquals(new Object[]{"4"}, (Object[]) stringToArraySelector.getObject());

        // [null, null, 4.4]
        Assert.assertNull(multiStringToStringSelector.getObject());
        Assert.assertArrayEquals(new Object[]{null, null, "4.4"}, (Object[]) multiStringToStringArraySelector.getObject());

        // 4
        Assert.assertEquals("4", longToStringSelector.getObject());
        Assert.assertArrayEquals(new Object[]{4.0}, (Object[]) longToDoubleArraySelector.getObject());

        // 4.4
        Assert.assertEquals("4.4", doubleToStringSelector.getObject());
        Assert.assertArrayEquals(new Object[]{4L}, (Object[]) doubleToLongArraySelector.getObject());

        // []
        Assert.assertNull(stringArrayToStringSelector.getObject());
        Assert.assertArrayEquals(new Object[0], (Object[]) stringArrayToLongArraySelector.getObject());
        Assert.assertNull(stringArrayToDoubleSelector.getObject());

        // []
        Assert.assertNull(longArrayToStringSelector.getObject());
        Assert.assertArrayEquals(new Object[0], (Object[]) longArrayToDoubleArraySelector.getObject());

        // []
        Assert.assertArrayEquals(new Object[0], (Object[]) doubleArrayToStringArraySelector.getObject());
        Assert.assertNull(doubleArrayToLongSelector.getObject());
      }
    }
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
    return new TestObjectColumnSelector<>()
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

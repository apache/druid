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

package org.apache.druid.segment.shim;

import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.SimpleQueryableIndex;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Compares the results between using a {@link ShimCursor} and {@link Cursor}
 */
public class ShimCursorTest
{
  static {
    ExpressionProcessing.initializeForTests();
  }

  @Parameterized.Parameters
  public static Collection<Object> data()
  {
    return List.of(1, 2, 4, 7, 512);
  }

  /**
   * Tests long and double columns.
   */
  @ParameterizedTest(name = "Number columns with vector size {0}")
  @MethodSource("data")
  public void testNumberColumns(int vectorSize)
  {
    IncrementalIndex incrementalIndex = new OnheapIncrementalIndex.Builder()
        .setMaxRowCount(100)
        .setIndexSchema(
            IncrementalIndexSchema.builder()
                                  .withDimensionsSpec(
                                      DimensionsSpec.builder()
                                                    .useSchemaDiscovery(true)
                                                    .setIncludeAllDimensions(true)
                                                    .build()
                                  )
                                  .withRollup(false)
                                  .build()
        )
        .build();

    final List<String> signature = List.of("A", "B", "C");

    incrementalIndex.add(autoRow(signature, 1, 2.0, 3));
    incrementalIndex.add(autoRow(signature, 4, 5, 6));
    incrementalIndex.add(autoRow(signature, null, 423, 13));
    incrementalIndex.add(autoRow(signature, 51, 0, null));
    incrementalIndex.add(autoRow(signature, 51, -1.0, 4));
    incrementalIndex.add(autoRow(signature, 123, 413.132, 2));
    incrementalIndex.add(autoRow(signature, 0, null, 331));
    incrementalIndex.add(autoRow(signature, Long.MAX_VALUE, -824.0f, Long.MIN_VALUE));
    incrementalIndex.add(autoRow(signature, -1, -2.0d, 112));

    final SimpleQueryableIndex index = (SimpleQueryableIndex) TestIndex.persistAndMemoryMap(incrementalIndex);
    final QueryableIndexCursorFactory queryableIndexCursorFactory = new QueryableIndexCursorFactory(index);

    CursorBuildSpec cursorBuildSpec = CursorBuildSpec.builder()
                                                     .setQueryContext(
                                                         QueryContext.of(
                                                             Map.of(QueryContexts.VECTOR_SIZE_KEY, vectorSize)
                                                         )
                                                     )
                                                     .build();
    CursorHolder cursorHolder = queryableIndexCursorFactory.makeCursorHolder(cursorBuildSpec);
    Assertions.assertTrue(cursorHolder.canVectorize());
    VectorCursor vectorCursor = cursorHolder.asVectorCursor();

    Cursor cursor = cursorHolder.asCursor();
    ShimCursor shimCursor = new ShimCursor(vectorCursor);

    compareCursors(List.of("A", "B", "C", "non-existent"), cursor, shimCursor);
  }

  /**
   * Tests a few dictionary encoded columns, with a few number columns thrown in.
   */
  @ParameterizedTest(name = "Number columns with vector size {0}")
  @MethodSource("data")
  public void testDictionaryColumns(int vectorSize)
  {
    IncrementalIndex incrementalIndex = new OnheapIncrementalIndex.Builder()
        .setMaxRowCount(100)
        .setIndexSchema(
            IncrementalIndexSchema.builder()
                                  .withDimensionsSpec(
                                      DimensionsSpec.builder()
                                                    .useSchemaDiscovery(true)
                                                    .setIncludeAllDimensions(true)
                                                    .build()
                                  )
                                  .withRollup(false)
                                  .build()
        )
        .build();

    final List<String> signature = List.of("A", "B", "C", "D", "E", "F");

    incrementalIndex.add(autoRow(signature, 1, 1.0, "Tom", arr("Bat", "Knife"), obj("A", "B"), null));
    incrementalIndex.add(autoRow(signature, 2, 2.0, "Bob", arr("Builder", "Carpenter"), obj("A", "B"), null));
    incrementalIndex.add(autoRow(signature, 3, 4.0, "Jack", arr("A", "B"), obj("Jacky", "Bobby"), null));
    incrementalIndex.add(autoRow(signature, 4, 8.0, "Will", arr("Sing", "Dance"), obj("Sing", "B"), null));
    incrementalIndex.add(autoRow(signature, 5, 16.0, "Smith", arr("Car", "Trunk"), obj(), null));
    incrementalIndex.add(autoRow(signature, 6, -1, "Cat", arr(), obj("Bat", "Knife"), null));
    incrementalIndex.add(autoRow(signature, 7, -2.0, "Drew", arr("Machine", "Rabbit"), obj("bat", "knife"), null));

    final SimpleQueryableIndex index = (SimpleQueryableIndex) TestIndex.persistAndMemoryMap(incrementalIndex);
    final QueryableIndexCursorFactory queryableIndexCursorFactory = new QueryableIndexCursorFactory(index);

    CursorBuildSpec cursorBuildSpec = CursorBuildSpec.builder()
                                                     .setQueryContext(
                                                         QueryContext.of(
                                                             Map.of(QueryContexts.VECTOR_SIZE_KEY, vectorSize)
                                                         )
                                                     )
                                                     .build();
    CursorHolder cursorHolder = queryableIndexCursorFactory.makeCursorHolder(cursorBuildSpec);
    Assertions.assertTrue(cursorHolder.canVectorize());
    VectorCursor vectorCursor = cursorHolder.asVectorCursor();

    Cursor cursor = cursorHolder.asCursor();
    ShimCursor shimCursor = new ShimCursor(vectorCursor);

    compareCursors(signature, cursor, shimCursor);
  }

  @ParameterizedTest(name = "Non dict-encoded string columns with vector size {0}")
  @MethodSource("data")
  public void testMultiDimColumns(int vectorSize)
  {
    IncrementalIndex incrementalIndex = new OnheapIncrementalIndex.Builder()
        .setMaxRowCount(100)
        .setIndexSchema(
            IncrementalIndexSchema.builder()
                                  .withDimensionsSpec(
                                      DimensionsSpec.builder()
                                                    .useSchemaDiscovery(false)
                                                    .setIncludeAllDimensions(true)
                                                    .build()
                                  )
                                  .withRollup(false)
                                  .build()
        )
        .build();

    final List<String> signature = List.of("A", "B");

    incrementalIndex.add(autoRow(signature, 1, arr("Bat", "Knife")));
    incrementalIndex.add(autoRow(signature, 2, arr("Builder", "Carpenter")));
    incrementalIndex.add(autoRow(signature, 3, arr("A", "B")));
    incrementalIndex.add(autoRow(signature, 4, arr("Sing", "Dance")));
    incrementalIndex.add(autoRow(signature, 5, arr("Car", "Trunk")));
    incrementalIndex.add(autoRow(signature, 6, arr()));
    incrementalIndex.add(autoRow(signature, 7, arr("Machine", "Rabbit")));

    final SimpleQueryableIndex index = (SimpleQueryableIndex) TestIndex.persistAndMemoryMap(incrementalIndex);
    final QueryableIndexCursorFactory queryableIndexCursorFactory = new QueryableIndexCursorFactory(index);

    CursorBuildSpec cursorBuildSpec = CursorBuildSpec.builder()
                                                     .setQueryContext(
                                                         QueryContext.of(
                                                             Map.of(QueryContexts.VECTOR_SIZE_KEY, vectorSize)
                                                         )
                                                     )
                                                     .build();
    CursorHolder cursorHolder = queryableIndexCursorFactory.makeCursorHolder(cursorBuildSpec);
    Assertions.assertTrue(cursorHolder.canVectorize());
    VectorCursor vectorCursor = cursorHolder.asVectorCursor();

    Cursor cursor = cursorHolder.asCursor();
    ShimCursor shimCursor = new ShimCursor(vectorCursor);

    compareCursors(signature, cursor, shimCursor);
  }

  @ParameterizedTest(name = "Non dict-encoded string columns with vector size {0}")
  @MethodSource("data")
  public void testNonDictStringColumns(int vectorSize)
  {
    IncrementalIndex incrementalIndex = new OnheapIncrementalIndex.Builder()
        .setMaxRowCount(100)
        .setIndexSchema(
            IncrementalIndexSchema.builder()
                                  .withDimensionsSpec(
                                      DimensionsSpec.builder()
                                                    .useSchemaDiscovery(false)
                                                    .setIncludeAllDimensions(true)
                                                    .build()
                                  )
                                  .withVirtualColumns(
                                      VirtualColumns.create(
                                          new ExpressionVirtualColumn(
                                              "v1",
                                              "concat(\"A\", \"B\")",
                                              ColumnType.STRING,
                                              TestExprMacroTable.INSTANCE)
                                      )
                                  )
                                  .withRollup(false)
                                  .build()
        )
        .build();

    final List<String> signature = List.of("A", "B");

    incrementalIndex.add(autoRow(signature, 1, "Tom"));
    incrementalIndex.add(autoRow(signature, 2, "Bob"));
    incrementalIndex.add(autoRow(signature, 3, "Jack"));
    incrementalIndex.add(autoRow(signature, 4, "Will"));
    incrementalIndex.add(autoRow(signature, 5, "Smith"));
    incrementalIndex.add(autoRow(signature, 6, "Cat"));
    incrementalIndex.add(autoRow(signature, 7, "Drew"));

    final SimpleQueryableIndex index = (SimpleQueryableIndex) TestIndex.persistAndMemoryMap(incrementalIndex);
    final QueryableIndexCursorFactory queryableIndexCursorFactory = new QueryableIndexCursorFactory(index);

    CursorBuildSpec cursorBuildSpec = CursorBuildSpec.builder()
                                                     .setQueryContext(
                                                         QueryContext.of(
                                                             Map.of(QueryContexts.VECTOR_SIZE_KEY, vectorSize)
                                                         )
                                                     )
                                                     .setVirtualColumns(VirtualColumns.create(
                                                                            new ExpressionVirtualColumn(
                                                                                "v1",
                                                                                "concat(\"A\", \"B\")",
                                                                                ColumnType.STRING,
                                                                                TestExprMacroTable.INSTANCE)
                                                                        )
                                                     )
                                                     .build();
    CursorHolder cursorHolder = queryableIndexCursorFactory.makeCursorHolder(cursorBuildSpec);
    Assertions.assertTrue(cursorHolder.canVectorize());
    VectorCursor vectorCursor = cursorHolder.asVectorCursor();

    Cursor cursor = cursorHolder.asCursor();
    ShimCursor shimCursor = new ShimCursor(vectorCursor);

    compareCursors(List.of("A", "B", "v1"), cursor, shimCursor);
  }

  private static List<Object> arr(Object... vals)
  {
    return Arrays.asList(vals);
  }

  private static Map<String, Object> obj(Object... vals)
  {
    if (vals.length % 2 != 0) {
      throw new ISE("vals.length[%d] needs to be a multiple of 2", vals.length);
    }

    Map<String, Object> retVal = new LinkedHashMap<>();
    for (int i = 0; i < vals.length; i += 2) {
      retVal.put((String) vals[i], vals[i + 1]);
    }
    return retVal;
  }

  /**
   * Compares an expected {@link Cursor} to a {@link ShimCursor} and asserts that they perform in a similar way.
   */
  private static void compareCursors(List<String> signature, Cursor expected, ShimCursor actual)
  {
    final ColumnSelectorFactory expectedFactory = expected.getColumnSelectorFactory();
    final ColumnSelectorFactory actualFactory = actual.getColumnSelectorFactory();
    while (!expected.isDone()) {
      Assertions.assertFalse(actual.isDone());
      for (String columnName : signature) {
        compareColumnValueSelector(columnName, expectedFactory, actualFactory);
        compareDimSelectorIfSupported(columnName, expectedFactory, actualFactory);
      }

      expected.advance();
      actual.advance();
    }

    Assertions.assertTrue(actual.isDone());
  }

  private static void compareColumnValueSelector(
      String columnName,
      ColumnSelectorFactory expectedFactory,
      ColumnSelectorFactory actualFactory
  )
  {
    final ColumnCapabilities expectedCapabilities = expectedFactory.getColumnCapabilities(columnName);
    final ColumnCapabilities actualCapabilities = actualFactory.getColumnCapabilities(columnName);

    final ColumnValueSelector<?> expectedSelector = expectedFactory.makeColumnValueSelector(columnName);
    final ColumnValueSelector<?> actualSelector = actualFactory.makeColumnValueSelector(columnName);

    if (expectedCapabilities == null) {
      Assertions.assertNull(actualCapabilities);
      Assertions.assertTrue(actualSelector.isNull());
      return;
    }

    if (expectedCapabilities.isNumeric()) {
      Assertions.assertTrue(actualCapabilities.isNumeric());
      Assertions.assertEquals(expectedSelector.getDouble(), actualSelector.getDouble());
      Assertions.assertEquals(expectedSelector.getLong(), actualSelector.getLong());
      Assertions.assertEquals(expectedSelector.getFloat(), actualSelector.getFloat());
    } else if (expectedCapabilities.isArray()) {
      Assertions.assertTrue(actualCapabilities.isArray());
      Assertions.assertArrayEquals((Object[]) expectedSelector.getObject(), (Object[]) actualSelector.getObject());
    } else {
      Assertions.assertEquals(expectedSelector.getObject(), actualSelector.getObject());
    }
  }

  private static void compareDimSelectorIfSupported(
      String columnName,
      ColumnSelectorFactory expectedFactory,
      ColumnSelectorFactory actualFactory
  )
  {
    final ColumnCapabilities expectedCapabilities = expectedFactory.getColumnCapabilities(columnName);

    if (expectedCapabilities != null && expectedCapabilities.toColumnType().equals(ColumnType.STRING)) {
      final DimensionSelector expectedDimSelector = expectedFactory.makeDimensionSelector(DefaultDimensionSpec.of(
          columnName));
      final DimensionSelector actualDimSelector = actualFactory.makeDimensionSelector(DefaultDimensionSpec.of(
          columnName));

      Assertions.assertEquals(expectedDimSelector.getObject(), actualDimSelector.getObject());
      if (expectedDimSelector.idLookup() != null) {
        // TODO: is this correct?
        IndexedInts expectedInts = expectedDimSelector.getRow();
        IndexedInts actualInts = actualDimSelector.getRow();
        Assertions.assertEquals(expectedInts.size(), actualInts.size());
        for (int i = 0; i < expectedInts.size(); i++) {
          Assertions.assertEquals(expectedInts.get(i), actualInts.get(i));
        }
      }
    }
  }

  private static MapBasedInputRow autoRow(List<String> signature, Object... values)
  {
    if (signature.size() != values.length) {
      throw new RuntimeException("Signature and values do not match");
    }
    Map<String, Object> row = new HashMap<>();

    for (int i = 0; i < signature.size(); i++) {
      row.put(signature.get(i), values[i]);
    }

    return new MapBasedInputRow(0, signature, row);
  }
}

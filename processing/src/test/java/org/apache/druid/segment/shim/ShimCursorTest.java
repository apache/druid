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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.AutoTypeColumnSchema;
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
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compares the results between using a {@link ShimCursor} and {@link Cursor}
 */
public class ShimCursorTest
{
  static {
    ExpressionProcessing.initializeForTests();
  }

  @Parameterized.Parameters
  public static Collection<Object> vectorSizes()
  {
    return List.of(1, 2, 4, 7, 512);
  }

  private Closer closer;

  @BeforeEach
  void setUp()
  {
    closer = Closer.create();
  }

  @AfterEach
  void tearDown() throws IOException
  {
    closer.close();
  }

  /**
   * Tests long and double columns.
   */
  @ParameterizedTest(name = "vectorSize = {0}")
  @MethodSource("vectorSizes")
  public void testNumberColumns(int vectorSize)
  {
    IncrementalIndex incrementalIndex = closer.register(
        new OnheapIncrementalIndex.Builder()
            .setMaxRowCount(100)
            .setIndexSchema(
                IncrementalIndexSchema
                    .builder()
                    .withDimensionsSpec(
                        DimensionsSpec.builder()
                                      .useSchemaDiscovery(true)
                                      .setIncludeAllDimensions(true)
                                      .setDimensions(
                                          List.of(
                                              new AutoTypeColumnSchema("A", null, null),
                                              // set B to DOUBLE to avoid mixed types, which causes the base nonvector
                                              // and vector selectors to return different objects
                                              new AutoTypeColumnSchema("B", ColumnType.DOUBLE, null),
                                              new AutoTypeColumnSchema("C", null, null)

                                          )
                                      )
                                      .build()
                    )
                    .withRollup(false)
                    .build()
            )
            .build()
    );

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

    final SimpleQueryableIndex index =
        closer.register((SimpleQueryableIndex) TestIndex.persistAndMemoryMap(incrementalIndex));
    final QueryableIndexCursorFactory queryableIndexCursorFactory = new QueryableIndexCursorFactory(index);

    CursorBuildSpec cursorBuildSpec = CursorBuildSpec.builder()
                                                     .setQueryContext(
                                                         QueryContext.of(
                                                             Map.of(QueryContexts.VECTOR_SIZE_KEY, vectorSize)
                                                         )
                                                     )
                                                     .build();
    CursorHolder cursorHolder = closer.register(queryableIndexCursorFactory.makeCursorHolder(cursorBuildSpec));
    assertTrue(cursorHolder.canVectorize());
    VectorCursor vectorCursor = cursorHolder.asVectorCursor();

    Cursor cursor = cursorHolder.asCursor();
    ShimCursor shimCursor = new ShimCursor(vectorCursor);

    compareCursors(List.of("A", "B", "C", "non-existent"), cursor, shimCursor);
  }

  /**
   * Tests a few dictionary encoded columns, with a few number columns thrown in.
   */
  @ParameterizedTest(name = "vectorSize = {0}")
  @MethodSource("vectorSizes")
  public void testDictionaryColumns(int vectorSize)
  {
    IncrementalIndex incrementalIndex = closer.register(
        new OnheapIncrementalIndex.Builder()
            .setMaxRowCount(100)
            .setIndexSchema(
                IncrementalIndexSchema
                    .builder()
                    .withDimensionsSpec(
                        DimensionsSpec.builder()
                                      .useSchemaDiscovery(false)
                                      .setIncludeAllDimensions(true)
                                      .setDimensions(
                                          List.of(
                                              new AutoTypeColumnSchema("A", null, null),
                                              // set B to DOUBLE to avoid mixed types, which causes the base nonvector
                                              // and vector selectors to return different objects
                                              new AutoTypeColumnSchema("B", ColumnType.DOUBLE, null),
                                              new AutoTypeColumnSchema("C", null, null),
                                              new AutoTypeColumnSchema("D", null, null),
                                              new AutoTypeColumnSchema("E", null, null),
                                              new AutoTypeColumnSchema("F", null, null)
                                          )
                                      )
                                      .build()
                    )
                    .withRollup(false)
                    .build()
            )
            .build()
    );

    final List<String> signature = List.of("A", "B", "C", "D", "E", "F");

    incrementalIndex.add(autoRow(signature, 1, 1.0, "Tom", arr("Bat", "Knife"), obj("A", "B"), null));
    incrementalIndex.add(autoRow(signature, 2, 2.0, "Bob", arr("Builder", "Carpenter"), obj("A", "B"), null));
    incrementalIndex.add(autoRow(signature, 3, 4.0, "Jack", arr("A", "B"), obj("Jacky", "Bobby"), null));
    incrementalIndex.add(autoRow(signature, 4, 8.0, "Will", arr("Sing", "Dance"), obj("Sing", "B"), null));
    incrementalIndex.add(autoRow(signature, 5, 16.0, "Smith", arr("Car", "Trunk"), obj(), null));
    incrementalIndex.add(autoRow(signature, 6, -1, "Cat", arr(), obj("Bat", "Knife"), null));
    incrementalIndex.add(autoRow(signature, 7, -2.0, "Drew", arr("Machine", "Rabbit"), obj("bat", "knife"), null));

    final SimpleQueryableIndex index =
        closer.register((SimpleQueryableIndex) TestIndex.persistAndMemoryMap(incrementalIndex));
    final QueryableIndexCursorFactory queryableIndexCursorFactory = new QueryableIndexCursorFactory(index);

    CursorBuildSpec cursorBuildSpec = CursorBuildSpec.builder()
                                                     .setQueryContext(
                                                         QueryContext.of(
                                                             Map.of(QueryContexts.VECTOR_SIZE_KEY, vectorSize)
                                                         )
                                                     )
                                                     .build();
    CursorHolder cursorHolder = closer.register(queryableIndexCursorFactory.makeCursorHolder(cursorBuildSpec));
    assertTrue(cursorHolder.canVectorize());
    VectorCursor vectorCursor = cursorHolder.asVectorCursor();

    Cursor cursor = cursorHolder.asCursor();
    ShimCursor shimCursor = new ShimCursor(vectorCursor);

    compareCursors(signature, cursor, shimCursor);
  }

  @ParameterizedTest(name = "vectorSize = {0}")
  @MethodSource("vectorSizes")
  public void testMultiDimColumns(int vectorSize)
  {
    IncrementalIndex incrementalIndex = closer.register(
        new OnheapIncrementalIndex.Builder()
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
            .build()
    );

    final List<String> signature = List.of("A", "B");

    incrementalIndex.add(autoRow(signature, 1, arr("Bat", "Knife")));
    incrementalIndex.add(autoRow(signature, 2, arr("Builder", "Carpenter")));
    incrementalIndex.add(autoRow(signature, 3, arr("A", "B")));
    incrementalIndex.add(autoRow(signature, 4, arr("Sing", "Dance")));
    incrementalIndex.add(autoRow(signature, 5, arr("Car", "Trunk")));
    incrementalIndex.add(autoRow(signature, 6, arr()));
    incrementalIndex.add(autoRow(signature, 7, arr("Machine", "Rabbit")));

    final SimpleQueryableIndex index = closer.register((SimpleQueryableIndex) TestIndex.persistAndMemoryMap(
        incrementalIndex));
    final QueryableIndexCursorFactory queryableIndexCursorFactory = new QueryableIndexCursorFactory(index);

    CursorBuildSpec cursorBuildSpec = CursorBuildSpec.builder()
                                                     .setQueryContext(
                                                         QueryContext.of(
                                                             Map.of(QueryContexts.VECTOR_SIZE_KEY, vectorSize)
                                                         )
                                                     )
                                                     .build();
    CursorHolder cursorHolder = closer.register(queryableIndexCursorFactory.makeCursorHolder(cursorBuildSpec));
    assertTrue(cursorHolder.canVectorize());
    VectorCursor vectorCursor = cursorHolder.asVectorCursor();

    Cursor cursor = cursorHolder.asCursor();
    ShimCursor shimCursor = new ShimCursor(vectorCursor);

    compareCursors(signature, cursor, shimCursor);
  }

  @ParameterizedTest(name = "Non dict-encoded string columns with vector size {0}")
  @MethodSource("vectorSizes")
  public void testNonDictStringColumns(int vectorSize)
  {
    IncrementalIndex incrementalIndex = closer.register(
        new OnheapIncrementalIndex.Builder()
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
                                                  "v0",
                                                  "concat(\"A\", \"B\")",
                                                  ColumnType.STRING,
                                                  TestExprMacroTable.INSTANCE
                                              )
                                          )
                                      )
                                      .withRollup(false)
                                      .build()
            )
            .build()
    );

    final List<String> signature = List.of("A", "B");

    incrementalIndex.add(autoRow(signature, 1, "Tom"));
    incrementalIndex.add(autoRow(signature, 2, "Bob"));
    incrementalIndex.add(autoRow(signature, 3, "Jack"));
    incrementalIndex.add(autoRow(signature, 4, "Will"));
    incrementalIndex.add(autoRow(signature, 5, "Smith"));
    incrementalIndex.add(autoRow(signature, 6, "Cat"));
    incrementalIndex.add(autoRow(signature, 7, "Drew"));

    final SimpleQueryableIndex index =
        closer.register((SimpleQueryableIndex) TestIndex.persistAndMemoryMap(incrementalIndex));
    final QueryableIndexCursorFactory queryableIndexCursorFactory = new QueryableIndexCursorFactory(index);

    CursorBuildSpec cursorBuildSpec =
        CursorBuildSpec.builder()
                       .setQueryContext(
                           QueryContext.of(
                               Map.of(QueryContexts.VECTOR_SIZE_KEY, vectorSize)
                           )
                       )
                       .setVirtualColumns(VirtualColumns.create(
                                              new ExpressionVirtualColumn(
                                                  "v0",
                                                  "concat(\"A\", \"B\")",
                                                  ColumnType.STRING,
                                                  TestExprMacroTable.INSTANCE
                                              )
                                          )
                       )
                       .build();
    CursorHolder cursorHolder = closer.register(queryableIndexCursorFactory.makeCursorHolder(cursorBuildSpec));
    assertTrue(cursorHolder.canVectorize());
    VectorCursor vectorCursor = cursorHolder.asVectorCursor();

    Cursor cursor = cursorHolder.asCursor();
    ShimCursor shimCursor = new ShimCursor(vectorCursor);

    compareCursors(List.of("A", "B", "v0"), cursor, shimCursor);
  }

  @ParameterizedTest(name = "vectorSize = {0}")
  @MethodSource("vectorSizes")
  public void testArrayColumns(int vectorSize)
  {
    IncrementalIndex incrementalIndex = closer.register(
        new OnheapIncrementalIndex.Builder()
            .setMaxRowCount(100)
            .setIndexSchema(
                IncrementalIndexSchema
                    .builder()
                    .withDimensionsSpec(
                        DimensionsSpec.builder()
                                      .useSchemaDiscovery(false)
                                      .setIncludeAllDimensions(false)
                                      .setDimensions(
                                          List.of(
                                              new AutoTypeColumnSchema("longArr", ColumnType.LONG_ARRAY, null),
                                              new AutoTypeColumnSchema("doubleArr", ColumnType.DOUBLE_ARRAY, null),
                                              new AutoTypeColumnSchema("stringArr", ColumnType.STRING_ARRAY, null)
                                          )
                                      )
                                      .build()
                    )
                    .withRollup(false)
                    .build()
            )
            .build());

    final List<String> signature = List.of("longArr", "doubleArr", "stringArr");

    incrementalIndex.add(autoRow(signature, arr(2L, 3L), arr(2.2, 3.2), arr("a", "b")));
    incrementalIndex.add(autoRow(signature, null, null, null));
    incrementalIndex.add(autoRow(signature, arr(4L), arr(4.2), arr("c")));

    final SimpleQueryableIndex index =
        closer.register((SimpleQueryableIndex) TestIndex.persistAndMemoryMap(incrementalIndex));
    final QueryableIndexCursorFactory queryableIndexCursorFactory = new QueryableIndexCursorFactory(index);
    final CursorBuildSpec cursorBuildSpec =
        CursorBuildSpec.builder()
                       .setQueryContext(QueryContext.of(Map.of(QueryContexts.VECTOR_SIZE_KEY, vectorSize)))
                       .build();
    final CursorHolder cursorHolder = closer.register(queryableIndexCursorFactory.makeCursorHolder(cursorBuildSpec));
    assertTrue(cursorHolder.canVectorize());

    final VectorCursor vectorCursor = cursorHolder.asVectorCursor();
    final Cursor cursor = cursorHolder.asCursor();
    final ShimCursor shimCursor = new ShimCursor(vectorCursor);

    compareCursors(signature, cursor, shimCursor);
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
  private static void compareCursors(List<String> signature, Cursor expectedCursor, ShimCursor actualCursor)
  {
    final Map<String, ColumnCapabilities> capabilitiesMap = new HashMap<>();
    final ColumnSelectorFactory expectedFactory = expectedCursor.getColumnSelectorFactory();
    final ColumnSelectorFactory actualFactory = actualCursor.getColumnSelectorFactory();

    for (String columnName : signature) {
      // Check that capabilities match.
      final ColumnCapabilities expectedCapabilities = expectedFactory.getColumnCapabilities(columnName);
      final ColumnCapabilities actualCapabilities = actualFactory.getColumnCapabilities(columnName);
      compareCapabilities(columnName, expectedCapabilities, actualCapabilities);
      capabilitiesMap.put(columnName, expectedCapabilities);
    }

    final Map<String, DimensionSelector> expectedDimensionSelectors = new HashMap<>();
    final Map<String, DimensionSelector> actualDimensionSelectors = new HashMap<>();

    final Map<String, ColumnValueSelector<?>> expectedValueSelectors = new HashMap<>();
    final Map<String, ColumnValueSelector<?>> actualValueSelectors = new HashMap<>();

    for (String columnName : signature) {
      if (shouldTestDimensionSelector(capabilitiesMap.get(columnName))) {
        expectedDimensionSelectors.put(
            columnName,
            expectedFactory.makeDimensionSelector(new DefaultDimensionSpec(columnName, columnName))
        );

        actualDimensionSelectors.put(
            columnName,
            actualFactory.makeDimensionSelector(new DefaultDimensionSpec(columnName, columnName))
        );
      }

      expectedValueSelectors.put(columnName, expectedFactory.makeColumnValueSelector(columnName));
      actualValueSelectors.put(columnName, actualFactory.makeColumnValueSelector(columnName));
    }

    while (!expectedCursor.isDone()) {
      assertFalse(actualCursor.isDone());

      for (String columnName : signature) {
        final ColumnCapabilities capabilities = capabilitiesMap.get(columnName);

        compareColumnValueSelector(
            columnName,
            capabilities,
            expectedValueSelectors.get(columnName),
            actualValueSelectors.get(columnName)
        );

        if (shouldTestDimensionSelector(capabilities)) {
          compareDimensionSelector(
              columnName,
              expectedDimensionSelectors.get(columnName),
              actualDimensionSelectors.get(columnName)
          );
        }
      }

      expectedCursor.advance();
      actualCursor.advance();
    }

    assertTrue(actualCursor.isDone());
  }

  private static boolean shouldTestDimensionSelector(final ColumnCapabilities capabilities)
  {
    // makeDimensionSelector for array columns throws an exception on creation, don't test it
    return capabilities == null || !capabilities.is(ValueType.ARRAY);
  }

  private static void compareCapabilities(
      String columnName,
      @Nullable ColumnCapabilities expectedCapabilities,
      @Nullable ColumnCapabilities actualCapabilities
  )
  {
    assertEquals(
        expectedCapabilities != null,
        actualCapabilities != null,
        "presence of capabilities for " + columnName
    );

    if (expectedCapabilities == null) {
      return;
    }

    assertEquals(
        expectedCapabilities.getType(),
        actualCapabilities.getType(),
        "type of " + columnName
    );

    assertEquals(
        expectedCapabilities.getElementType(),
        actualCapabilities.getElementType(),
        "elementType of " + columnName
    );

    assertEquals(
        expectedCapabilities.getComplexTypeName(),
        actualCapabilities.getComplexTypeName(),
        "complexTypeName of " + columnName
    );

    assertEquals(
        expectedCapabilities.isDictionaryEncoded(),
        actualCapabilities.isDictionaryEncoded(),
        "dictionaryEncoded of " + columnName
    );

    assertEquals(
        expectedCapabilities.areDictionaryValuesUnique(),
        actualCapabilities.areDictionaryValuesUnique(),
        "areDictionaryValuesUnique of " + columnName
    );

    assertEquals(
        expectedCapabilities.areDictionaryValuesSorted(),
        actualCapabilities.areDictionaryValuesSorted(),
        "areDictionaryValuesSorted of " + columnName
    );

    assertEquals(
        expectedCapabilities.hasNulls(),
        actualCapabilities.hasNulls(),
        "hasNulls of " + columnName
    );

    assertEquals(
        expectedCapabilities.hasMultipleValues(),
        actualCapabilities.hasMultipleValues(),
        "hasMultipleValues of " + columnName
    );
  }

  /**
   * Verify that the "actualFactory" capabilities and current value match
   */
  private static void compareColumnValueSelector(
      String columnName,
      ColumnCapabilities capabilities,
      ColumnValueSelector<?> expectedSelector,
      ColumnValueSelector<?> actualSelector
  )
  {
    if (capabilities != null && capabilities.isNumeric()) {
      assertEquals(expectedSelector.getLong(), actualSelector.getLong(), "getLong for " + columnName);
      assertEquals(expectedSelector.getDouble(), actualSelector.getDouble(), "getDouble for " + columnName);
      assertEquals(expectedSelector.getFloat(), actualSelector.getFloat(), "getFloat for " + columnName);
      assertEquals(expectedSelector.isNull(), actualSelector.isNull(), "isNull for " + columnName);
    }

    final Object expectedObject = expectedSelector.getObject();
    final Object actualObject = actualSelector.getObject();

    if (expectedObject instanceof Object[]) {
      assertThat("type of getObject for " + columnName, actualObject, CoreMatchers.instanceOf(Object[].class));
      assertArrayEquals((Object[]) expectedObject, (Object[]) actualObject, "getObject for " + columnName);
    } else {
      assertEquals(expectedObject, actualObject, "getObject for " + columnName);
    }
  }

  private static void compareDimensionSelector(
      String columnName,
      DimensionSelector expectedSelector,
      DimensionSelector actualSelector
  )
  {
    assertEquals(
        expectedSelector.nameLookupPossibleInAdvance(),
        actualSelector.nameLookupPossibleInAdvance(),
        "nameLookupPossibleInAdvance for " + columnName
    );
    assertEquals(
        expectedSelector.supportsLookupNameUtf8(),
        actualSelector.supportsLookupNameUtf8(),
        "supportsLookupNameUtf8 for " + columnName
    );
    assertEquals(
        expectedSelector.idLookup() != null,
        actualSelector.idLookup() != null,
        "presence of idLookup for " + columnName
    );

    // getObject checks
    assertEquals(expectedSelector.getObject(), actualSelector.getObject(), "getObject for " + columnName);

    // getRow and lookupName/lookupNameUtf8 checks
    final IntList expectedRow = new IntArrayList();
    final IntList actualRow = new IntArrayList();
    expectedSelector.getRow().forEach(expectedRow::add);
    actualSelector.getRow().forEach(actualRow::add);

    assertEquals(expectedRow, actualRow, "getRow for " + columnName);

    for (int i = 0; i < expectedRow.size(); i++) {
      assertEquals(
          expectedSelector.lookupName(expectedRow.getInt(i)),
          actualSelector.lookupName(actualRow.getInt(i)),
          "lookupName for " + columnName + " id#" + expectedRow.getInt(i)
      );

      if (expectedSelector.supportsLookupNameUtf8()) {
        assertEquals(
            StringUtils.fromUtf8Nullable(expectedSelector.lookupNameUtf8(expectedRow.getInt(i))),
            StringUtils.fromUtf8Nullable(actualSelector.lookupNameUtf8(actualRow.getInt(i))),
            "lookupNameUtf8 for " + columnName + " id#" + expectedRow.getInt(i)
        );
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

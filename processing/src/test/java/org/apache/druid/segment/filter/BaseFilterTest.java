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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.segment.FrameSegment;
import org.apache.druid.frame.segment.FrameStorageAdapter;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.RowBasedStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.filter.cnf.CNFFilterExplosionException;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.virtual.ListFilteredVirtualColumn;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class BaseFilterTest extends InitializedNullHandlingTest
{
  static final String TIMESTAMP_COLUMN = "timestamp";

  static final VirtualColumns VIRTUAL_COLUMNS = VirtualColumns.create(
      ImmutableList.of(
          new ExpressionVirtualColumn("expr", "1.0 + 0.1", ColumnType.FLOAT, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("exprDouble", "1.0 + 1.1", ColumnType.DOUBLE, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("exprLong", "1 + 2", ColumnType.LONG, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("vdim0", "dim0", ColumnType.STRING, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("vdim1", "dim1", ColumnType.STRING, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("vs0", "s0", ColumnType.STRING, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("vd0", "d0", ColumnType.DOUBLE, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("vf0", "f0", ColumnType.FLOAT, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("vl0", "l0", ColumnType.LONG, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("vd0-add-sub", "d0 + (d0 - d0)", ColumnType.DOUBLE, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("vf0-add-sub", "f0 + (f0 - f0)", ColumnType.FLOAT, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("vl0-add-sub", "l0 + (l0 - l0)", ColumnType.LONG, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("double-vd0-add-sub", "vd0 + (vd0 - vd0)", ColumnType.DOUBLE, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("double-vf0-add-sub", "vf0 + (vf0 - vf0)", ColumnType.FLOAT, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("double-vl0-add-sub", "vl0 + (vl0 - vl0)", ColumnType.LONG, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("vdim3-concat", "dim3 + dim3", ColumnType.LONG, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("vdim2-offset", "array_offset(dim2, 1)", ColumnType.STRING, TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("nestedArrayLong", "array(arrayLong)", ColumnType.ofArray(ColumnType.LONG_ARRAY), TestExprMacroTable.INSTANCE),
          new ExpressionVirtualColumn("fake-nvl", "nvl(fake, 'hello')", ColumnType.STRING, TestExprMacroTable.INSTANCE),
          new ListFilteredVirtualColumn("allow-dim0", DefaultDimensionSpec.of("dim0"), ImmutableSet.of("3", "4"), true),
          new ListFilteredVirtualColumn("deny-dim0", DefaultDimensionSpec.of("dim0"), ImmutableSet.of("3", "4"), false),
          new ListFilteredVirtualColumn("allow-dim2", DefaultDimensionSpec.of("dim2"), ImmutableSet.of("a"), true),
          new ListFilteredVirtualColumn("deny-dim2", DefaultDimensionSpec.of("dim2"), ImmutableSet.of("a"), false),
          new NestedFieldVirtualColumn("nested", "$.s0", "nested.s0", ColumnType.STRING),
          new NestedFieldVirtualColumn("nested", "$.d0", "nested.d0", ColumnType.DOUBLE),
          new NestedFieldVirtualColumn("nested", "$.l0", "nested.l0", ColumnType.LONG),
          new NestedFieldVirtualColumn("nested", "$.arrayLong", "nested.arrayLong", ColumnType.LONG_ARRAY),
          new NestedFieldVirtualColumn("nested", "$.arrayDouble", "nested.arrayDouble", ColumnType.DOUBLE_ARRAY),
          new NestedFieldVirtualColumn("nested", "$.arrayString", "nested.arrayString", ColumnType.STRING_ARRAY)
      )
  );

  static final TimestampSpec DEFAULT_TIMESTAMP_SPEC = new TimestampSpec(TIMESTAMP_COLUMN, "iso", DateTimes.of("2000"));
  static final DimensionsSpec DEFAULT_DIM_SPEC = new DimensionsSpec(
      ImmutableList.<DimensionSchema>builder()
                   .addAll(
                       DimensionsSpec.getDefaultSchemas(
                           ImmutableList.of(
                               "dim0",
                               "dim1",
                               "dim2",
                               "dim3",
                               "timeDim",
                               "s0"
                           )
                       )
                   )
                   .add(new DoubleDimensionSchema("d0"))
                   .add(new FloatDimensionSchema("f0"))
                   .add(new LongDimensionSchema("l0"))
                   .add(new AutoTypeColumnSchema("arrayString", ColumnType.STRING_ARRAY))
                   .add(new AutoTypeColumnSchema("arrayLong", ColumnType.LONG_ARRAY))
                   .add(new AutoTypeColumnSchema("arrayDouble", ColumnType.DOUBLE_ARRAY))
                   .add(new AutoTypeColumnSchema("variant", null))
                   .add(new AutoTypeColumnSchema("nested", null))
                   .build()
  );

  static final InputRowParser<Map<String, Object>> DEFAULT_PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          DEFAULT_TIMESTAMP_SPEC,
          DEFAULT_DIM_SPEC
      )
  );

  // missing 'dim3' because makeDefaultSchemaRow does not expect to set it...
  static final RowSignature DEFAULT_ROW_SIGNATURE =
      RowSignature.builder()
                  .add("dim0", ColumnType.STRING)
                  .add("dim1", ColumnType.STRING)
                  .add("dim2", ColumnType.STRING)
                  .add("timeDim", ColumnType.STRING)
                  .add("s0", ColumnType.STRING)
                  .add("d0", ColumnType.DOUBLE)
                  .add("f0", ColumnType.FLOAT)
                  .add("l0", ColumnType.LONG)
                  .add("arrayString", ColumnType.STRING_ARRAY)
                  .add("arrayLong", ColumnType.LONG_ARRAY)
                  .add("arrayDouble", ColumnType.DOUBLE_ARRAY)
                  .add("variant", ColumnType.STRING_ARRAY)
                  .add("nested", ColumnType.NESTED_DATA)
                  .build();

  static final List<InputRow> DEFAULT_ROWS = ImmutableList.of(
      makeDefaultSchemaRow(
          "0",
          "",
          ImmutableList.of("a", "b"),
          "2017-07-25",
          "",
          0.0,
          0.0f,
          0L,
          ImmutableList.of("a", "b", "c"),
          ImmutableList.of(1L, 2L, 3L),
          ImmutableList.of(1.1, 2.2, 3.3),
          "abc",
          TestHelper.makeMapWithExplicitNull(
              "s0", "",
              "d0", 0.0,
              "f0", 0.0f,
              "l0", 0L,
              "arrayString", ImmutableList.of("a", "b", "c"),
              "arrayLong", ImmutableList.of(1L, 2L, 3L),
              "arrayDouble", ImmutableList.of(1.1, 2.2, 3.3),
              "variant", "abc"
          )
      ),
      makeDefaultSchemaRow(
          "1",
          "10",
          ImmutableList.of(),
          "2017-07-25",
          "a",
          10.1,
          10.1f,
          100L,
          ImmutableList.of(),
          ImmutableList.of(),
          new Object[]{1.1, 2.2, 3.3},
          100L,
          TestHelper.makeMapWithExplicitNull(
              "s0", "a",
              "d0", 10.1,
              "f0", 10.1f,
              "l0", 100L,
              "arrayString", ImmutableList.of(),
              "arrayLong", ImmutableList.of(),
              "arrayDouble", new Object[]{1.1, 2.2, 3.3},
              "variant", 100L
          )
      ),
      makeDefaultSchemaRow(
          "2",
          "2",
          ImmutableList.of(""),
          "2017-05-25",
          "b",
          null,
          5.5f,
          40L,
          null,
          new Object[]{1L, 2L, 3L},
          Collections.singletonList(null),
          "100",
          TestHelper.makeMapWithExplicitNull(
              "s0", "b",
              "d0", null,
              "f0", 5.5f,
              "l0", 40L,
              "arrayString", null,
              "arrayLong", new Object[]{1L, 2L, 3L},
              "arrayDouble", Collections.singletonList(null),
              "variant", "100"
          )
      ),
      makeDefaultSchemaRow(
          "3",
          "1",
          ImmutableList.of("a"),
          "2020-01-25",
          null,
          120.0245,
          110.0f,
          null,
          new Object[]{"a", "b", "c"},
          null,
          ImmutableList.of(),
          Arrays.asList(1.1, 2.2, 3.3),
          TestHelper.makeMapWithExplicitNull(
              "s0", null,
              "d0", 120.0245,
              "f0", 110.0f,
              "l0", null,
              "arrayString", new Object[]{"a", "b", "c"},
              "arrayLong", null,
              "arrayDouble", ImmutableList.of(),
              "variant", Arrays.asList(1.1, 2.2, 3.3)
          )
      ),
      makeDefaultSchemaRow(
          "4",
          "abdef",
          ImmutableList.of("c"),
          null,
          "c",
          60.0,
          null,
          9001L,
          ImmutableList.of("c", "d"),
          Collections.singletonList(null),
          new Object[]{-1.1, -333.3},
          12.34,
          TestHelper.makeMapWithExplicitNull(
              "s0", "c",
              "d0", 60.0,
              "f0", null,
              "l0", 9001L,
              "arrayString", ImmutableList.of("c", "d"),
              "arrayLong", Collections.singletonList(null),
              "arrayDouble", new Object[]{-1.1, -333.3},
              "variant", 12.34
          )
      ),
      makeDefaultSchemaRow(
          "5",
          "abc",
          null,
          "2020-01-25",
          "a",
          765.432,
          123.45f,
          12345L,
          Collections.singletonList(null),
          new Object[]{123L, 345L},
          null,
          Arrays.asList(100, 200, 300),
          TestHelper.makeMapWithExplicitNull(
              "s0", "a",
              "d0", 765.432,
              "f0", 123.45f,
              "l0", 12345L,
              "arrayString", Collections.singletonList(null),
              "arrayLong", new Object[]{123L, 345L},
              "arrayDouble", null,
              "variant", Arrays.asList(100, 200, 300)
          )
      )
  );

  static final IncrementalIndexSchema DEFAULT_INDEX_SCHEMA = new IncrementalIndexSchema.Builder()
      .withDimensionsSpec(DEFAULT_DIM_SPEC)
      .withMetrics(new CountAggregatorFactory("count"))
      .build();

  static InputRow makeDefaultSchemaRow(
      @Nullable Object... elements
  )
  {
    return makeSchemaRow(DEFAULT_PARSER, DEFAULT_ROW_SIGNATURE, elements);
  }


  public static InputRow makeSchemaRow(
      final InputRowParser<Map<String, Object>> parser,
      final RowSignature signature,
      @Nullable Object... elements
  )
  {
    Map<String, Object> mapRow = Maps.newHashMapWithExpectedSize(signature.size());
    for (int i = 0; i < signature.size(); i++) {
      final String columnName = signature.getColumnName(i);
      if (elements != null && i < elements.length) {
        final Object value = elements[i];
        mapRow.put(columnName, value);
      } else {
        mapRow.put(columnName, null);
      }
    }
    return parser.parseBatch(mapRow).get(0);
  }


  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final List<InputRow> rows;

  protected final IndexBuilder indexBuilder;
  protected final Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher;
  protected final boolean cnf;
  protected final boolean optimize;
  protected final String testName;

  // 'rowBasedWithoutTypeSignature' does not handle numeric null default values correctly, is equivalent to
  // druid.generic.useDefaultValueForNull being set to false, regardless of how it is actually set.
  // In other words, numeric null values will be treated as nulls instead of the default value
  protected final boolean canTestNumericNullsAsDefaultValues;

  protected StorageAdapter adapter;

  // JUnit creates a new test instance for every test method call.
  // For filter tests, the test setup creates a segment.
  // Creating a new segment for every test method call is pretty slow, so cache the StorageAdapters.
  // Each thread gets its own map.
  private static ThreadLocal<Map<String, Map<String, Pair<StorageAdapter, Closeable>>>> adapterCache =
      ThreadLocal.withInitial(HashMap::new);

  public BaseFilterTest(
      String testName,
      List<InputRow> rows,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    this.testName = testName;
    this.rows = rows;
    this.indexBuilder = indexBuilder;
    this.finisher = finisher;
    this.cnf = cnf;
    this.optimize = optimize;
    this.canTestNumericNullsAsDefaultValues =
        NullHandling.replaceWithDefault() && !testName.contains("finisher[rowBasedWithoutTypeSignature]");
  }

  @Before
  public void setUp() throws Exception
  {
    NestedDataModule.registerHandlersAndSerde();
    String className = getClass().getName();
    Map<String, Pair<StorageAdapter, Closeable>> adaptersForClass = adapterCache.get().get(className);
    if (adaptersForClass == null) {
      adaptersForClass = new HashMap<>();
      adapterCache.get().put(className, adaptersForClass);
    }

    Pair<StorageAdapter, Closeable> pair = adaptersForClass.get(testName);
    if (pair == null) {
      pair = finisher.apply(
          indexBuilder.tmpDir(temporaryFolder.newFolder()).rows(rows)
      );
      adaptersForClass.put(testName, pair);
    }

    this.adapter = pair.lhs;

  }

  public static void tearDown(String className) throws Exception
  {
    Map<String, Pair<StorageAdapter, Closeable>> adaptersForClass = adapterCache.get().get(className);

    if (adaptersForClass != null) {
      for (Map.Entry<String, Pair<StorageAdapter, Closeable>> entry : adaptersForClass.entrySet()) {
        Closeable closeable = entry.getValue().rhs;
        closeable.close();
      }
      adapterCache.get().put(className, null);
    }
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    return makeConstructors();
  }

  public static Collection<Object[]> makeConstructors()
  {
    final List<Object[]> constructors = new ArrayList<>();

    final Map<String, BitmapSerdeFactory> bitmapSerdeFactories = ImmutableMap.of(
        "concise", new ConciseBitmapSerdeFactory(),
        "roaring", RoaringBitmapSerdeFactory.getInstance()
    );

    final Map<String, SegmentWriteOutMediumFactory> segmentWriteOutMediumFactories = ImmutableMap.of(
        "tmpFile segment write-out medium", TmpFileSegmentWriteOutMediumFactory.instance(),
        "off-heap memory segment write-out medium", OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );

    final Map<String, Function<IndexBuilder, Pair<StorageAdapter, Closeable>>> finishers =
        ImmutableMap.<String, Function<IndexBuilder, Pair<StorageAdapter, Closeable>>>builder()
                    .put(
                        "incremental",
                        input -> {
                          final IncrementalIndex index = input.buildIncrementalIndex();
                          return Pair.of(new IncrementalIndexStorageAdapter(index), index);
                        }
                    )
                    .put(
                        "incrementalAutoTypes",
                        input -> {
                          input.indexSpec(IndexSpec.builder().build());
                          input.mapSchema(
                              schema ->
                                  new IncrementalIndexSchema(
                                      schema.getMinTimestamp(),
                                      schema.getTimestampSpec(),
                                      schema.getGran(),
                                      schema.getVirtualColumns(),
                                      schema.getDimensionsSpec().withDimensions(
                                          schema.getDimensionsSpec()
                                                .getDimensions()
                                                .stream()
                                                .map(
                                                    dimensionSchema -> new AutoTypeColumnSchema(dimensionSchema.getName(), null)
                                                )
                                                .collect(Collectors.toList())
                                      ),
                                      schema.getMetrics(),
                                      schema.isRollup()
                                  )
                          );
                          final IncrementalIndex index = input.buildIncrementalIndex();
                          return Pair.of(new IncrementalIndexStorageAdapter(index), index);
                        }
                    )
                    .put(
                        "mmappedAutoTypes",
                        input -> {
                          input.indexSpec(IndexSpec.builder().build());
                          input.mapSchema(
                              schema ->
                                  new IncrementalIndexSchema(
                                      schema.getMinTimestamp(),
                                      schema.getTimestampSpec(),
                                      schema.getGran(),
                                      schema.getVirtualColumns(),
                                      schema.getDimensionsSpec().withDimensions(
                                          schema.getDimensionsSpec()
                                                .getDimensions()
                                                .stream()
                                                .map(
                                                    dimensionSchema -> new AutoTypeColumnSchema(dimensionSchema.getName(), null)
                                                )
                                                .collect(Collectors.toList())
                                      ),
                                      schema.getMetrics(),
                                      schema.isRollup()
                                  )
                          );
                          final QueryableIndex index = input.buildMMappedIndex();
                          return Pair.of(new QueryableIndexStorageAdapter(index), index);
                        }
                    )
                    .put(
                        "mmappedAutoTypesMerged",
                        input -> {
                          final QueryableIndex index =
                              input
                                  .mapSchema(
                                      schema ->
                                          new IncrementalIndexSchema(
                                              schema.getMinTimestamp(),
                                              schema.getTimestampSpec(),
                                              schema.getGran(),
                                              schema.getVirtualColumns(),
                                              schema.getDimensionsSpec().withDimensions(
                                                  schema.getDimensionsSpec()
                                                        .getDimensions()
                                                        .stream()
                                                        .map(
                                                            dimensionSchema -> new AutoTypeColumnSchema(dimensionSchema.getName(), null)
                                                        )
                                                        .collect(Collectors.toList())
                                              ),
                                              schema.getMetrics(),
                                              schema.isRollup()
                                          )
                                  )
                                  // if 1 row per segment some of the columns have null values for the row which causes 'auto'
                                  // typing default value coercion to be lost in default value mode, so make sure there is at
                                  // least one number in each segment for these tests to pass correctly because the column
                                  // is typeless and so doesn't write out zeros like regular numbers do
                                  .intermediaryPersistSize(3)
                                  .buildMMappedIndex();

                          return Pair.of(new QueryableIndexStorageAdapter(index), index);
                        }
                    )
                    .put(
                        "mmapped",
                        input -> {
                          final QueryableIndex index = input.buildMMappedIndex();
                          return Pair.of(new QueryableIndexStorageAdapter(index), index);
                        }
                    )
                    .put(
                        "mmappedMerged",
                        input -> {
                          final QueryableIndex index = input.buildMMappedMergedIndex();
                          return Pair.of(new QueryableIndexStorageAdapter(index), index);
                        }
                    )
                    .put(
                        "mmappedWithSqlCompatibleNulls",
                        input -> {
                          // Build mmapped index in SQL-compatible null handling mode; read it in default-value mode.
                          Assume.assumeTrue(NullHandling.replaceWithDefault());
                          final File file;
                          try {
                            NullHandling.initializeForTestsWithValues(false, null);
                            Assert.assertTrue(NullHandling.sqlCompatible());
                            file = input.buildMMappedIndexFile();
                          }
                          finally {
                            NullHandling.initializeForTests();
                          }

                          Assert.assertTrue(NullHandling.replaceWithDefault());
                          try {
                            final QueryableIndex index = input.getIndexIO().loadIndex(file);
                            return Pair.of(new QueryableIndexStorageAdapter(index), index);
                          }
                          catch (IOException e) {
                            throw new RuntimeException(e);
                          }
                        }
                    )
                    .put(
                        "rowBasedWithoutTypeSignature",
                        input -> Pair.of(input.buildRowBasedSegmentWithoutTypeSignature().asStorageAdapter(), () -> {})
                    )
                    .put(
                        "rowBasedWithTypeSignature",
                        input -> Pair.of(input.buildRowBasedSegmentWithTypeSignature().asStorageAdapter(), () -> {})
                    )
                    .put("frame (row-based)", input -> {
                      // remove variant type columns from row frames since they aren't currently supported
                      input.mapSchema(
                          schema ->
                              new IncrementalIndexSchema(
                                  schema.getMinTimestamp(),
                                  schema.getTimestampSpec(),
                                  schema.getGran(),
                                  schema.getVirtualColumns(),
                                  schema.getDimensionsSpec().withDimensions(
                                      schema.getDimensionsSpec()
                                            .getDimensions()
                                            .stream()
                                            .filter(dimensionSchema -> !dimensionSchema.getName().equals("variant"))
                                            .collect(Collectors.toList())
                                  ),
                                  schema.getMetrics(),
                                  schema.isRollup()
                              )
                      );
                      final FrameSegment segment = input.buildFrameSegment(FrameType.ROW_BASED);
                      return Pair.of(segment.asStorageAdapter(), segment);
                    })
                    .put("frame (columnar)", input -> {
                      // remove array type columns from columnar frames since they aren't currently supported
                      input.mapSchema(
                          schema ->
                              new IncrementalIndexSchema(
                                  schema.getMinTimestamp(),
                                  schema.getTimestampSpec(),
                                  schema.getGran(),
                                  schema.getVirtualColumns(),
                                  schema.getDimensionsSpec().withDimensions(
                                      schema.getDimensionsSpec()
                                            .getDimensions()
                                            .stream()
                                            .filter(dimensionSchema -> !(dimensionSchema instanceof AutoTypeColumnSchema))
                                            .collect(Collectors.toList())
                                  ),
                                  schema.getMetrics(),
                                  schema.isRollup()
                              )
                      );
                      final FrameSegment segment = input.buildFrameSegment(FrameType.COLUMNAR);
                      return Pair.of(segment.asStorageAdapter(), segment);
                    })
                    .build();

    StringEncodingStrategy[] stringEncoding = new StringEncodingStrategy[]{
        new StringEncodingStrategy.Utf8(),
        new StringEncodingStrategy.FrontCoded(4, FrontCodedIndexed.V0),
        new StringEncodingStrategy.FrontCoded(4, FrontCodedIndexed.V1)
    };
    for (Map.Entry<String, BitmapSerdeFactory> bitmapSerdeFactoryEntry : bitmapSerdeFactories.entrySet()) {
      for (Map.Entry<String, SegmentWriteOutMediumFactory> segmentWriteOutMediumFactoryEntry :
          segmentWriteOutMediumFactories.entrySet()) {
        for (Map.Entry<String, Function<IndexBuilder, Pair<StorageAdapter, Closeable>>> finisherEntry :
            finishers.entrySet()) {
          for (boolean cnf : ImmutableList.of(false, true)) {
            for (boolean optimize : ImmutableList.of(false, true)) {
              for (boolean storeNullColumns : ImmutableList.of(false, true)) {
                for (StringEncodingStrategy encodingStrategy : stringEncoding) {
                  final String testName = StringUtils.format(
                      "bitmaps[%s], indexMerger[%s], finisher[%s], cnf[%s], optimize[%s], stringDictionaryEncoding[%s], storeNullColumns[%s]",
                      bitmapSerdeFactoryEntry.getKey(),
                      segmentWriteOutMediumFactoryEntry.getKey(),
                      finisherEntry.getKey(),
                      cnf,
                      optimize,
                      encodingStrategy.getType(),
                      storeNullColumns
                  );
                  final IndexBuilder indexBuilder = IndexBuilder
                      .create()
                      .schema(DEFAULT_INDEX_SCHEMA)
                      .writeNullColumns(storeNullColumns)
                      .indexSpec(
                          IndexSpec.builder()
                                   .withBitmapSerdeFactory(bitmapSerdeFactoryEntry.getValue())
                                   .withStringDictionaryEncoding(encodingStrategy)
                                   .build()
                      )
                      .segmentWriteOutMediumFactory(segmentWriteOutMediumFactoryEntry.getValue());
                  constructors.add(new Object[]{testName, indexBuilder, finisherEntry.getValue(), cnf, optimize});
                }
              }
            }
          }
        }
      }
    }

    return constructors;
  }

  protected boolean isAutoSchema()
  {
    if (testName.contains("AutoTypes")) {
      return true;
    }
    return false;
  }

  protected boolean canTestArrayColumns()
  {
    if (testName.contains("frame (columnar)") || testName.contains("rowBasedWithoutTypeSignature")) {
      return false;
    }
    return true;
  }

  private Filter makeFilter(final DimFilter dimFilter)
  {
    if (dimFilter == null) {
      return null;
    }

    final DimFilter maybeOptimized = optimize ? dimFilter.optimize(false) : dimFilter;
    final Filter filter = maybeOptimized.toFilter();
    try {
      return cnf ? Filters.toCnf(filter) : filter;
    }
    catch (CNFFilterExplosionException cnfFilterExplosionException) {
      throw new RuntimeException(cnfFilterExplosionException);
    }
  }

  private DimFilter maybeOptimize(final DimFilter dimFilter)
  {
    if (dimFilter == null) {
      return null;
    }
    return optimize ? dimFilter.optimize(false) : dimFilter;
  }

  private Sequence<Cursor> makeCursorSequence(final Filter filter)
  {
    return adapter.makeCursors(
        filter,
        Intervals.ETERNITY,
        VIRTUAL_COLUMNS,
        Granularities.ALL,
        false,
        null
    );
  }

  private VectorCursor makeVectorCursor(final Filter filter)
  {

    return adapter.makeVectorCursor(
        filter,
        Intervals.ETERNITY,
        VIRTUAL_COLUMNS,
        false,
        3, // Vector size smaller than the number of rows, to ensure we use more than one.
        null
    );
  }

  /**
   * Selects elements from "selectColumn" from rows matching a filter. selectColumn must be a single valued dimension.
   */
  private List<String> selectColumnValuesMatchingFilter(final DimFilter filter, final String selectColumn)
  {
    final Sequence<Cursor> cursors = makeCursorSequence(makeFilter(filter));
    Sequence<List<String>> seq = Sequences.map(
        cursors,
        cursor -> {
          final DimensionSelector selector = cursor
              .getColumnSelectorFactory()
              .makeDimensionSelector(new DefaultDimensionSpec(selectColumn, selectColumn));

          final List<String> values = new ArrayList<>();

          while (!cursor.isDone()) {
            IndexedInts row = selector.getRow();
            Preconditions.checkState(row.size() == 1);
            values.add(selector.lookupName(row.get(0)));
            cursor.advance();
          }

          return values;
        }
    );
    return seq.toList().get(0);
  }

  private long selectCountUsingFilteredAggregator(final DimFilter filter)
  {
    final Sequence<Cursor> cursors = makeCursorSequence(null);
    Sequence<Aggregator> aggSeq = Sequences.map(
        cursors,
        cursor -> {
          Aggregator agg = new FilteredAggregatorFactory(
              new CountAggregatorFactory("count"),
              maybeOptimize(filter)
          ).factorize(cursor.getColumnSelectorFactory());

          for (; !cursor.isDone(); cursor.advance()) {
            agg.aggregate();
          }

          return agg;
        }
    );
    return aggSeq.toList().get(0).getLong();
  }

  private long selectCountUsingVectorizedFilteredAggregator(final DimFilter dimFilter)
  {
    Preconditions.checkState(
        makeFilter(dimFilter).canVectorizeMatcher(adapter),
        "Cannot vectorize filter: %s",
        dimFilter
    );

    try (final VectorCursor cursor = makeVectorCursor(null)) {
      final FilteredAggregatorFactory aggregatorFactory = new FilteredAggregatorFactory(
          new CountAggregatorFactory("count"),
          maybeOptimize(dimFilter)
      );
      final VectorAggregator aggregator = aggregatorFactory.factorizeVector(cursor.getColumnSelectorFactory());
      final ByteBuffer buf = ByteBuffer.allocate(aggregatorFactory.getMaxIntermediateSizeWithNulls() * 2);

      // Use two slots: one for each form of aggregate.
      aggregator.init(buf, 0);
      aggregator.init(buf, aggregatorFactory.getMaxIntermediateSizeWithNulls());

      for (; !cursor.isDone(); cursor.advance()) {
        aggregator.aggregate(buf, 0, 0, cursor.getCurrentVectorSize());

        final int[] positions = new int[cursor.getCurrentVectorSize()];
        Arrays.fill(positions, aggregatorFactory.getMaxIntermediateSizeWithNulls());

        final int[] allRows = new int[cursor.getCurrentVectorSize()];
        for (int i = 0; i < allRows.length; i++) {
          allRows[i] = i;
        }

        aggregator.aggregate(buf, cursor.getCurrentVectorSize(), positions, allRows, 0);
      }

      final long val1 = (long) aggregator.get(buf, 0);
      final long val2 = (long) aggregator.get(buf, aggregatorFactory.getMaxIntermediateSizeWithNulls());

      if (val1 != val2) {
        throw new ISE("Oh no, val1[%d] != val2[%d]", val1, val2);
      }

      return val1;
    }
  }

  private List<String> selectColumnValuesMatchingFilterUsingPostFiltering(
      final DimFilter filter,
      final String selectColumn
  )
  {
    final Filter theFilter = makeFilter(filter);
    final Filter postFilteringFilter = new Filter()
    {

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
      {
        return theFilter.makeMatcher(factory);
      }

      @Override
      public Set<String> getRequiredColumns()
      {
        return Collections.emptySet();
      }

      @Nullable
      @Override
      public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
      {
        return null;
      }
    };

    final Sequence<Cursor> cursors = makeCursorSequence(postFilteringFilter);
    Sequence<List<String>> seq = Sequences.map(
        cursors,
        cursor -> {
          final DimensionSelector selector = cursor
              .getColumnSelectorFactory()
              .makeDimensionSelector(new DefaultDimensionSpec(selectColumn, selectColumn));

          final List<String> values = new ArrayList<>();

          while (!cursor.isDone()) {
            IndexedInts row = selector.getRow();
            Preconditions.checkState(row.size() == 1);
            values.add(selector.lookupName(row.get(0)));
            cursor.advance();
          }

          return values;
        }
    );
    return seq.toList().get(0);
  }

  private List<String> selectColumnValuesMatchingFilterUsingVectorizedPostFiltering(
      final DimFilter filter,
      final String selectColumn
  )
  {
    final Filter theFilter = makeFilter(filter);
    final Filter postFilteringFilter = new Filter()
    {

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
      {
        return theFilter.makeMatcher(factory);
      }

      @Override
      public VectorValueMatcher makeVectorMatcher(VectorColumnSelectorFactory factory)
      {
        return theFilter.makeVectorMatcher(factory);
      }

      @Override
      public boolean canVectorizeMatcher(ColumnInspector inspector)
      {
        return theFilter.canVectorizeMatcher(inspector);
      }

      @Override
      public Set<String> getRequiredColumns()
      {
        return null;
      }

      @Nullable
      @Override
      public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
      {
        return null;
      }
    };

    try (final VectorCursor cursor = makeVectorCursor(postFilteringFilter)) {
      final SingleValueDimensionVectorSelector selector = cursor
          .getColumnSelectorFactory()
          .makeSingleValueDimensionSelector(new DefaultDimensionSpec(selectColumn, selectColumn));

      final List<String> values = new ArrayList<>();

      while (!cursor.isDone()) {
        final int[] rowVector = selector.getRowVector();
        for (int i = 0; i < cursor.getCurrentVectorSize(); i++) {
          values.add(selector.lookupName(rowVector[i]));
        }
        cursor.advance();
      }

      return values;
    }
  }

  private List<String> selectColumnValuesMatchingFilterUsingVectorCursor(
      final DimFilter filter,
      final String selectColumn
  )
  {
    try (final VectorCursor cursor = makeVectorCursor(makeFilter(filter))) {
      final SingleValueDimensionVectorSelector selector = cursor
          .getColumnSelectorFactory()
          .makeSingleValueDimensionSelector(new DefaultDimensionSpec(selectColumn, selectColumn));

      final List<String> values = new ArrayList<>();

      while (!cursor.isDone()) {
        final int[] rowVector = selector.getRowVector();
        for (int i = 0; i < cursor.getCurrentVectorSize(); i++) {
          values.add(selector.lookupName(rowVector[i]));
        }
        cursor.advance();
      }

      return values;
    }
  }

  private List<String> selectColumnValuesMatchingFilterUsingVectorVirtualColumnCursor(
      final DimFilter filter,
      final String virtualColumn,
      final String selectColumn
  )
  {
    final Expr parsedIdentifier = Parser.parse(selectColumn, TestExprMacroTable.INSTANCE);
    try (final VectorCursor cursor = makeVectorCursor(makeFilter(filter))) {

      final ExpressionType outputType = parsedIdentifier.getOutputType(cursor.getColumnSelectorFactory());
      final List<String> values = new ArrayList<>();

      if (outputType.is(ExprType.STRING)) {
        final VectorObjectSelector objectSelector = cursor.getColumnSelectorFactory().makeObjectSelector(
            virtualColumn
        );
        while (!cursor.isDone()) {
          final Object[] rowVector = objectSelector.getObjectVector();
          for (int i = 0; i < cursor.getCurrentVectorSize(); i++) {
            values.add((String) rowVector[i]);
          }
          cursor.advance();
        }
      } else {
        final VectorValueSelector valueSelector = cursor.getColumnSelectorFactory().makeValueSelector(virtualColumn);
        while (!cursor.isDone()) {
          final boolean[] nulls = valueSelector.getNullVector();
          if (outputType.is(ExprType.DOUBLE)) {
            final double[] doubles = valueSelector.getDoubleVector();
            for (int i = 0; i < cursor.getCurrentVectorSize(); i++) {
              if (nulls != null && nulls[i]) {
                values.add(null);
              } else {
                values.add(String.valueOf(doubles[i]));
              }
            }
          } else {
            final long[] longs = valueSelector.getLongVector();
            for (int i = 0; i < cursor.getCurrentVectorSize(); i++) {
              if (nulls != null && nulls[i]) {
                values.add(null);
              } else {
                values.add(String.valueOf(longs[i]));
              }
            }
          }

          cursor.advance();
        }
      }


      return values;
    }
  }

  private List<String> selectColumnValuesMatchingFilterUsingRowBasedColumnSelectorFactory(
      final DimFilter filter,
      final String selectColumn
  )
  {
    // Generate rowSignature
    final RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    for (String columnName : Iterables.concat(adapter.getAvailableDimensions(), adapter.getAvailableMetrics())) {
      rowSignatureBuilder.add(columnName, adapter.getColumnCapabilities(columnName).toColumnType());
    }

    // Perform test
    final SettableSupplier<InputRow> rowSupplier = new SettableSupplier<>();
    final ValueMatcher matcher = makeFilter(filter).makeMatcher(
        VIRTUAL_COLUMNS.wrap(
            RowBasedColumnSelectorFactory.create(
                RowAdapters.standardRow(),
                rowSupplier::get,
                rowSignatureBuilder.build(),
                false,
                false
            )
        )
    );
    final List<String> values = new ArrayList<>();
    for (InputRow row : rows) {
      rowSupplier.set(row);
      if (matcher.matches(false)) {
        values.add((String) row.getRaw(selectColumn));
      }
    }
    return values;
  }

  protected void assertFilterMatches(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    // IncrementalIndex, RowBasedSegment cannot vectorize.
    // Columnar FrameStorageAdapter *can* vectorize, but the tests won't pass, because the vectorizable cases
    // differ from QueryableIndexStorageAdapter due to frames not having indexes. So, skip these too.
    final boolean testVectorized =
        !(adapter instanceof IncrementalIndexStorageAdapter)
        && !(adapter instanceof RowBasedStorageAdapter)
        && !(adapter instanceof FrameStorageAdapter);

    assertFilterMatches(filter, expectedRows, testVectorized);
    // test double inverted
    if (!StringUtils.toLowerCase(testName).contains("concise")) {
      assertFilterMatches(NotDimFilter.of(NotDimFilter.of(filter)), expectedRows, testVectorized);
    }
  }

  protected void assertFilterMatchesSkipArrays(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    // IncrementalIndex, RowBasedSegment cannot vectorize.
    // Columnar FrameStorageAdapter *can* vectorize, but the tests won't pass, because the vectorizable cases
    // differ from QueryableIndexStorageAdapter due to frames not having indexes. So, skip these too.
    final boolean testVectorized =
        !(adapter instanceof IncrementalIndexStorageAdapter)
        && !(adapter instanceof RowBasedStorageAdapter)
        && !(adapter instanceof FrameStorageAdapter);

    if (isAutoSchema()) {
      Throwable t = Assert.assertThrows(
          Throwable.class,
          () -> assertFilterMatches(filter, expectedRows, testVectorized)
      );
      Assert.assertTrue(t.getMessage().contains("ARRAY"));
    } else {
      assertFilterMatches(filter, expectedRows, testVectorized);
      // test double inverted
      if (!StringUtils.toLowerCase(testName).contains("concise")) {
        assertFilterMatches(NotDimFilter.of(NotDimFilter.of(filter)), expectedRows, testVectorized);
      }
    }
  }

  protected void assertFilterMatchesSkipVectorize(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    assertFilterMatches(filter, expectedRows, false);
    // test double inverted
    if (!StringUtils.toLowerCase(testName).contains("concise")) {
      assertFilterMatches(NotDimFilter.of(NotDimFilter.of(filter)), expectedRows, false);
    }
  }

  private void assertFilterMatches(
      final DimFilter filter,
      final List<String> expectedRows,
      final boolean testVectorized
  )
  {
    Assert.assertEquals(
        "Cursor: " + filter,
        expectedRows,
        selectColumnValuesMatchingFilter(filter, "dim0")
    );

    Assert.assertEquals(
        "Cursor with postFiltering: " + filter,
        expectedRows,
        selectColumnValuesMatchingFilterUsingPostFiltering(filter, "dim0")
    );

    Assert.assertEquals(
        "Filtered aggregator: " + filter,
        expectedRows.size(),
        selectCountUsingFilteredAggregator(filter)
    );

    Assert.assertEquals(
        "RowBasedColumnSelectorFactory: " + filter,
        expectedRows,
        selectColumnValuesMatchingFilterUsingRowBasedColumnSelectorFactory(filter, "dim0")
    );

    if (testVectorized) {
      Assert.assertEquals(
          "Cursor (vectorized): " + filter,
          expectedRows,
          selectColumnValuesMatchingFilterUsingVectorCursor(filter, "dim0")
      );

      Assert.assertEquals(
          "Cursor Virtual Column (vectorized): " + filter,
          expectedRows,
          selectColumnValuesMatchingFilterUsingVectorVirtualColumnCursor(filter, "vdim0", "dim0")
      );

      Assert.assertEquals(
          "Cursor with postFiltering (vectorized): " + filter,
          expectedRows,
          selectColumnValuesMatchingFilterUsingVectorizedPostFiltering(filter, "dim0")
      );
      Assert.assertEquals(
          "Filtered aggregator (vectorized): " + filter,
          expectedRows.size(),
          selectCountUsingVectorizedFilteredAggregator(filter)
      );
    }
  }
}

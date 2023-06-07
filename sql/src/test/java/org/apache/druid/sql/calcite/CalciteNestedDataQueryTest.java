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

package org.apache.druid.sql.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.ResourceInputSource;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.ExpressionLambdaAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.UnsupportedSQLQueryException;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CalciteNestedDataQueryTest extends BaseCalciteQueryTest
{
  private static final String DATA_SOURCE = "nested";
  private static final String DATA_SOURCE_MIXED = "nested_mix";
  private static final String DATA_SOURCE_MIXED_2 = "nested_mix_2";
  private static final String DATA_SOURCE_ARRAYS = "arrays";

  private static final List<ImmutableMap<String, Object>> RAW_ROWS = ImmutableList.of(
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-01")
                  .put("string", "aaa")
                  .put("string_sparse", "zzz")
                  .put("nest", ImmutableMap.of("x", 100L, "y", 2.02, "z", "300", "mixed", 1L, "mixed2", "1"))
                  .put(
                      "nester",
                      ImmutableMap.of("array", ImmutableList.of("a", "b"), "n", ImmutableMap.of("x", "hello"))
                  )
                  .put("long", 5L)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-01")
                  .put("string", "bbb")
                  .put("long", 4L)
                  .put("nester", "hello")
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-01")
                  .put("string", "ccc")
                  .put("string_sparse", "10")
                  .put("nest", ImmutableMap.of("x", 200L, "y", 3.03, "z", "abcdef", "mixed", 1.1, "mixed2", 1L))
                  .put("long", 3L)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-01")
                  .put("string", "ddd")
                  .put("string_sparse", "yyy")
                  .put("long", 2L)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-01")
                  .put("string", "eee")
                  .put("long", 1L)
                  .build(),
      // repeat on another day
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-02")
                  .put("string", "aaa")
                  .put("nest", ImmutableMap.of("x", 100L, "y", 2.02, "z", "400", "mixed2", 1.1))
                  .put("nester", ImmutableMap.of("array", ImmutableList.of("a", "b"), "n", ImmutableMap.of("x", 1L)))
                  .put("long", 5L)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-02")
                  .put("string", "ddd")
                  .put("long", 2L)
                  .put("nester", 2L)
                  .build()
  );

  private static final InputRowSchema ALL_JSON_COLUMNS = new InputRowSchema(
      new TimestampSpec("t", "iso", null),
      DimensionsSpec.builder().setDimensions(
          ImmutableList.<DimensionSchema>builder()
                       .add(new AutoTypeColumnSchema("string"))
                       .add(new AutoTypeColumnSchema("nest"))
                       .add(new AutoTypeColumnSchema("nester"))
                       .add(new AutoTypeColumnSchema("long"))
                       .add(new AutoTypeColumnSchema("string_sparse"))
                       .build()
      ).build(),
      null
  );

  private static final InputRowSchema JSON_AND_SCALAR_MIX = new InputRowSchema(
      new TimestampSpec("t", "iso", null),
      DimensionsSpec.builder().setDimensions(
          ImmutableList.<DimensionSchema>builder()
                       .add(new StringDimensionSchema("string"))
                       .add(new AutoTypeColumnSchema("nest"))
                       .add(new AutoTypeColumnSchema("nester"))
                       .add(new LongDimensionSchema("long"))
                       .add(new StringDimensionSchema("string_sparse"))
                       .build()
      ).build(),
      null
  );
  private static final List<InputRow> ROWS =
      RAW_ROWS.stream().map(raw -> TestDataBuilder.createRow(raw, ALL_JSON_COLUMNS)).collect(Collectors.toList());

  private static final List<InputRow> ROWS_MIX =
      RAW_ROWS.stream().map(raw -> TestDataBuilder.createRow(raw, JSON_AND_SCALAR_MIX)).collect(Collectors.toList());

  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModule(new NestedDataModule());
  }

  @SuppressWarnings("resource")
  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final JoinableFactoryWrapper joinableFactory,
      final Injector injector
  ) throws IOException
  {
    NestedDataModule.registerHandlersAndSerde();
    final QueryableIndex index =
        IndexBuilder.create()
                    .tmpDir(temporaryFolder.newFolder())
                    .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                    .schema(
                        new IncrementalIndexSchema.Builder()
                            .withMetrics(
                                new CountAggregatorFactory("cnt")
                            )
                            .withDimensionsSpec(ALL_JSON_COLUMNS.getDimensionsSpec())
                            .withRollup(false)
                            .build()
                    )
                    .rows(ROWS)
                    .buildMMappedIndex();

    final QueryableIndex indexMix11 =
        IndexBuilder.create()
                    .tmpDir(temporaryFolder.newFolder())
                    .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                    .schema(
                        new IncrementalIndexSchema.Builder()
                            .withMetrics(
                                new CountAggregatorFactory("cnt")
                            )
                            .withDimensionsSpec(ALL_JSON_COLUMNS.getDimensionsSpec())
                            .withRollup(false)
                            .build()
                    )
                    .rows(ROWS)
                    .buildMMappedIndex();


    final QueryableIndex indexMix12 =
        IndexBuilder.create()
                    .tmpDir(temporaryFolder.newFolder())
                    .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                    .schema(
                        new IncrementalIndexSchema.Builder()
                            .withMetrics(
                                new CountAggregatorFactory("cnt")
                            )
                            .withDimensionsSpec(JSON_AND_SCALAR_MIX.getDimensionsSpec())
                            .withRollup(false)
                            .build()
                    )
                    .rows(ROWS_MIX)
                    .buildMMappedIndex();

    final QueryableIndex indexMix21 =
        IndexBuilder.create()
                    .tmpDir(temporaryFolder.newFolder())
                    .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                    .schema(
                        new IncrementalIndexSchema.Builder()
                            .withMetrics(
                                new CountAggregatorFactory("cnt")
                            )
                            .withDimensionsSpec(JSON_AND_SCALAR_MIX.getDimensionsSpec())
                            .withRollup(false)
                            .build()
                    )
                    .rows(ROWS_MIX)
                    .buildMMappedIndex();

    final QueryableIndex indexMix22 =
        IndexBuilder.create()
                    .tmpDir(temporaryFolder.newFolder())
                    .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                    .schema(
                        new IncrementalIndexSchema.Builder()
                            .withMetrics(
                                new CountAggregatorFactory("cnt")
                            )
                            .withDimensionsSpec(ALL_JSON_COLUMNS.getDimensionsSpec())
                            .withRollup(false)
                            .build()
                    )
                    .rows(ROWS)
                    .buildMMappedIndex();

    final QueryableIndex indexArrays =
        IndexBuilder.create()
                    .tmpDir(temporaryFolder.newFolder())
                    .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                    .schema(
                        new IncrementalIndexSchema.Builder()
                            .withTimestampSpec(NestedDataTestUtils.AUTO_SCHEMA.getTimestampSpec())
                            .withDimensionsSpec(NestedDataTestUtils.AUTO_SCHEMA.getDimensionsSpec())
                            .withMetrics(
                                new CountAggregatorFactory("cnt")
                            )
                            .withRollup(false)
                            .build()
                    )
                    .inputSource(
                        ResourceInputSource.of(
                            NestedDataTestUtils.class.getClassLoader(),
                            NestedDataTestUtils.ARRAY_TYPES_DATA_FILE
                        )
                    )
                    .inputFormat(TestDataBuilder.DEFAULT_JSON_INPUT_FORMAT)
                    .inputTmpDir(temporaryFolder.newFolder())
                    .buildMMappedIndex();

    SpecificSegmentsQuerySegmentWalker walker = new SpecificSegmentsQuerySegmentWalker(conglomerate);
    walker.add(
        DataSegment.builder()
                   .dataSource(DATA_SOURCE)
                   .interval(index.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index
    ).add(
        DataSegment.builder()
                   .dataSource(DATA_SOURCE_MIXED)
                   .interval(indexMix11.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        indexMix11
    ).add(
        DataSegment.builder()
                   .dataSource(DATA_SOURCE_MIXED)
                   .interval(indexMix12.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(1))
                   .size(0)
                   .build(),
        indexMix12
    ).add(
        DataSegment.builder()
                   .dataSource(DATA_SOURCE_MIXED_2)
                   .interval(indexMix21.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        indexMix21
    ).add(
        DataSegment.builder()
                   .dataSource(DATA_SOURCE_MIXED_2)
                   .interval(index.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(1))
                   .size(0)
                   .build(),
        indexMix22
    ).add(
        DataSegment.builder()
                   .dataSource(DATA_SOURCE_ARRAYS)
                   .version("1")
                   .interval(indexArrays.getDataInterval())
                   .shardSpec(new LinearShardSpec(1))
                   .size(0)
                   .build(),
        indexArrays
    );

    return walker;
  }

  @Test
  public void testGroupByPath()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 4L},
            new Object[]{"100", 2L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupJsonValueAny()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE_ANY(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 4L},
            new Object[]{"100", 2L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByJsonValue()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 4L},
            new Object[]{"100", 2L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testTopNPath()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(DATA_SOURCE)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .virtualColumns(
                    new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.STRING)
                )
                .dimension(
                    new DefaultDimensionSpec("v0", "d0")
                )
                .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 4L},
            new Object[]{"100", 2L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRootPath()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nester, '$'), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nester", "$.", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 5L},
            new Object[]{"2", 1L},
            new Object[]{"hello", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRootSingleTypeLong()
  {
    testQuery(
        "SELECT "
        + "long, "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("long", "d0", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 1L},
            new Object[]{2L, 2L},
            new Object[]{3L, 1L},
            new Object[]{4L, 1L},
            new Object[]{5L, 2L}
        ),
        RowSignature.builder()
                    .add("long", ColumnType.LONG)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRootSingleTypeString()
  {
    testQuery(
        "SELECT "
        + "string, "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("string", "d0")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"aaa", 2L},
            new Object[]{"bbb", 1L},
            new Object[]{"ccc", 1L},
            new Object[]{"ddd", 2L},
            new Object[]{"eee", 1L}
        ),
        RowSignature.builder()
                    .add("string", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRootSingleTypeLongMixed1()
  {
    testQuery(
        "SELECT "
        + "long, "
        + "SUM(cnt) "
        + "FROM druid.nested_mix GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE_MIXED)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("long", "d0", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 2L},
            new Object[]{2L, 4L},
            new Object[]{3L, 2L},
            new Object[]{4L, 2L},
            new Object[]{5L, 4L}
        ),
        RowSignature.builder()
                    .add("long", ColumnType.LONG)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRootSingleTypeStringMixed1()
  {
    testQuery(
        "SELECT "
        + "string, "
        + "SUM(cnt) "
        + "FROM druid.nested_mix GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE_MIXED)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("string", "d0")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"aaa", 4L},
            new Object[]{"bbb", 2L},
            new Object[]{"ccc", 2L},
            new Object[]{"ddd", 4L},
            new Object[]{"eee", 2L}
        ),
        RowSignature.builder()
                    .add("string", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRootSingleTypeStringMixed1Sparse()
  {
    testQuery(
        "SELECT "
        + "string_sparse, "
        + "SUM(cnt) "
        + "FROM druid.nested_mix GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE_MIXED)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("string_sparse", "d0")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 8L},
            new Object[]{"10", 2L},
            new Object[]{"yyy", 2L},
            new Object[]{"zzz", 2L}
        ),
        RowSignature.builder()
                    .add("string_sparse", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRootSingleTypeLongMixed2()
  {
    testQuery(
        "SELECT "
        + "long, "
        + "SUM(cnt) "
        + "FROM druid.nested_mix_2 GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE_MIXED_2)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("long", "d0", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 2L},
            new Object[]{2L, 4L},
            new Object[]{3L, 2L},
            new Object[]{4L, 2L},
            new Object[]{5L, 4L}
        ),
        RowSignature.builder()
                    .add("long", ColumnType.LONG)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRootSingleTypeStringMixed2()
  {
    testQuery(
        "SELECT "
        + "string, "
        + "SUM(cnt) "
        + "FROM druid.nested_mix_2 GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE_MIXED_2)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("string", "d0")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"aaa", 4L},
            new Object[]{"bbb", 2L},
            new Object[]{"ccc", 2L},
            new Object[]{"ddd", 4L},
            new Object[]{"eee", 2L}
        ),
        RowSignature.builder()
                    .add("string", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRootSingleTypeStringMixed2Sparse()
  {
    testQuery(
        "SELECT "
        + "string_sparse, "
        + "SUM(cnt) "
        + "FROM druid.nested_mix_2 GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE_MIXED_2)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("string_sparse", "d0")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 8L},
            new Object[]{"10", 2L},
            new Object[]{"yyy", 2L},
            new Object[]{"zzz", 2L}
        ),
        RowSignature.builder()
                    .add("string_sparse", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRootSingleTypeStringMixed2SparseJsonValueNonExistentPath()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(string_sparse, '$[1]'), "
        + "SUM(cnt) "
        + "FROM druid.nested_mix_2 GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE_MIXED_2)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("string_sparse", "$[1]", "v0", ColumnType.STRING)
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 14L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testJsonValueArrays()
  {
    testBuilder()
        .sql(
            "SELECT "
            + "JSON_VALUE(arrayString, '$' RETURNING VARCHAR ARRAY), "
            + "JSON_VALUE(arrayLong, '$' RETURNING BIGINT ARRAY), "
            + "JSON_VALUE(arrayDouble, '$' RETURNING DOUBLE ARRAY), "
            + "JSON_VALUE(arrayNestedLong, '$[0]' RETURNING BIGINT ARRAY) "
            + "FROM druid.arrays"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                Druids.newScanQueryBuilder()
                      .dataSource(DATA_SOURCE_ARRAYS)
                      .intervals(querySegmentSpec(Filtration.eternity()))
                      .virtualColumns(
                          new NestedFieldVirtualColumn("arrayString", "$", "v0", ColumnType.STRING_ARRAY),
                          new NestedFieldVirtualColumn("arrayLong", "$", "v1", ColumnType.LONG_ARRAY),
                          new NestedFieldVirtualColumn("arrayDouble", "$", "v2", ColumnType.DOUBLE_ARRAY),
                          new NestedFieldVirtualColumn("arrayNestedLong", "$[0]", "v3", ColumnType.LONG_ARRAY)
                      )
                      .columns("v0", "v1", "v2", "v3")
                      .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                      .legacy(false)
                      .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{null, Arrays.asList(1L, 2L, 3L), Arrays.asList(1.1D, 2.2D, 3.3D), null},
                new Object[]{null, null, null, null},
                new Object[]{Arrays.asList("d", "e"), Arrays.asList(1L, 4L), Arrays.asList(2.2D, 3.3D, 4.0D), Arrays.asList(1L, 2L)},
                new Object[]{Arrays.asList("a", "b"), null, null, Collections.singletonList(1L)},
                new Object[]{Arrays.asList("a", "b"), Arrays.asList(1L, 2L, 3L), Arrays.asList(1.1D, 2.2D, 3.3D), Arrays.asList(1L, 2L, null)},
                new Object[]{Arrays.asList("b", "c"), Arrays.asList(1L, 2L, 3L, 4L), Arrays.asList(1.1D, 3.3D), Collections.singletonList(1L)},
                new Object[]{Arrays.asList("a", "b", "c"), Arrays.asList(2L, 3L), Arrays.asList(3.3D, 4.4D, 5.5D), null},
                new Object[]{null, Arrays.asList(1L, 2L, 3L), Arrays.asList(1.1D, 2.2D, 3.3D), null},
                new Object[]{null, null, null, null},
                new Object[]{Arrays.asList("d", "e"), Arrays.asList(1L, 4L), Arrays.asList(2.2D, 3.3D, 4.0D), Arrays.asList(1L, 2L)},
                new Object[]{Arrays.asList("a", "b"), null, null, null},
                new Object[]{Arrays.asList("a", "b"), Arrays.asList(1L, 2L, 3L), Arrays.asList(1.1D, 2.2D, 3.3D), Arrays.asList(2L, 3L)},
                new Object[]{Arrays.asList("b", "c"), Arrays.asList(1L, 2L, 3L, 4L), Arrays.asList(1.1D, 3.3D), Collections.singletonList(1L)},
                new Object[]{Arrays.asList("a", "b", "c"), Arrays.asList(2L, 3L), Arrays.asList(3.3D, 4.4D, 5.5D), null}

            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("EXPR$0", ColumnType.STRING_ARRAY)
                        .add("EXPR$1", ColumnType.LONG_ARRAY)
                        .add("EXPR$2", ColumnType.DOUBLE_ARRAY)
                        .add("EXPR$3", ColumnType.LONG_ARRAY)
                        .build()
        )
        .run();
  }

  @Test
  public void testUnnestRootSingleTypeArrayLongNulls()
  {
    testBuilder()
        .sql("SELECT longs FROM druid.arrays, UNNEST(arrayLongNulls) as u(longs)")
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                Druids.newScanQueryBuilder()
                      .dataSource(
                          UnnestDataSource.create(
                              TableDataSource.create(DATA_SOURCE_ARRAYS),
                              expressionVirtualColumn("j0.unnest", "\"arrayLongNulls\"", ColumnType.LONG_ARRAY),
                              null
                          )
                      )
                      .intervals(querySegmentSpec(Filtration.eternity()))
                      .columns("j0.unnest")
                      .context(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                      .legacy(false)
                      .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                      .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{2L},
                new Object[]{3L},
                new Object[]{1L},
                new Object[]{null},
                new Object[]{2L},
                new Object[]{9L},
                new Object[]{1L},
                new Object[]{null},
                new Object[]{3L},
                new Object[]{1L},
                new Object[]{2L},
                new Object[]{3L},
                new Object[]{2L},
                new Object[]{3L},
                new Object[]{null},
                new Object[]{null},
                new Object[]{2L},
                new Object[]{9L},
                new Object[]{1L},
                new Object[]{null},
                new Object[]{3L},
                new Object[]{1L},
                new Object[]{2L},
                new Object[]{3L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("longs", ColumnType.LONG)
                        .build()
        )
        .run();
  }
  @Test
  public void testUnnestRootSingleTypeArrayStringNulls()
  {
    cannotVectorize();
    testBuilder()
        .sql("SELECT strings FROM druid.arrays, UNNEST(arrayStringNulls) as u(strings)")
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                Druids.newScanQueryBuilder()
                      .dataSource(
                          UnnestDataSource.create(
                              TableDataSource.create(DATA_SOURCE_ARRAYS),
                              expressionVirtualColumn("j0.unnest", "\"arrayStringNulls\"", ColumnType.STRING_ARRAY),
                              null
                          )
                      )
                      .intervals(querySegmentSpec(Filtration.eternity()))
                      .columns("j0.unnest")
                      .context(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                      .legacy(false)
                      .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                      .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{"a"},
                new Object[]{"b"},
                new Object[]{"b"},
                new Object[]{"b"},
                new Object[]{"a"},
                new Object[]{"b"},
                new Object[]{"d"},
                new Object[]{NullHandling.defaultStringValue()},
                new Object[]{"b"},
                new Object[]{NullHandling.defaultStringValue()},
                new Object[]{"b"},
                new Object[]{"a"},
                new Object[]{"b"},
                new Object[]{"b"},
                new Object[]{"b"},
                new Object[]{NullHandling.defaultStringValue()},
                new Object[]{"d"},
                new Object[]{NullHandling.defaultStringValue()},
                new Object[]{"b"},
                new Object[]{NullHandling.defaultStringValue()},
                new Object[]{"b"}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("strings", ColumnType.STRING)
                        .build()
        )
        .run();
  }

  @Test
  public void testUnnestRootSingleTypeArrayDoubleNulls()
  {
    cannotVectorize();
    testBuilder()
        .sql("SELECT doubles FROM druid.arrays, UNNEST(arrayDoubleNulls) as u(doubles)")
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                Druids.newScanQueryBuilder()
                      .dataSource(
                          UnnestDataSource.create(
                              TableDataSource.create(DATA_SOURCE_ARRAYS),
                              expressionVirtualColumn("j0.unnest", "\"arrayDoubleNulls\"", ColumnType.DOUBLE_ARRAY),
                              null
                          )
                      )
                      .intervals(querySegmentSpec(Filtration.eternity()))
                      .columns("j0.unnest")
                      .context(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                      .legacy(false)
                      .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                      .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{null},
                new Object[]{999.0D},
                new Object[]{5.5D},
                new Object[]{null},
                new Object[]{1.1D},
                new Object[]{2.2D},
                new Object[]{null},
                new Object[]{null},
                new Object[]{2.2D},
                new Object[]{null},
                new Object[]{999.0D},
                new Object[]{null},
                new Object[]{5.5D},
                new Object[]{null},
                new Object[]{1.1D},
                new Object[]{999.0D},
                new Object[]{5.5D},
                new Object[]{null},
                new Object[]{1.1D},
                new Object[]{2.2D},
                new Object[]{null},
                new Object[]{null},
                new Object[]{2.2D},
                new Object[]{null},
                new Object[]{999.0D},
                new Object[]{null},
                new Object[]{5.5D}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("doubles", ColumnType.DOUBLE)
                        .build()
        )
        .run();
  }


  @Test
  public void testGroupByRootSingleTypeArrayLong()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "arrayLong, "
            + "SUM(cnt) "
            + "FROM druid.arrays GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("arrayLong", "d0", ColumnType.LONG_ARRAY)
                                )
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{null, 4L},
                new Object[]{Arrays.asList(1L, 2L, 3L), 4L},
                new Object[]{Arrays.asList(1L, 2L, 3L, 4L), 2L},
                new Object[]{Arrays.asList(1L, 4L), 2L},
                new Object[]{Arrays.asList(2L, 3L), 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("arrayLong", ColumnType.LONG_ARRAY)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayLongNulls()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "arrayLongNulls, "
            + "SUM(cnt) "
            + "FROM druid.arrays GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("arrayLongNulls", "d0", ColumnType.LONG_ARRAY)
                                )
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{null, 3L},
                new Object[]{Collections.emptyList(), 1L},
                new Object[]{Collections.singletonList(null), 1L},
                new Object[]{Arrays.asList(null, 2L, 9L), 2L},
                new Object[]{Collections.singletonList(1L), 1L},
                new Object[]{Arrays.asList(1L, null, 3L), 2L},
                new Object[]{Arrays.asList(1L, 2L, 3L), 2L},
                new Object[]{Arrays.asList(2L, 3L), 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("arrayLongNulls", ColumnType.LONG_ARRAY)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayLongNullsUnnest()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "longs, "
            + "SUM(cnt) "
            + "FROM druid.arrays, UNNEST(arrayLongNulls) as u (longs) GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(
                                UnnestDataSource.create(
                                    TableDataSource.create(DATA_SOURCE_ARRAYS),
                                    expressionVirtualColumn("j0.unnest", "\"arrayLongNulls\"", ColumnType.LONG_ARRAY),
                                    null
                                )
                            )
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("j0.unnest", "d0", ColumnType.LONG)
                                )
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{NullHandling.defaultLongValue(), 5L},
                new Object[]{1L, 5L},
                new Object[]{2L, 6L},
                new Object[]{3L, 6L},
                new Object[]{9L, 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("longs", ColumnType.LONG)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayLongNullsFiltered()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "arrayLongNulls, "
            + "SUM(cnt), "
            + "SUM(ARRAY_LENGTH(arrayLongNulls)) "
            + "FROM druid.arrays "
            + "WHERE ARRAY_CONTAINS(arrayLongNulls, 1) "
            + "GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("arrayLongNulls", "d0", ColumnType.LONG_ARRAY)
                                )
                            )
                            .setVirtualColumns(
                                new ExpressionVirtualColumn("v0", "array_length(\"arrayLongNulls\")", ColumnType.LONG, queryFramework().macroTable())
                            )
                            .setDimFilter(
                                new ExpressionDimFilter("array_contains(\"arrayLongNulls\",1)", queryFramework().macroTable())
                            )
                            .setAggregatorSpecs(
                                aggregators(
                                    new LongSumAggregatorFactory("a0", "cnt"),
                                    new LongSumAggregatorFactory("a1", "v0")
                                )
                            )
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{Collections.singletonList(1L), 1L, 1L},
                new Object[]{Arrays.asList(1L, null, 3L), 2L, 6L},
                new Object[]{Arrays.asList(1L, 2L, 3L), 2L, 6L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("arrayLongNulls", ColumnType.LONG_ARRAY)
                        .add("EXPR$1", ColumnType.LONG)
                        .add("EXPR$2", ColumnType.LONG)
                        .build()
        )
        .run();
  }


  @Test
  public void testGroupByRootSingleTypeArrayLongNullsFilteredMore()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "arrayLongNulls, "
            + "SUM(cnt) "
            + "FROM druid.arrays WHERE ARRAY_CONTAINS(arrayLongNulls, 1) OR ARRAY_OVERLAP(arrayLongNulls, ARRAY[2, 3]) GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("arrayLongNulls", "d0", ColumnType.LONG_ARRAY)
                                )
                            )
                            .setDimFilter(
                                or(
                                    expressionFilter("array_contains(\"arrayLongNulls\",1)"),
                                    expressionFilter("array_overlap(\"arrayLongNulls\",array(2,3))")
                                )
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{Arrays.asList(null, 2L, 9L), 2L},
                new Object[]{Collections.singletonList(1L), 1L},
                new Object[]{Arrays.asList(1L, null, 3L), 2L},
                new Object[]{Arrays.asList(1L, 2L, 3L), 2L},
                new Object[]{Arrays.asList(2L, 3L), 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("arrayLongNulls", ColumnType.LONG_ARRAY)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayString()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "arrayString, "
            + "SUM(cnt) "
            + "FROM druid.arrays GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("arrayString", "d0", ColumnType.STRING_ARRAY)
                                )
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{null, 4L},
                new Object[]{Arrays.asList("a", "b"), 4L},
                new Object[]{Arrays.asList("a", "b", "c"), 2L},
                new Object[]{Arrays.asList("b", "c"), 2L},
                new Object[]{Arrays.asList("d", "e"), 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("arrayString", ColumnType.STRING_ARRAY)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayStringNulls()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "arrayStringNulls, "
            + "SUM(cnt) "
            + "FROM druid.arrays GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("arrayStringNulls", "d0", ColumnType.STRING_ARRAY)
                                )
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{null, 3L},
                new Object[]{Collections.emptyList(), 1L},
                new Object[]{Collections.singletonList(null), 1L},
                new Object[]{Arrays.asList(null, "b"), 2L},
                new Object[]{Arrays.asList("a", "b"), 3L},
                new Object[]{Arrays.asList("b", "b"), 2L},
                new Object[]{Arrays.asList("d", null, "b"), 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("arrayStringNulls", ColumnType.STRING_ARRAY)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayStringNullsUnnest()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "strings, "
            + "SUM(cnt) "
            + "FROM druid.arrays, unnest(arrayStringNulls) as u (strings) GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(
                                UnnestDataSource.create(
                                    TableDataSource.create(DATA_SOURCE_ARRAYS),
                                    expressionVirtualColumn("j0.unnest", "\"arrayStringNulls\"", ColumnType.STRING_ARRAY),
                                    null
                                )
                            )
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("j0.unnest", "d0", ColumnType.STRING)
                                )
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{NullHandling.defaultStringValue(), 5L},
                new Object[]{"a", 3L},
                new Object[]{"b", 11L},
                new Object[]{"d", 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("strings", ColumnType.STRING)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayStringNullsFiltered()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "arrayStringNulls, "
            + "SUM(cnt), "
            + "SUM(ARRAY_LENGTH(arrayStringNulls)) "
            + "FROM druid.arrays "
            + "WHERE ARRAY_CONTAINS(arrayStringNulls, 'b') "
            + "GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("arrayStringNulls", "d0", ColumnType.STRING_ARRAY)
                                )
                            )
                            .setVirtualColumns(
                                new ExpressionVirtualColumn("v0", "array_length(\"arrayStringNulls\")", ColumnType.LONG, queryFramework().macroTable())
                            )
                            .setDimFilter(
                                new ExpressionDimFilter("array_contains(\"arrayStringNulls\",'b')", queryFramework().macroTable())
                            )
                            .setAggregatorSpecs(
                                aggregators(
                                    new LongSumAggregatorFactory("a0", "cnt"),
                                    new LongSumAggregatorFactory("a1", "v0")
                                )
                            )
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{Arrays.asList(null, "b"), 2L, 4L},
                new Object[]{Arrays.asList("a", "b"), 3L, 6L},
                new Object[]{Arrays.asList("b", "b"), 2L, 4L},
                new Object[]{Arrays.asList("d", null, "b"), 2L, 6L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("arrayStringNulls", ColumnType.STRING_ARRAY)
                        .add("EXPR$1", ColumnType.LONG)
                        .add("EXPR$2", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayDouble()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "arrayDouble, "
            + "SUM(cnt) "
            + "FROM druid.arrays GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("arrayDouble", "d0", ColumnType.DOUBLE_ARRAY)
                                )
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{null, 4L},
                new Object[]{Arrays.asList(1.1, 2.2, 3.3), 4L},
                new Object[]{Arrays.asList(1.1, 3.3), 2L},
                new Object[]{Arrays.asList(2.2, 3.3, 4.0), 2L},
                new Object[]{Arrays.asList(3.3, 4.4, 5.5), 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("arrayDouble", ColumnType.DOUBLE_ARRAY)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayDoubleNulls()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "arrayDoubleNulls, "
            + "SUM(cnt) "
            + "FROM druid.arrays GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("arrayDoubleNulls", "d0", ColumnType.DOUBLE_ARRAY)
                                )
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{null, 3L},
                new Object[]{Collections.emptyList(), 1L},
                new Object[]{Collections.singletonList(null), 1L},
                new Object[]{Arrays.asList(null, 1.1), 1L},
                new Object[]{Arrays.asList(null, 2.2, null), 2L},
                new Object[]{Arrays.asList(1.1, 2.2, null), 2L},
                new Object[]{Arrays.asList(999.0, null, 5.5), 2L},
                new Object[]{Arrays.asList(999.0, 5.5, null), 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("arrayDoubleNulls", ColumnType.DOUBLE_ARRAY)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayDoubleNullsUnnest()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "doubles, "
            + "SUM(cnt) "
            + "FROM druid.arrays, UNNEST(arrayDoubleNulls) as u (doubles) GROUP BY doubles"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(
                                UnnestDataSource.create(
                                    TableDataSource.create(DATA_SOURCE_ARRAYS),
                                    expressionVirtualColumn("j0.unnest", "\"arrayDoubleNulls\"", ColumnType.DOUBLE_ARRAY),
                                    null
                                )
                            )
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("j0.unnest", "d0", ColumnType.DOUBLE)
                                )
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{NullHandling.defaultDoubleValue(), 12L},
                new Object[]{1.1D, 3L},
                new Object[]{2.2D, 4L},
                new Object[]{5.5D, 4L},
                new Object[]{999.0D, 4L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("doubles", ColumnType.DOUBLE)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayDoubleNullsFiltered()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "arrayDoubleNulls, "
            + "SUM(cnt), "
            + "SUM(ARRAY_LENGTH(arrayDoubleNulls)) "
            + "FROM druid.arrays "
            + "WHERE ARRAY_CONTAINS(arrayDoubleNulls, 2.2)"
            + "GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("arrayDoubleNulls", "d0", ColumnType.DOUBLE_ARRAY)
                                )
                            )
                            .setVirtualColumns(
                                new ExpressionVirtualColumn("v0", "array_length(\"arrayDoubleNulls\")", ColumnType.LONG, queryFramework().macroTable())
                            )
                            .setDimFilter(
                                new ExpressionDimFilter("array_contains(\"arrayDoubleNulls\",2.2)", queryFramework().macroTable())
                            )
                            .setAggregatorSpecs(
                                aggregators(
                                    new LongSumAggregatorFactory("a0", "cnt"),
                                    new LongSumAggregatorFactory("a1", "v0")
                                )
                            )
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{Arrays.asList(null, 2.2, null), 2L, 6L},
                new Object[]{Arrays.asList(1.1, 2.2, null), 2L, 6L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("arrayDoubleNulls", ColumnType.DOUBLE_ARRAY)
                        .add("EXPR$1", ColumnType.LONG)
                        .add("EXPR$2", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayLongElement()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "JSON_VALUE(arrayLong, '$[1]' RETURNING BIGINT),"
            + "SUM(cnt) "
            + "FROM druid.arrays GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)
                                )
                            )
                            .setVirtualColumns(
                                new NestedFieldVirtualColumn("arrayLong", "$[1]", "v0", ColumnType.LONG)
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{NullHandling.defaultLongValue(), 4L},
                new Object[]{2L, 6L},
                new Object[]{3L, 2L},
                new Object[]{4L, 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("EXPR$0", ColumnType.LONG)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayLongElementFiltered()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "JSON_VALUE(arrayLong, '$[1]' RETURNING BIGINT),"
            + "SUM(cnt) "
            + "FROM druid.arrays "
            + "WHERE JSON_VALUE(arrayLong, '$[1]' RETURNING BIGINT) = 2"
            + "GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)
                                )
                            )
                            .setVirtualColumns(
                                new NestedFieldVirtualColumn("arrayLong", "$[1]", "v0", ColumnType.LONG)
                            )
                            .setDimFilter(new SelectorDimFilter("v0", "2", null))
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{2L, 6L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("EXPR$0", ColumnType.LONG)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayLongElementDefault()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "JSON_VALUE(arrayLong, '$[1]'),"
            + "SUM(cnt) "
            + "FROM druid.arrays GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)
                                )
                            )
                            .setVirtualColumns(
                                new NestedFieldVirtualColumn("arrayLong", "$[1]", "v0", ColumnType.STRING)
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{NullHandling.defaultStringValue(), 4L},
                new Object[]{"2", 6L},
                new Object[]{"3", 2L},
                new Object[]{"4", 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("EXPR$0", ColumnType.STRING)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayStringElement()
  {
    testBuilder()
        .sql(
            "SELECT "
            + "JSON_VALUE(arrayStringNulls, '$[1]'),"
            + "SUM(cnt) "
            + "FROM druid.arrays GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)
                                )
                            )
                            .setVirtualColumns(
                                new NestedFieldVirtualColumn("arrayStringNulls", "$[1]", "v0", ColumnType.STRING)
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{NullHandling.defaultStringValue(), 7L},
                new Object[]{"b", 7L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("EXPR$0", ColumnType.STRING)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayStringElementFiltered()
  {
    testBuilder()
        .sql(
            "SELECT "
            + "JSON_VALUE(arrayStringNulls, '$[1]'),"
            + "SUM(cnt) "
            + "FROM druid.arrays "
            + "WHERE JSON_VALUE(arrayStringNulls, '$[1]') = 'b'"
            + "GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)
                                )
                            )
                            .setVirtualColumns(
                                new NestedFieldVirtualColumn("arrayStringNulls", "$[1]", "v0", ColumnType.STRING)
                            )
                            .setDimFilter(new SelectorDimFilter("v0", "b", null))
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{"b", 7L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("EXPR$0", ColumnType.STRING)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayDoubleElement()
  {
    testBuilder()
        .sql(
            "SELECT "
            + "JSON_VALUE(arrayDoubleNulls, '$[2]' RETURNING DOUBLE),"
            + "SUM(cnt) "
            + "FROM druid.arrays GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("v0", "d0", ColumnType.DOUBLE)
                                )
                            )
                            .setVirtualColumns(
                                new NestedFieldVirtualColumn("arrayDoubleNulls", "$[2]", "v0", ColumnType.DOUBLE)
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{NullHandling.defaultDoubleValue(), 12L},
                new Object[]{5.5, 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("EXPR$0", ColumnType.DOUBLE)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  @Test
  public void testGroupByRootSingleTypeArrayDoubleElementFiltered()
  {
    testBuilder()
        .sql(
            "SELECT "
            + "JSON_VALUE(arrayDoubleNulls, '$[2]' RETURNING DOUBLE),"
            + "SUM(cnt) "
            + "FROM druid.arrays "
            + "WHERE JSON_VALUE(arrayDoubleNulls, '$[2]' RETURNING DOUBLE) = 5.5"
            + "GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("v0", "d0", ColumnType.DOUBLE)
                                )
                            )
                            .setDimFilter(new SelectorDimFilter("v0", "5.5", null))
                            .setVirtualColumns(
                                new NestedFieldVirtualColumn("arrayDoubleNulls", "$[2]", "v0", ColumnType.DOUBLE)
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{5.5, 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("EXPR$0", ColumnType.DOUBLE)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }


  @Test
  public void testGroupByJsonValues()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "JSON_VALUE(nest, '$[''x'']'), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1, 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v0", "d1")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), NullHandling.defaultStringValue(), 4L},
            new Object[]{"100", "100", 2L},
            new Object[]{"200", "200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.STRING)
                    .add("EXPR$2", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathSelectorFilter()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.x') = '100' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(selector("v0", "100", null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "100",
                2L
            }
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathSelectorFilterCoalesce()
  {
    cannotVectorize();
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE COALESCE(JSON_VALUE(nest, '$.x'), '0') = '100' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(
                            expressionFilter(
                                "case_searched(notnull(json_value(\"nest\",'$.x', 'STRING')),(json_value(\"nest\",'$.x', 'STRING') == '100'),0)"
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "100",
                2L
            }
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testJsonAndArrayAgg()
  {
    cannotVectorize();
    testQuery(
        "SELECT "
        + "string, "
        + "ARRAY_AGG(nest, 16384), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("string", "d0")
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new ExpressionLambdaAggregatorFactory(
                                    "a0",
                                    ImmutableSet.of("nest"),
                                    "__acc",
                                    "ARRAY<COMPLEX<json>>[]",
                                    "ARRAY<COMPLEX<json>>[]",
                                    true,
                                    true,
                                    false,
                                    "array_append(\"__acc\", \"nest\")",
                                    "array_concat(\"__acc\", \"a0\")",
                                    null,
                                    null,
                                    HumanReadableBytes.valueOf(16384),
                                    queryFramework().macroTable()
                                ),
                                new LongSumAggregatorFactory("a1", "cnt")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "aaa",
                "[{\"x\":100,\"y\":2.02,\"z\":\"300\",\"mixed\":1,\"mixed2\":\"1\"},{\"x\":100,\"y\":2.02,\"z\":\"400\",\"mixed2\":1.1}]",
                2L
            },
            new Object[]{
                "bbb",
                "[null]",
                1L
            },
            new Object[]{
                "ccc",
                "[{\"x\":200,\"y\":3.03,\"z\":\"abcdef\",\"mixed\":1.1,\"mixed2\":1}]",
                1L
            },
            new Object[]{
                "ddd",
                "[null,null]",
                2L
            },
            new Object[]{
                "eee",
                "[null]",
                1L
            }
        ),
        RowSignature.builder()
                    .add("string", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.ofArray(ColumnType.NESTED_DATA))
                    .add("EXPR$2", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathSelectorFilterLong()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.x') = 100 GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.LONG),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(selector("v0", "100", null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "100",
                2L
            }
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathSelectorFilterDouble()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.y') = 2.02 GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.y", "v0", ColumnType.DOUBLE),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(selector("v0", "2.02", null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "100",
                2L
            }
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathSelectorFilterString()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.z') = '400' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.z", "v0", ColumnType.STRING),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(selector("v0", "400", null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "100",
                1L
            }
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathSelectorFilterVariant()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nester, '$.n.x') = 1 GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nester", "$.n.x", "v0", ColumnType.LONG),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(selector("v0", "1", null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(new Object[]{"100", 1L}),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathSelectorFilterVariant2()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.mixed2') = '1' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.mixed2", "v0", ColumnType.STRING),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(selector("v0", "1", null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 1L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathSelectorFilterVariant3()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.mixed2') in ('1', '1.1') GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.mixed2", "v0", ColumnType.STRING),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(in("v0", ImmutableList.of("1", "1.1"), null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 2L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathSelectorFilterNonExistent()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.missing') = 'no way' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.missing", "v0", ColumnType.STRING),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(selector("v0", "no way", null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathSelectorFilterNull()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.z') IS NOT NULL GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.z", "v0", ColumnType.STRING),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(not(selector("v0", null, null)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 2L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathBoundFilterLong()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.x') >= '100' AND JSON_VALUE(nest, '$.x') <= '300' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", "100", "300", false, false, null, StringComparators.LEXICOGRAPHIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 2L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathBoundFilterLongNoUpper()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.x') >= '100' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", "100", null, false, false, null, StringComparators.LEXICOGRAPHIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 2L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathBoundFilterLongNoLower()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.x') <= '100' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", null, "100", false, false, null, StringComparators.LEXICOGRAPHIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 2L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathBoundFilterLongNumeric()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.x') >= 100 AND JSON_VALUE(nest, '$.x') <= 300 GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.LONG),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", "100", "300", false, false, null, StringComparators.NUMERIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 2L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathBoundFilterLongNoUpperNumeric()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.x') >= 100 GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.LONG),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", "100", null, false, false, null, StringComparators.NUMERIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 2L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathNumericBoundFilterLongNoUpperNumeric()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x' RETURNING BIGINT),"
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.x' RETURNING BIGINT) >= 100 GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.LONG)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)
                            )
                        )
                        .setDimFilter(bound("v0", "100", null, false, false, null, StringComparators.NUMERIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{100L, 2L},
            new Object[]{200L, 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.LONG)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathBoundFilterLongNoLowerNumeric()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.x') <= 100 GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.LONG),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", null, "100", false, false, null, StringComparators.NUMERIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 4L},
            new Object[]{"100", 2L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathBoundFilterDouble()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.y'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.y') >= '1.01' AND JSON_VALUE(nest, '$.y') <= '3.03' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.y", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", "1.01", "3.03", false, false, null, StringComparators.LEXICOGRAPHIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"2.02", 2L},
            new Object[]{"3.03", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathBoundFilterDoubleNoUpper()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.y'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.y') >= '1.01' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.y", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", "1.01", null, false, false, null, StringComparators.LEXICOGRAPHIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"2.02", 2L},
            new Object[]{"3.03", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathBoundFilterDoubleNoLower()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.y'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.y') <= '2.02' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.y", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", null, "2.02", false, false, null, StringComparators.LEXICOGRAPHIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"2.02", 2L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathBoundDoubleFilterNumeric()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.y'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.y') >= 2.0 AND JSON_VALUE(nest, '$.y') <= 3.5 GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.y", "v0", ColumnType.DOUBLE),
                            new NestedFieldVirtualColumn("nest", "$.y", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", "2.0", "3.5", false, false, null, StringComparators.NUMERIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"2.02", 2L},
            new Object[]{"3.03", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathBoundFilterDoubleNoUpperNumeric()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.y'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.y') >= 1.0 GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.y", "v0", ColumnType.DOUBLE),
                            new NestedFieldVirtualColumn("nest", "$.y", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", "1.0", null, false, false, null, StringComparators.NUMERIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"2.02", 2L},
            new Object[]{"3.03", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathBoundFilterDoubleNoLowerNumeric()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.y'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.y') <= 2.02 GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.y", "v0", ColumnType.DOUBLE),
                            new NestedFieldVirtualColumn("nest", "$.y", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", null, "2.02", false, false, null, StringComparators.NUMERIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 4L},
            new Object[]{"2.02", 2L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathBoundFilterString()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.x') >= '100' AND JSON_VALUE(nest, '$.x') <= '300' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", "100", "300", false, false, null, StringComparators.LEXICOGRAPHIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 2L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathBoundFilterStringNoUpper()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.z') >= '400' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.z", "v0", ColumnType.STRING),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", "400", null, false, false, null, StringComparators.LEXICOGRAPHIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 1L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathBoundFilterStringNoLower()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.z') <= '400' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.z", "v0", ColumnType.STRING),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", null, "400", false, false, null, StringComparators.LEXICOGRAPHIC))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 4L},
            new Object[]{"100", 2L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathLikeFilter()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.x') LIKE '10%' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(new LikeDimFilter("v0", "10%", null, null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 2L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathLikeFilterStringPrefix()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.z') LIKE '30%' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.z", "v0", ColumnType.STRING),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(new LikeDimFilter("v0", "30%", null, null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathLikeFilterString()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.z') LIKE '%00%' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.z", "v0", ColumnType.STRING),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(new LikeDimFilter("v0", "%00%", null, null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 2L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathLikeFilterVariant()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nester, '$.n.x') LIKE '%ell%' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nester", "$.n.x", "v0", ColumnType.STRING),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(new LikeDimFilter("v0", "%ell%", null, null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathInFilter()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.x') in (100, 200) GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.LONG),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(new InDimFilter("v0", ImmutableSet.of("100", "200")))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 2L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathInFilterDouble()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.y') in (2.02, 3.03) GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.y", "v0", ColumnType.DOUBLE),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(new InDimFilter("v0", ImmutableSet.of("2.02", "3.03")))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 2L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathInFilterString()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nest, '$.z') in ('300', 'abcdef') GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nest", "$.z", "v0", ColumnType.STRING),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(new InDimFilter("v0", ImmutableSet.of("300", "abcdef")))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 1L},
            new Object[]{"200", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathInFilterVariant()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nester, '$.n.x') in ('hello', 1) GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nester", "$.n.x", "v0", ColumnType.STRING),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0")
                            )
                        )
                        .setDimFilter(new InDimFilter("v0", ImmutableSet.of("hello", "1")))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 2L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testSumPath()
  {
    testQuery(
        "SELECT "
        + "SUM(JSON_VALUE(nest, '$.x')) "
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.DOUBLE))
                  .aggregators(aggregators(new DoubleSumAggregatorFactory("a0", "v0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{400.0}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.DOUBLE)
                    .build()
    );
  }


  @Test
  public void testSumPathFilteredAggDouble()
  {
    // this one actually equals 2.1 because the filter is a long so double is cast and is 1 so both rows match
    testQuery(
        "SELECT "
        + "SUM(JSON_VALUE(nest, '$.x')) FILTER(WHERE((JSON_VALUE(nest, '$.y'))=2.02))"
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new NestedFieldVirtualColumn("nest", "$.y", "v0", ColumnType.DOUBLE),
                      new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.DOUBLE)
                  )

                  .aggregators(
                      aggregators(
                          new FilteredAggregatorFactory(
                              new DoubleSumAggregatorFactory("a0", "v1"),
                              selector("v0", "2.02", null)
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{200.0}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.DOUBLE)
                    .build()
    );
  }

  @Test
  public void testSumPathFilteredAggString()
  {
    testQuery(
        "SELECT "
        + "SUM(JSON_VALUE(nest, '$.x')) FILTER(WHERE((JSON_VALUE(nest, '$.z'))='300'))"
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new NestedFieldVirtualColumn("nest", "$.z", "v0", ColumnType.STRING),
                      new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.DOUBLE)
                  )

                  .aggregators(
                      aggregators(
                          new FilteredAggregatorFactory(
                              new DoubleSumAggregatorFactory("a0", "v1"),
                              selector("v0", "300", null)
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{100.0}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.DOUBLE)
                    .build()
    );
  }

  @Test
  public void testSumPathMixed()
  {
    // throws a "Cannot make vector value selector for variant typed nested field [[LONG, DOUBLE]]"
    skipVectorize();
    testQuery(
        "SELECT "
        + "SUM(JSON_VALUE(nest, '$.mixed')) "
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(new NestedFieldVirtualColumn("nest", "$.mixed", "v0", ColumnType.DOUBLE))
                  .aggregators(aggregators(new DoubleSumAggregatorFactory("a0", "v0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2.1}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.DOUBLE)
                    .build()
    );
  }

  @Test
  public void testSumPathMixedFilteredAggLong()
  {
    // throws a "Cannot make vector value selector for variant typed nested field [[LONG, DOUBLE]]"
    skipVectorize();
    // this one actually equals 2.1 because the filter is a long so double is cast and is 1 so both rows match
    testQuery(
        "SELECT "
        + "SUM(JSON_VALUE(nest, '$.mixed')) FILTER(WHERE((JSON_VALUE(nest, '$.mixed'))=1))"
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new NestedFieldVirtualColumn("nest", "$.mixed", "v0", ColumnType.LONG),
                      new NestedFieldVirtualColumn("nest", "$.mixed", "v1", ColumnType.DOUBLE)
                  )

                  .aggregators(
                      aggregators(
                          new FilteredAggregatorFactory(
                              new DoubleSumAggregatorFactory("a0", "v1"),
                              selector("v0", "1", null)
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2.1}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.DOUBLE)
                    .build()
    );
  }

  @Test
  public void testSumPathMixedFilteredAggDouble()
  {
    // throws a "Cannot make vector value selector for variant typed nested field [[LONG, DOUBLE]]"
    skipVectorize();
    // with double matcher, only the one row matches since the long value cast is not picked up
    testQuery(
        "SELECT "
        + "SUM(JSON_VALUE(nest, '$.mixed')) FILTER(WHERE((JSON_VALUE(nest, '$.mixed'))=1.1))"
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(new NestedFieldVirtualColumn("nest", "$.mixed", "v0", ColumnType.DOUBLE))
                  .aggregators(
                      aggregators(
                          new FilteredAggregatorFactory(
                              new DoubleSumAggregatorFactory("a0", "v0"),
                              selector("v0", "1.1", null)
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1.1}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.DOUBLE)
                    .build()
    );
  }

  @Test
  public void testCastAndSumPath()
  {
    testQuery(
        "SELECT "
        + "SUM(CAST(JSON_VALUE(nest, '$.x') as BIGINT)) "
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.LONG))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "v0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{400L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.LONG)
                    .build()
    );
  }


  @Test
  public void testCastAndSumPathStrings()
  {
    testQuery(
        "SELECT "
        + "SUM(CAST(JSON_VALUE(nest, '$.z') as BIGINT)) "
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(new NestedFieldVirtualColumn("nest", "$.z", "v0", ColumnType.LONG))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "v0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{700L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testReturningAndSumPath()
  {
    testQuery(
        "SELECT "
        + "SUM(JSON_VALUE(nest, '$.x' RETURNING BIGINT)) "
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.LONG))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "v0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{400L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testReturningAndSumPathWithMaths()
  {
    testQuery(
        "SELECT "
        + "SUM(JSON_VALUE(nest, '$.x' RETURNING BIGINT) / 100) "
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "(\"v1\" / 100)", ColumnType.LONG),
                      new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.LONG)
                  )
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "v0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{4L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testReturningAndSumPathDouble()
  {
    testQuery(
        "SELECT "
        + "SUM(JSON_VALUE(nest, '$.x' RETURNING DOUBLE)) "
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.DOUBLE))
                  .aggregators(aggregators(new DoubleSumAggregatorFactory("a0", "v0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{400.0}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.DOUBLE)
                    .build()
    );
  }

  @Test
  public void testReturningAndSumPathDecimal()
  {
    testQuery(
        "SELECT "
        + "SUM(JSON_VALUE(nest, '$.x' RETURNING DECIMAL)) "
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(new NestedFieldVirtualColumn("nest", "$.x", "v0", ColumnType.DOUBLE))
                  .aggregators(aggregators(new DoubleSumAggregatorFactory("a0", "v0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{400.0}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.DOUBLE)
                    .build()
    );
  }

  @Test
  public void testReturningAndSumPathDecimalWithMaths()
  {
    testQuery(
        "SELECT "
        + "SUM(JSON_VALUE(nest, '$.x' RETURNING DECIMAL) / 100.0) "
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "(\"v1\" / 100.0)", ColumnType.DOUBLE),
                      new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.DOUBLE)
                  )
                  .aggregators(aggregators(new DoubleSumAggregatorFactory("a0", "v0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{4.0}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.DOUBLE)
                    .build()
    );
  }

  @Test
  public void testReturningAndSumPathStrings()
  {
    testQuery(
        "SELECT "
        + "SUM(JSON_VALUE(nest, '$.z' RETURNING BIGINT)) "
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(new NestedFieldVirtualColumn("nest", "$.z", "v0", ColumnType.LONG))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "v0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{700L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRootKeys()
  {
    cannotVectorize();
    testQuery(
        "SELECT "
        + "JSON_KEYS(nester, '$'), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new ExpressionVirtualColumn(
                                "v0",
                                "json_keys(\"nester\",'$')",
                                ColumnType.STRING_ARRAY,
                                queryFramework().macroTable()
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.STRING_ARRAY)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{null, 5L},
            new Object[]{"[\"array\",\"n\"]", 2L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING_ARRAY)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRootKeysJsonPath()
  {
    cannotVectorize();
    testQuery(
        "SELECT "
        + "JSON_KEYS(nester, '$.'), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new ExpressionVirtualColumn(
                                "v0",
                                "json_keys(\"nester\",'$.')",
                                ColumnType.STRING_ARRAY,
                                queryFramework().macroTable()
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.STRING_ARRAY)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{null, 5L},
            new Object[]{"[\"array\",\"n\"]", 2L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING_ARRAY)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRootKeys2()
  {
    cannotVectorize();
    testQuery(
        "SELECT "
        + "JSON_KEYS(nest, '$'), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new ExpressionVirtualColumn(
                                "v0",
                                "json_keys(\"nest\",'$')",
                                ColumnType.STRING_ARRAY,
                                queryFramework().macroTable()
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.STRING_ARRAY)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{null, 4L},
            new Object[]{"[\"x\",\"y\",\"z\",\"mixed\",\"mixed2\"]", 2L},
            new Object[]{"[\"x\",\"y\",\"z\",\"mixed2\"]", 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING_ARRAY)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByAllPaths()
  {
    cannotVectorize();
    testQuery(
        "SELECT "
        + "JSON_PATHS(nester), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new ExpressionVirtualColumn(
                                "v0",
                                "json_paths(\"nester\")",
                                ColumnType.STRING_ARRAY,
                                queryFramework().macroTable()
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.STRING_ARRAY)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"$\"]", 5L},
            new Object[]{"[\"$.array\",\"$.n.x\"]", 2L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING_ARRAY)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByNestedArrayPath()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nester, '$.array[1]'), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nester", "$.array[1]", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 5L},
            new Object[]{"b", 2L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByInvalidPath()
  {
    testQueryThrows(
        "SELECT "
        + "JSON_VALUE(nester, '.array.[1]'), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1",
        (expected) -> {
          expected.expect(UnsupportedSQLQueryException.class);
          expected.expectMessage(
              "Cannot use [JSON_VALUE_VARCHAR]: [Bad format, '.array.[1]' is not a valid JSONPath path: must start with '$']");
        }
    );
  }

  @Test
  public void testJsonQuery()
  {
    testQuery(
        "SELECT JSON_QUERY(nester, '$.n'), JSON_QUERY(nester, '$')\n"
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      new NestedFieldVirtualColumn(
                          "nester",
                          "v0",
                          ColumnType.NESTED_DATA,
                          null,
                          true,
                          "$.n",
                          false
                      ),
                      new NestedFieldVirtualColumn(
                          "nester",
                          "v1",
                          ColumnType.NESTED_DATA,
                          null,
                          true,
                          "$.",
                          false
                      )
                  )
                  .columns("v0", "v1")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"{\"x\":\"hello\"}", "{\"array\":[\"a\",\"b\"],\"n\":{\"x\":\"hello\"}}"},
            new Object[]{null, "\"hello\""},
            new Object[]{null, null},
            new Object[]{null, null},
            new Object[]{null, null},
            new Object[]{"{\"x\":1}", "{\"array\":[\"a\",\"b\"],\"n\":{\"x\":1}}"},
            new Object[]{null, "2"}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.NESTED_DATA)
                    .add("EXPR$1", ColumnType.NESTED_DATA)
                    .build()

    );
  }

  @Test
  public void testJsonQueryAndJsonObject()
  {
    testQuery(
        "SELECT JSON_OBJECT(KEY 'n' VALUE JSON_QUERY(nester, '$.n'), KEY 'x' VALUE JSON_VALUE(nest, '$.x'))\n"
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "json_object('n',\"v1\",'x',\"v2\")",
                          ColumnType.NESTED_DATA,
                          queryFramework().macroTable()
                      ),
                      new NestedFieldVirtualColumn(
                          "nester",
                          "v1",
                          ColumnType.NESTED_DATA,
                          null,
                          true,
                          "$.n",
                          false
                      ),
                      new NestedFieldVirtualColumn("nest", "$.x", "v2", ColumnType.STRING)
                  )
                  .columns("v0")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"{\"x\":\"100\",\"n\":{\"x\":\"hello\"}}"},
            new Object[]{"{\"x\":null,\"n\":null}"},
            new Object[]{"{\"x\":\"200\",\"n\":null}"},
            new Object[]{"{\"x\":null,\"n\":null}"},
            new Object[]{"{\"x\":null,\"n\":null}"},
            new Object[]{"{\"x\":\"100\",\"n\":{\"x\":1}}"},
            new Object[]{"{\"x\":null,\"n\":null}"}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.NESTED_DATA)
                    .build()
    );
  }

  @Test
  public void testCompositionTyping()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE((JSON_OBJECT(KEY 'x' VALUE JSON_VALUE(nest, '$.x' RETURNING BIGINT))), '$.x' RETURNING BIGINT)\n"
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "json_value(json_object('x',\"v1\"),'$.x', 'LONG')",
                          ColumnType.LONG,
                          queryFramework().macroTable()
                      ),
                      new NestedFieldVirtualColumn(
                          "nest",
                          "v1",
                          ColumnType.LONG,
                          null,
                          false,
                          "$.x",
                          false
                      )
                  )
                  .columns("v0")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{100L},
            new Object[]{NullHandling.defaultLongValue()},
            new Object[]{200L},
            new Object[]{NullHandling.defaultLongValue()},
            new Object[]{NullHandling.defaultLongValue()},
            new Object[]{100L},
            new Object[]{NullHandling.defaultLongValue()}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testToJsonAndParseJson()
  {
    ExprMacroTable macroTable = queryFramework().macroTable();
    testQuery(
        "SELECT string, TRY_PARSE_JSON(TO_JSON_STRING(string)), PARSE_JSON('{\"foo\":1}'), PARSE_JSON(TO_JSON_STRING(nester))\n"
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "try_parse_json(to_json_string(\"string\"))",
                          ColumnType.NESTED_DATA,
                          macroTable
                      ),
                      new ExpressionVirtualColumn(
                          "v1",
                          "parse_json('{\\u0022foo\\u0022:1}')",
                          ColumnType.NESTED_DATA,
                          macroTable
                      ),
                      new ExpressionVirtualColumn(
                          "v2",
                          "parse_json(to_json_string(\"nester\"))",
                          ColumnType.NESTED_DATA,
                          macroTable
                      )
                  )
                  .columns("string", "v0", "v1", "v2")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                "aaa",
                "\"aaa\"",
                "{\"foo\":1}",
                "{\"array\":[\"a\",\"b\"],\"n\":{\"x\":\"hello\"}}"
            },
            new Object[]{"bbb", "\"bbb\"", "{\"foo\":1}", "\"hello\""},
            new Object[]{"ccc", "\"ccc\"", "{\"foo\":1}", null},
            new Object[]{"ddd", "\"ddd\"", "{\"foo\":1}", null},
            new Object[]{"eee", "\"eee\"", "{\"foo\":1}", null},
            new Object[]{"aaa", "\"aaa\"", "{\"foo\":1}", "{\"array\":[\"a\",\"b\"],\"n\":{\"x\":1}}"},
            new Object[]{"ddd", "\"ddd\"", "{\"foo\":1}", "2"}
        ),
        RowSignature.builder()
                    .add("string", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.NESTED_DATA)
                    .add("EXPR$2", ColumnType.NESTED_DATA)
                    .add("EXPR$3", ColumnType.NESTED_DATA)
                    .build()
    );
  }

  @Test
  public void testGroupByNegativeJsonPathIndex()
  {
    // negative array index cannot vectorize
    cannotVectorize();
    testQuery(
        "SELECT "
        + "JSON_VALUE(nester, '$.array[-1]'), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nester", "$.array[-1]", "v0", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 5L},
            new Object[]{"b", 2L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testJsonPathNegativeIndex()
  {
    testQuery(
        "SELECT JSON_VALUE(nester, '$.array[-1]'), JSON_QUERY(nester, '$.array[-1]'), JSON_KEYS(nester, '$.array[-1]') FROM druid.nested",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      new NestedFieldVirtualColumn(
                          "nester",
                          "v0",
                          ColumnType.STRING,
                          null,
                          false,
                          "$.array[-1]",
                          false
                      ),
                      new NestedFieldVirtualColumn(
                          "nester",
                          "v1",
                          ColumnType.NESTED_DATA,
                          null,
                          true,
                          "$.array[-1]",
                          false
                      ),
                      expressionVirtualColumn("v2", "json_keys(\"nester\",'$.array[-1]')", ColumnType.STRING_ARRAY)
                  )
                  .columns("v0", "v1", "v2")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"b", "\"b\"", null},
            new Object[]{NullHandling.defaultStringValue(), null, null},
            new Object[]{NullHandling.defaultStringValue(), null, null},
            new Object[]{NullHandling.defaultStringValue(), null, null},
            new Object[]{NullHandling.defaultStringValue(), null, null},
            new Object[]{"b", "\"b\"", null},
            new Object[]{NullHandling.defaultStringValue(), null, null}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.NESTED_DATA)
                    .add("EXPR$2", ColumnType.STRING_ARRAY)
                    .build()

    );
  }

  @Test
  public void testJsonPathsNonJsonInput()
  {
    testQuery(
        "SELECT JSON_PATHS(string), JSON_PATHS(1234), JSON_PATHS('1234'), JSON_PATHS(1.1), JSON_PATHS(null)\n"
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      expressionVirtualColumn("v0", "json_paths(\"string\")", ColumnType.STRING_ARRAY),
                      expressionVirtualColumn("v1", "array('$')", ColumnType.STRING_ARRAY)
                  )
                  .columns("v0", "v1")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"$\"]", "[\"$\"]", "[\"$\"]", "[\"$\"]", "[\"$\"]"},
            new Object[]{"[\"$\"]", "[\"$\"]", "[\"$\"]", "[\"$\"]", "[\"$\"]"},
            new Object[]{"[\"$\"]", "[\"$\"]", "[\"$\"]", "[\"$\"]", "[\"$\"]"},
            new Object[]{"[\"$\"]", "[\"$\"]", "[\"$\"]", "[\"$\"]", "[\"$\"]"},
            new Object[]{"[\"$\"]", "[\"$\"]", "[\"$\"]", "[\"$\"]", "[\"$\"]"},
            new Object[]{"[\"$\"]", "[\"$\"]", "[\"$\"]", "[\"$\"]", "[\"$\"]"},
            new Object[]{"[\"$\"]", "[\"$\"]", "[\"$\"]", "[\"$\"]", "[\"$\"]"}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING_ARRAY)
                    .add("EXPR$1", ColumnType.STRING_ARRAY)
                    .add("EXPR$2", ColumnType.STRING_ARRAY)
                    .add("EXPR$3", ColumnType.STRING_ARRAY)
                    .add("EXPR$4", ColumnType.STRING_ARRAY)
                    .build()

    );
  }

  @Test
  public void testJsonKeysNonJsonInput()
  {
    testQuery(
        "SELECT JSON_KEYS(string, '$'), JSON_KEYS(1234, '$'), JSON_KEYS('1234', '$'), JSON_KEYS(1.1, '$'), JSON_KEYS(null, '$')\n"
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      expressionVirtualColumn("v0", "json_keys(\"string\",'$')", ColumnType.STRING_ARRAY),
                      expressionVirtualColumn("v1", "null", ColumnType.STRING_ARRAY)
                  )
                  .columns("v0", "v1")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{null, null, null, null, null},
            new Object[]{null, null, null, null, null},
            new Object[]{null, null, null, null, null},
            new Object[]{null, null, null, null, null},
            new Object[]{null, null, null, null, null},
            new Object[]{null, null, null, null, null},
            new Object[]{null, null, null, null, null}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING_ARRAY)
                    .add("EXPR$1", ColumnType.STRING_ARRAY)
                    .add("EXPR$2", ColumnType.STRING_ARRAY)
                    .add("EXPR$3", ColumnType.STRING_ARRAY)
                    .add("EXPR$4", ColumnType.STRING_ARRAY)
                    .build()

    );
  }

  @Test
  public void testJsonValueUnDocumentedButSupportedOptions()
  {
    testQuery(
        "SELECT "
        + "SUM(JSON_VALUE(nest, '$.z' RETURNING BIGINT NULL ON EMPTY NULL ON ERROR)) "
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(new NestedFieldVirtualColumn("nest", "$.z", "v0", ColumnType.LONG))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "v0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{700L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testJsonValueUnsupportedOptions()
  {
    testQueryThrows(
        "SELECT "
        + "SUM(JSON_VALUE(nest, '$.z' RETURNING BIGINT ERROR ON EMPTY ERROR ON ERROR)) "
        + "FROM druid.nested",
        exception -> {
          expectedException.expect(IllegalArgumentException.class);
          expectedException.expectMessage(
              "Unsupported JSON_VALUE parameter 'ON EMPTY' defined - please re-issue this query without this argument"
          );
        }
    );
  }

  @Test
  public void testGroupByPathSelectorFilterVariantNull()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "JSON_VALUE(nester, '$.n.x' RETURNING BIGINT), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nester, '$.n.x' RETURNING BIGINT) IS NULL GROUP BY 1, 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nester", "$.n.x", "v0", ColumnType.LONG),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0"),
                                new DefaultDimensionSpec("v0", "d1", ColumnType.LONG)
                            )
                        )
                        .setDimFilter(selector("v0", null, null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), NullHandling.defaultLongValue(), 4L},
            new Object[]{"100", NullHandling.defaultLongValue(), 1L},
            new Object[]{"200", NullHandling.defaultLongValue(), 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .add("EXPR$2", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testSelectPathSelectorFilterVariantNull()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "JSON_VALUE(nester, '$.n.x' RETURNING BIGINT) "
        + "FROM druid.nested WHERE JSON_VALUE(nester, '$.n.x' RETURNING BIGINT) IS NULL",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      new NestedFieldVirtualColumn("nester", "$.n.x", "v0", ColumnType.LONG),
                      new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                  )
                  .columns(
                      "v0", "v1"
                  )
                  .filters(selector("v0", null, null))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"100", null},
            new Object[]{NullHandling.defaultStringValue(), null},
            new Object[]{"200", null},
            new Object[]{NullHandling.defaultStringValue(), null},
            new Object[]{NullHandling.defaultStringValue(), null},
            new Object[]{NullHandling.defaultStringValue(), null}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByPathSelectorFilterVariantNotNull()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(nest, '$.x'), "
        + "JSON_VALUE(nester, '$.n.x' RETURNING BIGINT), "
        + "SUM(cnt) "
        + "FROM druid.nested WHERE JSON_VALUE(nester, '$.n.x' RETURNING BIGINT) IS NOT NULL GROUP BY 1, 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new NestedFieldVirtualColumn("nester", "$.n.x", "v0", ColumnType.LONG),
                            new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "d0"),
                                new DefaultDimensionSpec("v0", "d1", ColumnType.LONG)
                            )
                        )
                        .setDimFilter(not(selector("v0", null, null)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"100", 1L, 1L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.STRING)
                    .add("EXPR$1", ColumnType.LONG)
                    .add("EXPR$2", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRegularLongLongMixed1FilterNotNull()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(long, '$' RETURNING BIGINT), "
        + "SUM(cnt) "
        + "FROM druid.nested_mix WHERE JSON_VALUE(long, '$' RETURNING BIGINT) IS NOT NULL GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE_MIXED)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)
                            )
                        )
                        .setVirtualColumns(new NestedFieldVirtualColumn("long", "$", "v0", ColumnType.LONG))
                        .setDimFilter(not(selector("v0", null, null)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 2L},
            new Object[]{2L, 4L},
            new Object[]{3L, 2L},
            new Object[]{4L, 2L},
            new Object[]{5L, 4L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.LONG)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRootSingleTypeStringMixed1SparseNotNull()
  {
    testQuery(
        "SELECT "
        + "JSON_VALUE(string_sparse, '$' RETURNING BIGINT), "
        + "SUM(cnt) "
        + "FROM druid.nested_mix_2 WHERE JSON_VALUE(string_sparse, '$' RETURNING BIGINT) IS NOT NULL GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE_MIXED_2)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)
                            )
                        )
                        .setVirtualColumns(new NestedFieldVirtualColumn("string_sparse", "$", "v0", ColumnType.LONG))
                        .setDimFilter(not(selector("v0", null, null)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{10L, 2L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.LONG)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testScanStringNotNullCast()
  {
    skipVectorize();
    testQuery(
        "SELECT "
        + "CAST(string_sparse as BIGINT)"
        + "FROM druid.nested_mix WHERE CAST(string_sparse as BIGINT) IS NOT NULL",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(DATA_SOURCE_MIXED)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      expressionVirtualColumn("v0", "CAST(\"string_sparse\", 'LONG')", ColumnType.LONG)
                  )
                  .filters(not(selector("v0", null, null)))
                  .columns("v0")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        NullHandling.sqlCompatible() ?
        ImmutableList.of(
            new Object[]{10L},
            new Object[]{10L}
        ) :
        ImmutableList.of(
            new Object[]{0L},
            new Object[]{0L},
            new Object[]{10L},
            new Object[]{0L},
            new Object[]{0L},
            new Object[]{0L},
            new Object[]{0L},
            new Object[]{0L},
            new Object[]{0L},
            new Object[]{10L},
            new Object[]{0L},
            new Object[]{0L},
            new Object[]{0L},
            new Object[]{0L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.LONG)
                    .build()
    );
  }

  @Test
  public void testGroupByRootSingleTypeStringMixed1SparseNotNullCast2()
  {
    testQuery(
        "SELECT "
        + "CAST(string_sparse as BIGINT), "
        + "SUM(cnt) "
        + "FROM druid.nested_mix WHERE CAST(string_sparse as BIGINT) IS NOT NULL GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE_MIXED)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("string_sparse", "d0", ColumnType.LONG)
                            )
                        )
                        .setVirtualColumns(expressionVirtualColumn("v0", "CAST(\"string_sparse\", 'LONG')", ColumnType.LONG))
                        .setDimFilter(not(selector("v0", null, null)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible() ?
        ImmutableList.of(
            new Object[]{10L, 2L}
        ) :
        ImmutableList.of(
            new Object[]{0L, 12L},
            new Object[]{10L, 2L}
        ),
        RowSignature.builder()
                    .add("EXPR$0", ColumnType.LONG)
                    .add("EXPR$1", ColumnType.LONG)
                    .build()
    );
  }

  /**
   * MVD version of {@link #testGroupByRootSingleTypeArrayLongNullsUnnest()}
   */
  @Test
  public void testGroupByRootSingleTypeArrayLongNullsAsMvd()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "ARRAY_TO_MV(arrayLongNulls), "
            + "SUM(cnt) "
            + "FROM druid.arrays GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(TableDataSource.create(DATA_SOURCE_ARRAYS))
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)
                                )
                            )
                            .setVirtualColumns(expressionVirtualColumn("v0", "array_to_mv(\"arrayLongNulls\")", ColumnType.STRING))
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                // implicit mvd unnest treats null and empty as [null] so we get extra null matches than unnest
                // directly on the ARRAY
                new Object[]{NullHandling.defaultStringValue(), 9L},
                new Object[]{"1", 5L},
                new Object[]{"2", 6L},
                new Object[]{"3", 6L},
                new Object[]{"9", 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("EXPR$0", ColumnType.STRING)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  /**
   * MVD version of {@link #testGroupByRootSingleTypeArrayLongNullsFiltered()}
   * - implicit unnest since it is an mvd instead of array grouping
   * - filters are adjusted to match strings instead of numbers
   */
  @Test
  public void testGroupByRootSingleTypeArrayLongNullsAsMvdFiltered()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "ARRAY_TO_MV(arrayLongNulls), "
            + "SUM(cnt), "
            + "SUM(MV_LENGTH(ARRAY_TO_MV(arrayLongNulls))) "
            + "FROM druid.arrays "
            + "WHERE MV_CONTAINS(ARRAY_TO_MV(arrayLongNulls), '1') "
            + "GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)
                                )
                            )
                            .setVirtualColumns(
                                expressionVirtualColumn("v0", "array_to_mv(\"arrayLongNulls\")", ColumnType.STRING),
                                expressionVirtualColumn("v1", "array_length(array_to_mv(\"arrayLongNulls\"))", ColumnType.LONG)
                            )
                            .setDimFilter(
                                new ExpressionDimFilter("array_contains(array_to_mv(\"arrayLongNulls\"),'1')", queryFramework().macroTable())
                            )
                            .setAggregatorSpecs(
                                aggregators(
                                    new LongSumAggregatorFactory("a0", "cnt"),
                                    new LongSumAggregatorFactory("a1", "v1")
                                )
                            )
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{NullHandling.defaultStringValue(), 2L, 6L},
                new Object[]{"1", 5L, 13L},
                new Object[]{"2", 2L, 6L},
                new Object[]{"3", 4L, 12L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("EXPR$0", ColumnType.STRING)
                        .add("EXPR$1", ColumnType.LONG)
                        .add("EXPR$2", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  /**
   * MVD version of {@link #testGroupByRootSingleTypeArrayLongNullsFilteredMore()}
   * - implicit unnest since it is an mvd instead of array grouping
   * - filters are adjusted to match strings instead of numbers
   */
  @Test
  public void testGroupByRootSingleTypeArrayLongNullsAsMvdFilteredMore()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "ARRAY_TO_MV(arrayLongNulls), "
            + "SUM(cnt) "
            + "FROM druid.arrays WHERE MV_CONTAINS(ARRAY_TO_MV(arrayLongNulls), '1') OR MV_OVERLAP(ARRAY_TO_MV(arrayLongNulls), ARRAY['2', '3']) GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)
                                )
                            )
                            .setVirtualColumns(
                                expressionVirtualColumn("v0", "array_to_mv(\"arrayLongNulls\")", ColumnType.STRING)
                            )
                            .setDimFilter(
                                or(
                                    expressionFilter("array_contains(array_to_mv(\"arrayLongNulls\"),'1')"),
                                    expressionFilter("array_overlap(array_to_mv(\"arrayLongNulls\"),array('2','3'))")
                                )
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            // since array is converted to a MVD, implicit unnesting occurs
            ImmutableList.of(
                new Object[]{NullHandling.defaultStringValue(), 4L},
                new Object[]{"1", 5L},
                new Object[]{"2", 6L},
                new Object[]{"3", 6L},
                new Object[]{"9", 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("EXPR$0", ColumnType.STRING)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  /**
   * MVD version of {@link #testGroupByRootSingleTypeArrayStringNullsUnnest()}
   */
  @Test
  public void testGroupByRootSingleTypeArrayStringNullsAsMvdUnnest()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "ARRAY_TO_MV(arrayStringNulls), "
            + "SUM(cnt) "
            + "FROM druid.arrays GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(
                                TableDataSource.create(DATA_SOURCE_ARRAYS)
                            )
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)
                                )
                            )
                            .setVirtualColumns(
                                expressionVirtualColumn("v0", "array_to_mv(\"arrayStringNulls\")", ColumnType.STRING)
                            )
                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                // count is 9 instead of 5 because implicit unnest treats null and empty as [null]
                new Object[]{NullHandling.defaultStringValue(), 9L},
                new Object[]{"a", 3L},
                new Object[]{"b", 11L},
                new Object[]{"d", 2L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("EXPR$0", ColumnType.STRING)
                        .add("EXPR$1", ColumnType.LONG)
                        .build()
        )
        .run();
  }

  /**
   * MVD version of {@link #testGroupByRootSingleTypeArrayStringNullsFiltered()}
   * - implicit unnest since mvd instead of string array
   */
  @Test
  public void testGroupByRootSingleTypeArrayStringNullsFilteredAsMvd()
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT "
            + "ARRAY_TO_MV(arrayStringNulls), "
            + "SUM(cnt), "
            + "SUM(MV_LENGTH(ARRAY_TO_MV(arrayStringNulls))) "
            + "FROM druid.arrays "
            + "WHERE MV_CONTAINS(ARRAY_TO_MV(arrayStringNulls), 'b') "
            + "GROUP BY 1"
        )
        .queryContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(DATA_SOURCE_ARRAYS)
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(
                                dimensions(
                                    new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)
                                )
                            )
                            .setVirtualColumns(
                                expressionVirtualColumn("v0", "array_to_mv(\"arrayStringNulls\")", ColumnType.STRING),
                                new ExpressionVirtualColumn("v1", "array_length(array_to_mv(\"arrayStringNulls\"))", ColumnType.LONG, queryFramework().macroTable())
                            )
                            .setDimFilter(
                                new ExpressionDimFilter("array_contains(array_to_mv(\"arrayStringNulls\"),'b')", queryFramework().macroTable())
                            )
                            .setAggregatorSpecs(
                                aggregators(
                                    new LongSumAggregatorFactory("a0", "cnt"),
                                    new LongSumAggregatorFactory("a1", "v1")
                                )
                            )
                            .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{NullHandling.defaultStringValue(), 4L, 10L},
                new Object[]{"a", 3L, 6L},
                new Object[]{"b", 11L, 24L},
                new Object[]{"d", 2L, 6L}
            )
        )
        .expectedSignature(
            RowSignature.builder()
                        .add("EXPR$0", ColumnType.STRING)
                        .add("EXPR$1", ColumnType.LONG)
                        .add("EXPR$2", ColumnType.LONG)
                        .build()
        )
        .run();
  }
}

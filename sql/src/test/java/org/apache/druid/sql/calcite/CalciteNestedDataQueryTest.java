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

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.NestedDataDimensionSchema;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.UnsupportedSQLQueryException;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CalciteNestedDataQueryTest extends BaseCalciteQueryTest
{
  private static final String DATA_SOURCE = "nested";

  private static final List<ImmutableMap<String, Object>> RAW_ROWS = ImmutableList.of(
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-01")
                  .put("string", "aaa")
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
                  .put("nest", ImmutableMap.of("x", 200L, "y", 3.03, "z", "abcdef", "mixed", 1.1, "mixed2", 1L))
                  .put("long", 3L)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-01")
                  .put("string", "ddd")
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

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec("t", "iso", null),
          DimensionsSpec.builder().setDimensions(
              ImmutableList.<DimensionSchema>builder()
                           .add(new StringDimensionSchema("string"))
                           .add(new NestedDataDimensionSchema("nest"))
                           .add(new NestedDataDimensionSchema("nester"))
                           .add(new LongDimensionSchema("long"))
                           .build()
          ).build()
      ));

  private static final List<InputRow> ROWS =
      RAW_ROWS.stream().map(raw -> TestDataBuilder.createRow(raw, PARSER)).collect(Collectors.toList());

  @Override
  public Iterable<? extends Module> getJacksonModules()
  {
    return Iterables.concat(
        super.getJacksonModules(),
        NestedDataModule.getJacksonModulesList()
    );
  }

  @SuppressWarnings("resource")
  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      QueryRunnerFactoryConglomerate conglomerate
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
                            .withDimensionsSpec(PARSER)
                            .withRollup(false)
                            .build()
                    )
                    .rows(ROWS)
                    .buildMMappedIndex();

    return new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        DataSegment.builder()
                   .dataSource(DATA_SOURCE)
                   .interval(index.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index
    );
  }

  @Override
  public ExprMacroTable createMacroTable()
  {
    ComplexMetrics.registerSerde(NestedDataComplexTypeSerde.TYPE_NAME, NestedDataComplexTypeSerde.INSTANCE);
    final List<ExprMacroTable.ExprMacro> exprMacros = new ArrayList<>();
    for (Class<? extends ExprMacroTable.ExprMacro> clazz : ExpressionModule.EXPR_MACROS) {
      exprMacros.add(CalciteTests.INJECTOR.getInstance(clazz));
    }
    exprMacros.add(CalciteTests.INJECTOR.getInstance(LookupExprMacro.class));
    return new ExprMacroTable(exprMacros);
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
            new Object[]{"[\"$.n.x\",\"$.array[0]\",\"$.array[1]\"]", 2L}
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
                          NestedDataComplexTypeSerde.TYPE,
                          null,
                          true,
                          "$.n",
                          false
                      ),
                      new NestedFieldVirtualColumn(
                          "nester",
                          "v1",
                          NestedDataComplexTypeSerde.TYPE,
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
                    .add("EXPR$0", NestedDataComplexTypeSerde.TYPE)
                    .add("EXPR$1", NestedDataComplexTypeSerde.TYPE)
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
                          NestedDataComplexTypeSerde.TYPE,
                          queryFramework().macroTable()
                      ),
                      new NestedFieldVirtualColumn(
                          "nester",
                          "v1",
                          NestedDataComplexTypeSerde.TYPE,
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
                    .add("EXPR$0", NestedDataComplexTypeSerde.TYPE)
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
                          createMacroTable()
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
                          NestedDataComplexTypeSerde.TYPE,
                          macroTable
                      ),
                      new ExpressionVirtualColumn(
                          "v1",
                          "parse_json('{\\u0022foo\\u0022:1}')",
                          NestedDataComplexTypeSerde.TYPE,
                          macroTable
                      ),
                      new ExpressionVirtualColumn(
                          "v2",
                          "parse_json(to_json_string(\"nester\"))",
                          NestedDataComplexTypeSerde.TYPE,
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
                    .add("EXPR$1", NestedDataComplexTypeSerde.TYPE)
                    .add("EXPR$2", NestedDataComplexTypeSerde.TYPE)
                    .add("EXPR$3", NestedDataComplexTypeSerde.TYPE)
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
                          NestedDataComplexTypeSerde.TYPE,
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
                    .add("EXPR$1", NestedDataComplexTypeSerde.TYPE)
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
}

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
      RAW_ROWS.stream().map(raw -> CalciteTests.createRow(raw, PARSER)).collect(Collectors.toList());

  private ExprMacroTable macroTable;

  @Override
  public Iterable<? extends Module> getJacksonModules()
  {
    return Iterables.concat(
        super.getJacksonModules(),
        NestedDataModule.getJacksonModulesList()
    );
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker() throws IOException
  {
    NestedDataModule.registerHandlersAndSerde();
    macroTable = createMacroTable();
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
  public void testGroupByPath() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByJsonValue() throws Exception
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
        )
    );
  }

  @Test
  public void testTopNPath() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByRootPath() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByGetPaths() throws Exception
  {
    testQuery(
        "SELECT "
        + "GET_PATH(nest, '.x'), "
        + "GET_PATH(nest, '.\"x\"'), "
        + "GET_PATH(nest, '.[\"x\"]'), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1, 2, 3",
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
                                new DefaultDimensionSpec("v0", "d1"),
                                new DefaultDimensionSpec("v0", "d2")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                NullHandling.defaultStringValue(),
                NullHandling.defaultStringValue(),
                NullHandling.defaultStringValue(),
                4L
            },
            new Object[]{"100", "100", "100", 2L},
            new Object[]{"200", "200", "200", 1L}
        )
    );
  }

  @Test
  public void testGroupByJsonGetPaths() throws Exception
  {
    testQuery(
        "SELECT "
        + "JSON_GET_PATH(nest, '.x'), "
        + "JSON_GET_PATH(nest, '.\"x\"'), "
        + "JSON_GET_PATH(nest, '.[\"x\"]'), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1, 2, 3",
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
                                new DefaultDimensionSpec("v0", "d1"),
                                new DefaultDimensionSpec("v0", "d2")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                NullHandling.defaultStringValue(),
                NullHandling.defaultStringValue(),
                NullHandling.defaultStringValue(),
                4L
            },
            new Object[]{"100", "100", "100", 2L},
            new Object[]{"200", "200", "200", 1L}
        )
    );
  }

  @Test
  public void testGroupByJsonValues() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathSelectorFilter() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathSelectorFilterLong() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathSelectorFilterDouble() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathSelectorFilterString() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathSelectorFilterVariant() throws Exception
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
        ImmutableList.of(new Object[]{"100", 1L})
    );
  }

  @Test
  public void testGroupByPathSelectorFilterVariant2() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathSelectorFilterVariant3() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathSelectorFilterNonExistent() throws Exception
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
        ImmutableList.of()
    );
  }

  @Test
  public void testGroupByPathSelectorFilterNull() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathBoundFilterLong() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathBoundFilterLongNoUpper() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathBoundFilterLongNoLower() throws Exception
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
            new Object[]{NullHandling.defaultStringValue(), 4L},
            new Object[]{"100", 2L}
        )
    );
  }

  @Test
  public void testGroupByPathBoundFilterLongNumeric() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathBoundFilterLongNoUpperNumeric() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathBoundFilterLongNoLowerNumeric() throws Exception
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
            new Object[]{"100", 2L}
        )
    );
  }

  @Test
  public void testGroupByPathBoundFilterDouble() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathBoundFilterDoubleNoUpper() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathBoundFilterDoubleNoLower() throws Exception
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
            new Object[]{NullHandling.defaultStringValue(), 4L},
            new Object[]{"2.02", 2L}
        )
    );
  }

  @Test
  public void testGroupByPathBoundDoubleFilterNumeric() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathBoundFilterDoubleNoUpperNumeric() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathBoundFilterDoubleNoLowerNumeric() throws Exception
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
            new Object[]{"2.02", 2L}
        )
    );
  }

  @Test
  public void testGroupByPathBoundFilterString() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathBoundFilterStringNoUpper() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathBoundFilterStringNoLower() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathLikeFilter() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathLikeFilterStringPrefix() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathLikeFilterString() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathLikeFilterVariant() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathInFilter() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathInFilterDouble() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathInFilterString() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByPathInFilterVariant() throws Exception
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
        )
    );
  }

  @Test
  public void testSumPath() throws Exception
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
        )
    );
  }


  @Test
  public void testSumPathFilteredAggDouble() throws Exception
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
        )
    );
  }

  @Test
  public void testSumPathFilteredAggString() throws Exception
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
        )
    );
  }

  @Test
  public void testSumPathMixed() throws Exception
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
        )
    );
  }

  @Test
  public void testSumPathMixedFilteredAggLong() throws Exception
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
        )
    );
  }

  @Test
  public void testSumPathMixedFilteredAggDouble() throws Exception
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
        )
    );
  }

  @Test
  public void testCastAndSumPath() throws Exception
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
        )
    );
  }


  @Test
  public void testCastAndSumPathStrings() throws Exception
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
        )
    );
  }

  @Test
  public void testReturningAndSumPath() throws Exception
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
        )
    );
  }


  @Test
  public void testReturningAndSumPathStrings() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByRootKeys() throws Exception
  {
    cannotVectorize();
    testQuery(
        "SELECT "
        + "JSON_KEYS(nester, '.'), "
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
                                "json_keys(\"nester\",'.')",
                                ColumnType.STRING_ARRAY,
                                macroTable
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
        )
    );
  }

  @Test
  public void testGroupByRootKeysJsonPath() throws Exception
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
                                macroTable
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
        )
    );
  }

  @Test
  public void testGroupByRootKeys2() throws Exception
  {
    cannotVectorize();
    testQuery(
        "SELECT "
        + "JSON_KEYS(nest, '.'), "
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
                                "json_keys(\"nest\",'.')",
                                ColumnType.STRING_ARRAY,
                                macroTable
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
        )
    );
  }

  @Test
  public void testGroupByAllPaths() throws Exception
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
                                macroTable
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
            new Object[]{"[\"$.\"]", 5L},
            new Object[]{"[\"$.n.x\",\"$.array[0]\",\"$.array[1]\"]", 2L}
        )
    );
  }

  @Test
  public void testGroupByNestedArrayPath() throws Exception
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
        )
    );
  }

  @Test
  public void testGroupByInvalidPath() throws Exception
  {
    testQueryThrows(
        "SELECT "
        + "JSON_VALUE(nester, '.array.[1]'), "
        + "SUM(cnt) "
        + "FROM druid.nested GROUP BY 1",
        (expected) -> {
          expected.expect(UnsupportedSQLQueryException.class);
          expected.expectMessage(
              "Cannot use [JSON_VALUE_ANY]: [Bad format, '.array.[1]' is not a valid JSONPath path: must start with '$']");
        }
    );
  }

  @Test
  public void testJsonQuery() throws Exception
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
        )
    );
  }

  @Test
  public void testJsonQueryAndJsonObject() throws Exception
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
                          macroTable
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
        )
    );
  }

  @Test
  public void testToJsonAndParseJson() throws Exception
  {
    testQuery(
        "SELECT string, TO_JSON(string), PARSE_JSON(string), PARSE_JSON('{\"foo\":1}'), PARSE_JSON(TO_JSON_STRING(nester))\n"
        + "FROM druid.nested",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "to_json(\"string\")",
                          NestedDataComplexTypeSerde.TYPE,
                          macroTable
                      ),
                      new ExpressionVirtualColumn(
                          "v1",
                          "parse_json(\"string\")",
                          NestedDataComplexTypeSerde.TYPE,
                          macroTable
                      ),
                      new ExpressionVirtualColumn(
                          "v2",
                          "parse_json('{\\u0022foo\\u0022:1}')",
                          NestedDataComplexTypeSerde.TYPE,
                          macroTable
                      ),
                      new ExpressionVirtualColumn(
                          "v3",
                          "parse_json(to_json_string(\"nester\"))",
                          NestedDataComplexTypeSerde.TYPE,
                          macroTable
                      )
                  )
                  .columns("string", "v0", "v1", "v2", "v3")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                "aaa",
                "\"aaa\"",
                "\"aaa\"",
                "{\"foo\":1}",
                "{\"array\":[\"a\",\"b\"],\"n\":{\"x\":\"hello\"}}"
            },
            new Object[]{"bbb", "\"bbb\"", "\"bbb\"", "{\"foo\":1}", "\"hello\""},
            new Object[]{"ccc", "\"ccc\"", "\"ccc\"", "{\"foo\":1}", null},
            new Object[]{"ddd", "\"ddd\"", "\"ddd\"", "{\"foo\":1}", null},
            new Object[]{"eee", "\"eee\"", "\"eee\"", "{\"foo\":1}", null},
            new Object[]{"aaa", "\"aaa\"", "\"aaa\"", "{\"foo\":1}", "{\"array\":[\"a\",\"b\"],\"n\":{\"x\":1}}"},
            new Object[]{"ddd", "\"ddd\"", "\"ddd\"", "{\"foo\":1}", "2"}
        )
    );
  }
}

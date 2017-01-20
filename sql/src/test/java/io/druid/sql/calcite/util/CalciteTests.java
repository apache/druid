/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.util;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.druid.collections.StupidPool;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.DruidProcessingConfig;
import io.druid.query.Query;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import io.druid.query.groupby.strategy.GroupByStrategySelector;
import io.druid.query.metadata.SegmentMetadataQueryConfig;
import io.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import io.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryEngine;
import io.druid.query.select.SelectQueryQueryToolChest;
import io.druid.query.select.SelectQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNQueryRunnerFactory;
import io.druid.segment.IndexBuilder;
import io.druid.segment.QueryableIndex;
import io.druid.segment.TestHelper;
import io.druid.segment.column.ValueType;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.sql.calcite.aggregation.ApproxCountDistinctSqlAggregator;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.rel.QueryMaker;
import io.druid.sql.calcite.table.DruidTable;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.joda.time.DateTime;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Utility functions for Calcite tests.
 */
public class CalciteTests
{
  public static final String DATASOURCE1 = "foo";
  public static final String DATASOURCE2 = "foo2";

  private static final String TIMESTAMP_COLUMN = "t";

  private static final QueryRunnerFactoryConglomerate CONGLOMERATE = new DefaultQueryRunnerFactoryConglomerate(
      ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>builder()
          .put(
              SegmentMetadataQuery.class,
              new SegmentMetadataQueryRunnerFactory(
                  new SegmentMetadataQueryQueryToolChest(
                      new SegmentMetadataQueryConfig("P1W")
                  ),
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER
              )
          )
          .put(
              SelectQuery.class,
              new SelectQueryRunnerFactory(
                  new SelectQueryQueryToolChest(
                      TestHelper.getObjectMapper(),
                      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                  ),
                  new SelectQueryEngine(),
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER
              )
          )
          .put(
              TimeseriesQuery.class,
              new TimeseriesQueryRunnerFactory(
                  new TimeseriesQueryQueryToolChest(
                      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                  ),
                  new TimeseriesQueryEngine(),
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER
              )
          )
          .put(
              TopNQuery.class,
              new TopNQueryRunnerFactory(
                  new StupidPool<>(
                      "TopNQueryRunnerFactory-bufferPool",
                      new Supplier<ByteBuffer>()
                      {
                        @Override
                        public ByteBuffer get()
                        {
                          return ByteBuffer.allocate(10 * 1024 * 1024);
                        }
                      }
                  ),
                  new TopNQueryQueryToolChest(
                      new TopNQueryConfig(),
                      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                  ),
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER
              )
          )
          .put(
              GroupByQuery.class,
              GroupByQueryRunnerTest.makeQueryRunnerFactory(
                  GroupByQueryRunnerTest.DEFAULT_MAPPER,
                  new GroupByQueryConfig()
                  {
                    @Override
                    public String getDefaultStrategy()
                    {
                      return GroupByStrategySelector.STRATEGY_V2;
                    }
                  },
                  new DruidProcessingConfig()
                  {
                    @Override
                    public String getFormatString()
                    {
                      return null;
                    }

                    @Override
                    public int intermediateComputeSizeBytes()
                    {
                      return 10 * 1024 * 1024;
                    }

                    @Override
                    public int getNumMergeBuffers()
                    {
                      // Need 3 buffers for CalciteQueryTest.testDoubleNestedGroupby.
                      return 3;
                    }
                  }
              )
          )
          .build()
  );

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", null),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2")),
              null,
              null
          )
      )
  );

  private static final IncrementalIndexSchema INDEX_SCHEMA = new IncrementalIndexSchema.Builder()
      .withMetrics(
          new AggregatorFactory[]{
              new CountAggregatorFactory("cnt"),
              new DoubleSumAggregatorFactory("m1", "m1"),
              new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
          }
      )
      .withRollup(false)
      .build();

  private static final List<InputRow> ROWS1 = ImmutableList.of(
      createRow(ImmutableMap.of("t", "2000-01-01", "m1", "1.0", "dim1", "", "dim2", ImmutableList.of("a"))),
      createRow(ImmutableMap.of("t", "2000-01-02", "m1", "2.0", "dim1", "10.1", "dim2", ImmutableList.of())),
      createRow(ImmutableMap.of("t", "2000-01-03", "m1", "3.0", "dim1", "2", "dim2", ImmutableList.of(""))),
      createRow(ImmutableMap.of("t", "2001-01-01", "m1", "4.0", "dim1", "1", "dim2", ImmutableList.of("a"))),
      createRow(ImmutableMap.of("t", "2001-01-02", "m1", "5.0", "dim1", "def", "dim2", ImmutableList.of("abc"))),
      createRow(ImmutableMap.of("t", "2001-01-03", "m1", "6.0", "dim1", "abc"))
  );

  private static final List<InputRow> ROWS2 = ImmutableList.of(
      createRow("2000-01-01", "דרואיד", "he", 1.0),
      createRow("2000-01-01", "druid", "en", 1.0),
      createRow("2000-01-01", "друид", "ru", 1.0)
  );

  private static final Map<String, ValueType> COLUMN_TYPES = ImmutableMap.<String, ValueType>builder()
      .put("__time", ValueType.LONG)
      .put("cnt", ValueType.LONG)
      .put("dim1", ValueType.STRING)
      .put("dim2", ValueType.STRING)
      .put("m1", ValueType.FLOAT)
      .put("unique_dim1", ValueType.COMPLEX)
      .build();

  private CalciteTests()
  {
    // No instantiation.
  }

  public static QueryRunnerFactoryConglomerate queryRunnerFactoryConglomerate()
  {
    return CONGLOMERATE;
  }

  public static SpecificSegmentsQuerySegmentWalker createMockWalker(final File tmpDir)
  {
    final QueryableIndex index1 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "1"))
                                              .indexMerger(TestHelper.getTestIndexMergerV9())
                                              .schema(INDEX_SCHEMA)
                                              .rows(ROWS1)
                                              .buildMMappedIndex();

    final QueryableIndex index2 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "2"))
                                              .indexMerger(TestHelper.getTestIndexMergerV9())
                                              .schema(INDEX_SCHEMA)
                                              .rows(ROWS2)
                                              .buildMMappedIndex();

    return new SpecificSegmentsQuerySegmentWalker(queryRunnerFactoryConglomerate()).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE1)
                   .interval(index1.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index1
    ).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE2)
                   .interval(index2.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index2
    );
  }

  public static DruidOperatorTable createOperatorTable()
  {
    return new DruidOperatorTable(ImmutableSet.<SqlAggregator>of(new ApproxCountDistinctSqlAggregator()));
  }

  public static Schema createMockSchema(final QuerySegmentWalker walker, final PlannerConfig plannerConfig)
  {
    final QueryMaker queryMaker = new QueryMaker(walker, plannerConfig);
    final DruidTable druidTable1 = new DruidTable(queryMaker, new TableDataSource(DATASOURCE1), COLUMN_TYPES);
    final DruidTable druidTable2 = new DruidTable(queryMaker, new TableDataSource(DATASOURCE2), COLUMN_TYPES);
    final Map<String, Table> tableMap = ImmutableMap.<String, Table>of(
        DATASOURCE1, druidTable1,
        DATASOURCE2, druidTable2
    );
    return new AbstractSchema()
    {
      @Override
      protected Map<String, Table> getTableMap()
      {
        return tableMap;
      }
    };
  }

  public static InputRow createRow(final ImmutableMap<String, ?> map)
  {
    return PARSER.parse((Map<String, Object>) map);
  }

  public static InputRow createRow(final Object t, final String dim1, final String dim2, final double m1)
  {
    return PARSER.parse(
        ImmutableMap.<String, Object>of(
            "t", new DateTime(t).getMillis(),
            "dim1", dim1,
            "dim2", dim2,
            "m1", m1
        )
    );
  }
}

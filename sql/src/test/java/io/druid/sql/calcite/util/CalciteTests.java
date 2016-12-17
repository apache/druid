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
import io.druid.collections.StupidPool;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
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
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.table.DruidTable;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Utility functions for Calcite tests.
 */
public class CalciteTests
{
  public static final String DATASOURCE = "foo";

  private static final String TIMESTAMP_COLUMN = "t";
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
  private static final List<InputRow> ROWS = ImmutableList.of(
      ROW(ImmutableMap.of("t", "2000-01-01", "m1", "1.0", "dim1", "", "dim2", ImmutableList.of("a"))),
      ROW(ImmutableMap.of("t", "2000-01-02", "m1", "2.0", "dim1", "10.1", "dim2", ImmutableList.of())),
      ROW(ImmutableMap.of("t", "2000-01-03", "m1", "3.0", "dim1", "2", "dim2", ImmutableList.of(""))),
      ROW(ImmutableMap.of("t", "2001-01-01", "m1", "4.0", "dim1", "1", "dim2", ImmutableList.of("a"))),
      ROW(ImmutableMap.of("t", "2001-01-02", "m1", "5.0", "dim1", "def", "dim2", ImmutableList.of("abc"))),
      ROW(ImmutableMap.of("t", "2001-01-03", "m1", "6.0", "dim1", "abc"))
  );
  private static final Map<String, ValueType> COLUMN_TYPES = ImmutableMap.of(
      "__time", ValueType.LONG,
      "cnt", ValueType.LONG,
      "dim1", ValueType.STRING,
      "dim2", ValueType.STRING,
      "m1", ValueType.FLOAT
  );

  private CalciteTests()
  {
    // No instantiation.
  }

  public static SpecificSegmentsQuerySegmentWalker createWalker(final File tmpDir)
  {
    return createWalker(tmpDir, ROWS);
  }

  public static SpecificSegmentsQuerySegmentWalker createWalker(final File tmpDir, final List<InputRow> rows)
  {
    final QueryableIndex index = IndexBuilder.create()
                                             .tmpDir(tmpDir)
                                             .indexMerger(TestHelper.getTestIndexMergerV9())
                                             .schema(
                                                 new IncrementalIndexSchema.Builder()
                                                     .withMetrics(
                                                         new AggregatorFactory[]{
                                                             new CountAggregatorFactory("cnt"),
                                                             new DoubleSumAggregatorFactory("m1", "m1"),
                                                             new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
                                                         }
                                                     )
                                                     .withRollup(false)
                                                     .build()
                                             )
                                             .rows(rows)
                                             .buildMMappedIndex();

    final QueryRunnerFactoryConglomerate conglomerate = new DefaultQueryRunnerFactoryConglomerate(
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
                    new GroupByQueryConfig()
                    {
                      @Override
                      public String getDefaultStrategy()
                      {
                        return GroupByStrategySelector.STRATEGY_V2;
                      }
                    }
                )
            )
            .build()
    );

    return new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE)
                   .interval(index.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index
    );
  }

  public static DruidTable createDruidTable(final QuerySegmentWalker walker, final PlannerConfig plannerConfig)
  {
    return new DruidTable(walker, new TableDataSource(DATASOURCE), plannerConfig, COLUMN_TYPES);
  }

  public static Schema createMockSchema(final QuerySegmentWalker walker, final PlannerConfig plannerConfig)
  {
    final DruidTable druidTable = createDruidTable(walker, plannerConfig);
    final Map<String, Table> tableMap = ImmutableMap.<String, Table>of(DATASOURCE, druidTable);
    return new AbstractSchema()
    {
      @Override
      protected Map<String, Table> getTableMap()
      {
        return tableMap;
      }
    };
  }

  private static InputRow ROW(final ImmutableMap<String, ?> map)
  {
    return PARSER.parse((Map<String, Object>) map);
  }
}

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

package org.apache.druid.spectator.histogram.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.spectator.histogram.SpectatorHistogramAggregatorFactory;
import org.apache.druid.spectator.histogram.SpectatorHistogramCountPostAggregator;
import org.apache.druid.spectator.histogram.SpectatorHistogramModule;
import org.apache.druid.spectator.histogram.SpectatorHistogramPercentilePostAggregator;
import org.apache.druid.spectator.histogram.SpectatorHistogramPercentilesPostAggregator;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@SqlTestFrameworkConfig.ComponentSupplier(SpectatorHistogramSqlAggregatorTest.SpectatorHistogramComponentSupplier.class)
public class SpectatorHistogramSqlAggregatorTest extends BaseCalciteQueryTest
{
  private static final List<ImmutableMap<String, Object>> RAW_ROWS = ImmutableList.of(
      ImmutableMap.of("t", "2000-01-01", "dim1", "a", "metric", 100L),
      ImmutableMap.of("t", "2000-01-02", "dim1", "b", "metric", 200L),
      ImmutableMap.of("t", "2000-01-03", "dim1", "c", "metric", 300L),
      ImmutableMap.of("t", "2000-01-04", "dim1", "d", "metric", 400L),
      ImmutableMap.of("t", "2000-01-05", "dim1", "e", "metric", 500L),
      ImmutableMap.of("t", "2000-01-06", "dim1", "f", "metric", 600L)
  );

  private static final MapInputRowParser PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec("t", "auto", null),
          DimensionsSpec.builder()
                        .setDimensions(ImmutableList.of(
                            new StringDimensionSchema("dim1"),
                            new LongDimensionSchema("metric")
                        ))
                        .setDimensionExclusions(ImmutableList.of("t"))
                        .build()
      )
  );

  private static final List<InputRow> ROWS = RAW_ROWS.stream()
                                                     .map(raw -> PARSER.parseBatch(raw).get(0))
                                                     .collect(Collectors.toList());

  protected static class SpectatorHistogramComponentSupplier extends StandardComponentSupplier
  {
    public SpectatorHistogramComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public DruidModule getCoreModule()
    {
      return DruidModuleCollection.of(super.getCoreModule(), new SpectatorHistogramModule());
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(SpecificSegmentsQuerySegmentWalker walker)
    {
      SpectatorHistogramModule.registerSerde();

      final QueryableIndex index =
          IndexBuilder.create(CalciteTests.getJsonMapper())
                      .tmpDir(tempDirProducer.newTempFolder())
                      .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                      .schema(
                          new IncrementalIndexSchema.Builder()
                              .withDimensionsSpec(
                                  DimensionsSpec.builder()
                                                .setDimensions(ImmutableList.of(
                                                    new StringDimensionSchema("dim1"),
                                                    new LongDimensionSchema("metric")
                                                ))
                                                .build()
                              )
                              .withMetrics(
                                  new CountAggregatorFactory("cnt"),
                                  new SpectatorHistogramAggregatorFactory(
                                      "histogram_metric",
                                      "metric"
                                  )
                              )
                              .withRollup(false)
                              .build()
                      )
                      .rows(ROWS)
                      .buildMMappedIndex();

      return walker.add(
          DataSegment.builder()
                     .dataSource(CalciteTests.DATASOURCE1)
                     .interval(index.getDataInterval())
                     .version("1")
                     .shardSpec(new LinearShardSpec(0))
                     .size(0)
                     .build(),
          index
      );
    }
  }

  @Test
  public void testSpectatorPercentileOnPreAggregatedHistogram()
  {
    testQuery(
        "SELECT SPECTATOR_PERCENTILE(histogram_metric, 50) FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new SpectatorHistogramAggregatorFactory("a0:agg", "histogram_metric")
                  ))
                  .postAggregators(
                      new SpectatorHistogramPercentilePostAggregator(
                          "a0",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg"),
                          50.0
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            // Histogram bucket approximation for p50 of [100, 200, 300, 400, 500, 600]
            new Object[]{341.0}
        )
    );
  }

  @Test
  public void testSpectatorCountOnPreAggregatedHistogram()
  {
    testQuery(
        "SELECT SPECTATOR_COUNT(histogram_metric) FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new SpectatorHistogramAggregatorFactory("a0:agg", "histogram_metric")
                  ))
                  .postAggregators(
                      new SpectatorHistogramCountPostAggregator(
                          "a0",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg")
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{6L})
    );
  }

  @Test
  public void testSpectatorPercentileOnLongColumn()
  {
    // This creates a histogram from the raw long values and then computes percentile
    testQuery(
        "SELECT SPECTATOR_PERCENTILE(metric, 50) FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new SpectatorHistogramAggregatorFactory("a0:agg", "metric")
                  ))
                  .postAggregators(
                      new SpectatorHistogramPercentilePostAggregator(
                          "a0",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg"),
                          50.0
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            // Histogram bucket approximation for p50 of [100, 200, 300, 400, 500, 600]
            new Object[]{341.0}
        )
    );
  }

  @Test
  public void testSpectatorCountOnLongColumn()
  {
    // This creates a histogram from the raw values and then counts
    testQuery(
        "SELECT SPECTATOR_COUNT(metric) FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new SpectatorHistogramAggregatorFactory("a0:agg", "metric")
                  ))
                  .postAggregators(
                      new SpectatorHistogramCountPostAggregator(
                          "a0",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg")
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{6L})
    );
  }

  @Test
  public void testSpectatorCountOnLongColumnGroupBy()
  {
    testQuery(
        "SELECT dim1, SPECTATOR_COUNT(metric) FROM foo GROUP BY dim1",
        Collections.singletonList(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            new SpectatorHistogramAggregatorFactory("a0:agg", "metric")
                        )
                        .setPostAggregatorSpecs(
                            new SpectatorHistogramCountPostAggregator(
                                "a0",
                                new FieldAccessPostAggregator("a0:agg", "a0:agg")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 1L},
            new Object[]{"b", 1L},
            new Object[]{"c", 1L},
            new Object[]{"d", 1L},
            new Object[]{"e", 1L},
            new Object[]{"f", 1L}
        )
    );
  }

  @Test
  public void testSpectatorCountGroupBy()
  {
    testQuery(
        "SELECT dim1, SPECTATOR_COUNT(histogram_metric) FROM foo GROUP BY dim1",
        Collections.singletonList(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            new SpectatorHistogramAggregatorFactory("a0:agg", "histogram_metric")
                        )
                        .setPostAggregatorSpecs(
                            new SpectatorHistogramCountPostAggregator(
                                "a0",
                                new FieldAccessPostAggregator("a0:agg", "a0:agg")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 1L},
            new Object[]{"b", 1L},
            new Object[]{"c", 1L},
            new Object[]{"d", 1L},
            new Object[]{"e", 1L},
            new Object[]{"f", 1L}
        )
    );
  }

  @Test
  public void testMultipleAggregationsOnSameColumn()
  {
    // Test that multiple aggregations on the same column share a single aggregator
    testQuery(
        "SELECT SPECTATOR_COUNT(histogram_metric), SPECTATOR_PERCENTILE(histogram_metric, 50), "
        + "SPECTATOR_PERCENTILE(histogram_metric, 99.99) FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new SpectatorHistogramAggregatorFactory("a0:agg", "histogram_metric")
                  ))
                  .postAggregators(
                      new SpectatorHistogramCountPostAggregator(
                          "a0",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg")
                      ),
                      new SpectatorHistogramPercentilePostAggregator(
                          "a1",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg"),
                          50.0
                      ),
                      new SpectatorHistogramPercentilePostAggregator(
                          "a2",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg"),
                          99.99
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            // p50 = 341.0, p99.99 = 680.949 (interpolated value near max)
            new Object[]{6L, 341.0, 680.949}
        )
    );
  }

  @Test
  public void testSpectatorPercentileGroupBy()
  {
    testQuery(
        "SELECT dim1, SPECTATOR_PERCENTILE(histogram_metric, 50) FROM foo GROUP BY dim1",
        Collections.singletonList(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            new SpectatorHistogramAggregatorFactory("a0:agg", "histogram_metric")
                        )
                        .setPostAggregatorSpecs(
                            new SpectatorHistogramPercentilePostAggregator(
                                "a0",
                                new FieldAccessPostAggregator("a0:agg", "a0:agg"),
                                50.0
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            // Each row has a single value, so p50 returns the middle of that bucket
            // Values depend on Spectator histogram bucket boundaries
            new Object[]{"a", 95.5},
            new Object[]{"b", 200.5},
            new Object[]{"c", 298.5},
            new Object[]{"d", 383.5},
            new Object[]{"e", 468.5},
            new Object[]{"f", 638.5}
        )
    );
  }

  @Test
  public void testSpectatorPercentileWithArray()
  {
    testQuery(
        "SELECT SPECTATOR_PERCENTILE(histogram_metric, ARRAY[25, 50, 75, 99]) FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new SpectatorHistogramAggregatorFactory("a0:agg", "histogram_metric")
                  ))
                  .postAggregators(
                      new SpectatorHistogramPercentilesPostAggregator(
                          "a0",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg"),
                          new double[]{25.0, 50.0, 75.0, 99.0}
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            // Returns an array of percentile values
            new Object[]{"[200.5,341.0,468.5,675.9]"}
        )
    );
  }

  @Test
  public void testSpectatorPercentileWithArrayGroupBy()
  {
    testQuery(
        "SELECT dim1, SPECTATOR_PERCENTILE(histogram_metric, ARRAY[50, 99]) FROM foo GROUP BY dim1",
        Collections.singletonList(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            new SpectatorHistogramAggregatorFactory("a0:agg", "histogram_metric")
                        )
                        .setPostAggregatorSpecs(
                            new SpectatorHistogramPercentilesPostAggregator(
                                "a0",
                                new FieldAccessPostAggregator("a0:agg", "a0:agg"),
                                new double[]{50.0, 99.0}
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            // Returns array of [p50, p99] for each group
            new Object[]{"a", "[95.5,105.78999999999999]"},
            new Object[]{"b", "[200.5,210.79]"},
            new Object[]{"c", "[298.5,340.15]"},
            new Object[]{"d", "[383.5,425.15]"},
            new Object[]{"e", "[468.5,510.15]"},
            new Object[]{"f", "[638.5,680.15]"}
        )
    );
  }

  @Test
  public void testSpectatorFunctionsOnEmptyHistogram()
  {
    // Use a filter that matches no rows to get an empty/null histogram aggregation result
    // Both COUNT and PERCENTILE return null for empty histogram (same as native behavior)
    testQuery(
        "SELECT SPECTATOR_COUNT(histogram_metric), SPECTATOR_PERCENTILE(histogram_metric, 50) "
        + "FROM foo WHERE dim1 = 'nonexistent'",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .filters(equality("dim1", "nonexistent", ColumnType.STRING))
                  .aggregators(ImmutableList.of(
                      new SpectatorHistogramAggregatorFactory("a0:agg", "histogram_metric")
                  ))
                  .postAggregators(
                      new SpectatorHistogramCountPostAggregator(
                          "a0",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg")
                      ),
                      new SpectatorHistogramPercentilePostAggregator(
                          "a1",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg"),
                          50.0
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{null, null})
    );
  }

  @Test
  public void testSpectatorFunctionsOnNullHistogram()
  {
    // Both COUNT and PERCENTILE return null for null histogram (same as native behavior)
    testQuery(
        "SELECT SPECTATOR_COUNT(null), SPECTATOR_PERCENTILE(null, 99.9), SPECTATOR_PERCENTILE(null, ARRAY[90, 99.9]) FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .virtualColumns(expressionVirtualColumn("v0", "null", ColumnType.DOUBLE))
                  .aggregators(ImmutableList.of(
                      new SpectatorHistogramAggregatorFactory("a0:agg", "v0")
                  ))
                  .postAggregators(
                      new SpectatorHistogramCountPostAggregator(
                          "a0",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg")
                      ),
                      new SpectatorHistogramPercentilePostAggregator(
                          "a1",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg"),
                          99.9
                      ),
                      new SpectatorHistogramPercentilesPostAggregator(
                          "a2",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg"),
                          new double[]{90.0, 99.9}
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{null, null, null})
    );
  }
}

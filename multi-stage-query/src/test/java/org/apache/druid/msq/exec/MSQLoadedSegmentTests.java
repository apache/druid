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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.error.ThrowableMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.msq.indexing.LegacyMSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

public class MSQLoadedSegmentTests extends MSQTestBase
{
  public static final Map<String, Object> REALTIME_QUERY_CTX =
      QueryContext.builder()
                  .putAll(DEFAULT_MSQ_CONTEXT)
                  .put(QueryContextParameters.INCLUDE_SEGMENT_SOURCE, SegmentSource.REALTIME.name())
                  .toMap();
  public static final DataSegment LOADED_SEGMENT_1 =
      DataSegment.builder(SegmentId.of(CalciteTests.DATASOURCE1, Intervals.of("2003-01-01T00:00:00.000Z/2004-01-01T00:00:00.000Z"), "1", 0))
                 .shardSpec(new LinearShardSpec(0))
                 .size(0)
                 .build();

  public static final DruidServerMetadata DATA_SERVER_1 = new DruidServerMetadata(
      "TestDataServer",
      "hostName:9092",
      null,
      2,
      null,
      ServerType.REALTIME,
      "tier1",
      2
  );

  @BeforeEach
  public void setUp()
  {
    loadedSegmentsMetadata.add(new ImmutableSegmentLoadInfo(LOADED_SEGMENT_1, ImmutableSet.of(DATA_SERVER_1)));
  }

  @Test
  public void testSelectWithLoadedSegmentsOnFoo()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    doReturn(
        Futures.immediateFuture(
            new DataServerQueryResult<>(
                ImmutableList.of(
                    Yielders.each(
                        Sequences.simple(
                            ImmutableList.of(
                                new Object[]{1L, "qwe"},
                                new Object[]{1L, "tyu"}
                            )
                        )
                    )),
                ImmutableList.of(),
                "foo"
            )
        )).when(dataServerQueryHandler)
          .fetchRowsFromDataServer(any(), any(), any(), any());

    testSelectQuery()
        .setSql("select cnt, dim1 from foo")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("cnt", "dim1")
                           .columnTypes(resultSignature.getColumnTypes())
                           .context(REALTIME_QUERY_CTX)
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setQueryContext(REALTIME_QUERY_CTX)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, "qwe"},
            new Object[]{1L, ""},
            new Object[]{1L, "10.1"},
            new Object[]{1L, "tyu"},
            new Object[]{1L, "2"},
            new Object[]{1L, "1"},
            new Object[]{1L, "def"},
            new Object[]{1L, "abc"}
        ))
        .verifyResults();
  }

  @Test
  public void testSelectWithLoadedSegmentsOnFooWithOrderBy()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    doAnswer(
        invocationOnMock -> {
          ScanQuery query = invocationOnMock.getArgument(0);
          ScanQuery.verifyOrderByForNativeExecution(query);
          Assertions.assertEquals(Long.MAX_VALUE, query.getScanRowsLimit());
          return Futures.immediateFuture(
              new DataServerQueryResult<>(
                  ImmutableList.of(
                      Yielders.each(
                          Sequences.simple(
                              ImmutableList.of(
                                  new Object[]{1L, "qwe"},
                                  new Object[]{1L, "tyu"}
                              )
                          )
                      )),
                  ImmutableList.of(),
                  "foo"
              )
          );
        }
    )
        .when(dataServerQueryHandler)
        .fetchRowsFromDataServer(any(), any(), any(), any());

    testSelectQuery()
        .setSql("select cnt, dim1 from foo order by dim1")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("cnt", "dim1")
                           .columnTypes(resultSignature.getColumnTypes())
                           .orderBy(ImmutableList.of(OrderBy.ascending("dim1")))
                           .context(REALTIME_QUERY_CTX)
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setQueryContext(REALTIME_QUERY_CTX)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, ""},
            new Object[]{1L, "1"},
            new Object[]{1L, "10.1"},
            new Object[]{1L, "2"},
            new Object[]{1L, "abc"},
            new Object[]{1L, "def"},
            new Object[]{1L, "qwe"},
            new Object[]{1L, "tyu"}
        ))
        .verifyResults();
  }

  @Test
  public void testSelectWithLoadedSegmentsOnFooWithOrderByLimitOffset()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    stubDataServerScanResults(
        ImmutableList.of("cnt", "dim1"),
        ImmutableList.of(
            ImmutableList.of(1L, "qwe"),
            ImmutableList.of(1L, "tyu")
        )
    );

    testSelectQuery()
        .setSql("select cnt, dim1 from foo order by dim1 limit 2 offset 6")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("cnt", "dim1")
                           .columnTypes(resultSignature.getColumnTypes())
                           .orderBy(ImmutableList.of(OrderBy.ascending("dim1")))
                           .offset(6)
                           .limit(2)
                           .context(REALTIME_QUERY_CTX)
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setQueryContext(REALTIME_QUERY_CTX)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, "qwe"},
            new Object[]{1L, "tyu"}
        ))
        .verifyResults();
  }

  @Test
  public void testSelectWithLoadedSegmentsOnFooWithTimeOrderLimitOffset()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("__time", ColumnType.LONG)
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    final long time1 = DateTimes.of("2003-01-01").getMillis();
    final long time2 = DateTimes.of("2003-01-02").getMillis();

    stubDataServerScanResults(
        ImmutableList.of("__time", "cnt", "dim1"),
        ImmutableList.of(
            ImmutableList.of(time1, 1L, "qwe"),
            ImmutableList.of(time2, 1L, "tyu")
        )
    );

    testSelectQuery()
        .setSql("select __time, cnt, dim1 from foo order by __time limit 2 offset 6")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("__time", "cnt", "dim1")
                           .columnTypes(resultSignature.getColumnTypes())
                           .orderBy(ImmutableList.of(OrderBy.ascending("__time")))
                           .offset(6)
                           .limit(2)
                           .context(REALTIME_QUERY_CTX)
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setQueryContext(REALTIME_QUERY_CTX)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{time1, 1L, "qwe"},
            new Object[]{time2, 1L, "tyu"}
        ))
        .verifyResults();
  }

  @Test
  public void testGroupByWithLoadedSegmentsOnFoo()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cnt", ColumnType.LONG)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    doReturn(
        Futures.immediateFuture(
            new DataServerQueryResult<>(
                ImmutableList.of(
                    Yielders.each(
                        Sequences.simple(
                            ImmutableList.of(
                                ResultRow.of(1L, 2L)
                            )
                        )
                    )),
                ImmutableList.of(),
                "foo"
            )
        )
    )
        .when(dataServerQueryHandler)
        .fetchRowsFromDataServer(any(), any(), any(), any());

    testSelectQuery()
        .setSql("select cnt,count(*) as cnt1 from foo group by cnt")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(GroupByQuery.builder()
                                      .setDataSource(CalciteTests.DATASOURCE1)
                                      .setInterval(querySegmentSpec(Filtration
                                                                        .eternity()))
                                      .setGranularity(Granularities.ALL)
                                      .setDimensions(dimensions(
                                          new DefaultDimensionSpec(
                                              "cnt",
                                              "d0",
                                              ColumnType.LONG
                                          )
                                      ))
                                      .setAggregatorSpecs(aggregators(new CountAggregatorFactory(
                                          "a0")))
                                      .setContext(REALTIME_QUERY_CTX)
                                      .build())
                   .columnMappings(
                       new ColumnMappings(ImmutableList.of(
                           new ColumnMapping("d0", "cnt"),
                           new ColumnMapping("a0", "cnt1")))
                   )
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setQueryContext(REALTIME_QUERY_CTX)
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(new Object[]{1L, 8L}))
        .verifyResults();
  }

  @Test
  public void testGroupByWithOnlyLoadedSegmentsOnFoo()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cnt", ColumnType.LONG)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    doReturn(
        Futures.immediateFuture(
            new DataServerQueryResult<>(
                ImmutableList.of(
                    Yielders.each(
                        Sequences.simple(
                            ImmutableList.of(
                                ResultRow.of(1L, 2L)
                            )
                        )
                    )),
                ImmutableList.of(),
                "foo"
            )
        )
    )
        .when(dataServerQueryHandler)
        .fetchRowsFromDataServer(any(), any(), any(), any());

    testSelectQuery()
        .setSql("select cnt,count(*) as cnt1 from foo where (TIMESTAMP '2003-01-01 00:00:00' <= \"__time\" AND \"__time\" < TIMESTAMP '2005-01-01 00:00:00') group by cnt")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(GroupByQuery.builder()
                                      .setDataSource(CalciteTests.DATASOURCE1)
                                      .setInterval(Intervals.of("2003-01-01T00:00:00.000Z/2005-01-01T00:00:00.000Z"))
                                      .setGranularity(Granularities.ALL)
                                      .setDimensions(dimensions(
                                          new DefaultDimensionSpec(
                                              "cnt",
                                              "d0",
                                              ColumnType.LONG
                                          )
                                      ))
                                      .setQuerySegmentSpec(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2003-01-01T00:00:00.000Z/2005-01-01T00:00:00.000Z"))))
                                      .setAggregatorSpecs(aggregators(new CountAggregatorFactory(
                                          "a0")))
                                      .setContext(REALTIME_QUERY_CTX)
                                      .build())
                   .columnMappings(
                       new ColumnMappings(ImmutableList.of(
                           new ColumnMapping("d0", "cnt"),
                           new ColumnMapping("a0", "cnt1")))
                   )
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setQueryContext(REALTIME_QUERY_CTX)
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(new Object[]{1L, 2L}))
        .verifyResults();
  }

  @Test
  public void testDataServerQueryFailedShouldFail()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cnt", ColumnType.LONG)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    doThrow(
        new ISE("Segment could not be found on data server, but segment was not handed off.")
    )
        .when(dataServerQueryHandler)
        .fetchRowsFromDataServer(any(), any(), any(), any());

    testSelectQuery()
        .setSql("select cnt,count(*) as cnt1 from foo where (TIMESTAMP '2003-01-01 00:00:00' <= \"__time\" AND \"__time\" < TIMESTAMP '2005-01-01 00:00:00') group by cnt")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(GroupByQuery.builder()
                                      .setDataSource(CalciteTests.DATASOURCE1)
                                      .setInterval(Intervals.of("2003-01-01T00:00:00.000Z/2005-01-01T00:00:00.000Z"))
                                      .setGranularity(Granularities.ALL)
                                      .setDimensions(dimensions(
                                          new DefaultDimensionSpec(
                                              "cnt",
                                              "d0",
                                              ColumnType.LONG
                                          )
                                      ))
                                      .setQuerySegmentSpec(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2003-01-01T00:00:00.000Z/2005-01-01T00:00:00.000Z"))))
                                      .setAggregatorSpecs(aggregators(new CountAggregatorFactory(
                                          "a0")))
                                      .setContext(REALTIME_QUERY_CTX)
                                      .build())
                   .columnMappings(
                       new ColumnMappings(ImmutableList.of(
                           new ColumnMapping("d0", "cnt"),
                           new ColumnMapping("a0", "cnt1")))
                   )
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setQueryContext(REALTIME_QUERY_CTX)
        .setExpectedRowSignature(rowSignature)
        .setExpectedExecutionErrorMatcher(ThrowableMatcher.of(ISE.class))
        .verifyExecutionError();
  }

  /**
   * Stubs {@link #dataServerQueryHandler} to serve scan queries from the given rows, passing them through
   * {@link ScanQueryQueryToolChest#mergeResults} so that offset and limit are applied the way a realtime server
   * applies them.
   */
  private void stubDataServerScanResults(final List<String> columns, final List<List<Object>> rows)
  {
    doAnswer(
        invocationOnMock -> {
          final ScanQuery query = invocationOnMock.getArgument(0);
          final Function<Sequence<ScanResultValue>, Sequence<Object[]>> mappingFunction =
              invocationOnMock.getArgument(2);

          // One row per batch, since that is what the time-ordered merge expects.
          final QueryRunner<ScanResultValue> baseRunner = (queryPlus, responseContext) -> Sequences.simple(
              rows.stream()
                  .map(row -> new ScanResultValue(null, columns, ImmutableList.of(row)))
                  .collect(Collectors.toList())
          );

          final Sequence<ScanResultValue> results =
              new ScanQueryQueryToolChest(DefaultGenericQueryMetricsFactory.instance())
                  .mergeResults(baseRunner)
                  .run(QueryPlus.wrap(query), ResponseContext.createEmpty());

          return Futures.immediateFuture(
              new DataServerQueryResult<>(
                  ImmutableList.of(Yielders.each(mappingFunction.apply(results))),
                  ImmutableList.of(),
                  "foo"
              )
          );
        }
    )
        .when(dataServerQueryHandler)
        .fetchRowsFromDataServer(any(), any(), any(), any());
  }
}

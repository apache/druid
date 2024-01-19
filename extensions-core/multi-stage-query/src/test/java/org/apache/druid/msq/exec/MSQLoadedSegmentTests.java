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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.scan.ScanQuery;
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
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

public class MSQLoadedSegmentTests extends MSQTestBase
{
  public static final Map<String, Object> REALTIME_QUERY_CTX =
      ImmutableMap.<String, Object>builder()
                  .putAll(DEFAULT_MSQ_CONTEXT)
                  .put(MultiStageQueryContext.CTX_INCLUDE_SEGMENT_SOURCE, SegmentSource.REALTIME.name())
                  .build();
  public static final DataSegment LOADED_SEGMENT_1 =
      DataSegment.builder()
                 .dataSource(CalciteTests.DATASOURCE1)
                 .interval(Intervals.of("2003-01-01T00:00:00.000Z/2004-01-01T00:00:00.000Z"))
                 .version("1")
                 .shardSpec(new LinearShardSpec(0))
                 .size(0)
                 .build();

  public static final DruidServerMetadata DATA_SERVER_1 = new DruidServerMetadata(
      "TestDataServer",
      "hostName:9092",
      null,
      2,
      ServerType.REALTIME,
      "tier1",
      2
  );

  @Before
  public void setUp()
  {
    loadedSegmentsMetadata.add(new ImmutableSegmentLoadInfo(LOADED_SEGMENT_1, ImmutableSet.of(DATA_SERVER_1)));
  }

  @Test
  public void testSelectWithLoadedSegmentsOnFoo() throws IOException
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    doReturn(
        Pair.of(
            LoadedSegmentDataProvider.DataServerQueryStatus.SUCCESS,
            Yielders.each(
                Sequences.simple(
                    ImmutableList.of(
                        new Object[]{1L, "qwe"},
                        new Object[]{1L, "tyu"}
                    )
                )
            )
        )
    )
        .when(loadedSegmentDataProvider)
        .fetchRowsFromDataServer(any(), any(), any(), any());

    testSelectQuery()
        .setSql("select cnt, dim1 from foo")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("cnt", "dim1")
                           .context(defaultScanQueryContext(REALTIME_QUERY_CTX, resultSignature))
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
            new Object[]{1L, "qwe"},
            new Object[]{1L, "tyu"},
            new Object[]{1L, "10.1"},
            new Object[]{1L, "2"},
            new Object[]{1L, "1"},
            new Object[]{1L, "def"},
            new Object[]{1L, "abc"}
        ))
        .verifyResults();
  }

  @Test
  public void testSelectWithLoadedSegmentsOnFooWithOrderBy() throws IOException
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    doAnswer(
        invocationOnMock -> {
          ScanQuery query = invocationOnMock.getArgument(0);
          ScanQuery.verifyOrderByForNativeExecution(query);
          Assert.assertEquals(Long.MAX_VALUE, query.getScanRowsLimit());
          return Pair.of(
              LoadedSegmentDataProvider.DataServerQueryStatus.SUCCESS,
              Yielders.each(
                  Sequences.simple(
                      ImmutableList.of(
                          new Object[]{1L, "qwe"},
                          new Object[]{1L, "tyu"}
                      )
                  )
              )
          );
        }

    )
        .when(loadedSegmentDataProvider)
        .fetchRowsFromDataServer(any(), any(), any(), any());

    testSelectQuery()
        .setSql("select cnt, dim1 from foo order by dim1")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("cnt", "dim1")
                           .orderBy(ImmutableList.of(new ScanQuery.OrderBy("dim1", ScanQuery.Order.ASCENDING)))
                           .context(defaultScanQueryContext(REALTIME_QUERY_CTX, resultSignature))
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
  public void testGroupByWithLoadedSegmentsOnFoo() throws IOException
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cnt", ColumnType.LONG)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    doReturn(
        Pair.of(LoadedSegmentDataProvider.DataServerQueryStatus.SUCCESS,
                Yielders.each(
                    Sequences.simple(
                        ImmutableList.of(
                            ResultRow.of(1L, 2L)
                        )
                    )
                )
        )
    )
        .when(loadedSegmentDataProvider)
        .fetchRowsFromDataServer(any(), any(), any(), any());

    testSelectQuery()
        .setSql("select cnt,count(*) as cnt1 from foo group by cnt")
        .setExpectedMSQSpec(
            MSQSpec.builder()
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
  public void testGroupByWithOnlyLoadedSegmentsOnFoo() throws IOException
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cnt", ColumnType.LONG)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    doReturn(
        Pair.of(LoadedSegmentDataProvider.DataServerQueryStatus.SUCCESS,
                Yielders.each(
                    Sequences.simple(
                        ImmutableList.of(
                            ResultRow.of(1L, 2L)))))
    ).when(loadedSegmentDataProvider)
     .fetchRowsFromDataServer(any(), any(), any(), any());

    testSelectQuery()
        .setSql("select cnt,count(*) as cnt1 from foo where (TIMESTAMP '2003-01-01 00:00:00' <= \"__time\" AND \"__time\" < TIMESTAMP '2005-01-01 00:00:00') group by cnt")
        .setExpectedMSQSpec(
            MSQSpec.builder()
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
  public void testDataServerQueryFailedShouldFail() throws IOException
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cnt", ColumnType.LONG)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    doThrow(
        new ISE("Segment could not be found on data server, but segment was not handed off.")
    )
        .when(loadedSegmentDataProvider)
        .fetchRowsFromDataServer(any(), any(), any(), any());

    testSelectQuery()
        .setSql("select cnt,count(*) as cnt1 from foo where (TIMESTAMP '2003-01-01 00:00:00' <= \"__time\" AND \"__time\" < TIMESTAMP '2005-01-01 00:00:00') group by cnt")
        .setExpectedMSQSpec(
            MSQSpec.builder()
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
        .setExpectedExecutionErrorMatcher(CoreMatchers.instanceOf(ISE.class))
        .verifyExecutionError();
  }
}

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

package org.apache.druid.segment.projections;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.query.expression.TimestampFormatExprMacro;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.TestQueryRunnerKit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.times;

public class ProjectionsTest
{
  private static final TestQueryRunnerKit QUERY_RUNNER_KIT = TestQueryRunnerKit.DEFAULT;
  private static final Map<String, Object> NO_PROJECTIONS_CONTEXT = Map.of(QueryContexts.NO_PROJECTIONS, "true");
  private static final Map<String, Object> FORCE_PROJECTION_CONTEXT = Map.of(QueryContexts.FORCE_PROJECTION, "true");

  private static List<Arguments> getAllQueryableIndex()
  {
    // make sure the index is built on main thread, so it won't be closed by the test framework.
    return TestIndex.queryableIndexSupplierMap(true)
                    .entrySet()
                    .stream()
                    .map(entry -> Arguments.of(entry.getKey(), entry.getValue().get()))
                    .collect(Collectors.toList());
  }

  private static List<Arguments> getAllIncrementalIndex()
  {
    // make sure the index is built on main thread, so it won't be closed by the test framework.
    return TestIndex.incrementalIndexSupplierMap(true)
                    .entrySet()
                    .stream()
                    .filter(entry -> !entry.getKey()
                                           .equals("rtPartialSchemaStringDiscoveryIndex")) // exclude this index since it doesn't have our projection dimensions
                    .map(entry -> Arguments.of(entry.getKey(), entry.getValue().get()))
                    .collect(Collectors.toList());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getAllQueryableIndex")
  void testProjectionMatch_withQueryableIndex(String indexName, QueryableIndex index)
  {
    QueryableIndex indexSpy = Mockito.spy(index);
    ReferenceCountedSegmentProvider segmentProvider = ReferenceCountedSegmentProvider.wrapRootGenerationSegment(
        new QueryableIndexSegment(indexSpy, TestIndex.SEGMENT_ID));
    ArgumentCaptor<CursorBuildSpec> cursorCaptor = ArgumentCaptor.forClass(CursorBuildSpec.class);
    Druids.TimeseriesQueryBuilder queryBuilder =
        Druids.newTimeseriesQueryBuilder()
              .dataSource(TestIndex.DATA_SOURCE)
              .intervals("2011-01-20/2011-01-22")
              .aggregators(new CountAggregatorFactory("count"));

    final Query<?> noProjectionQuery = queryBuilder.context(NO_PROJECTIONS_CONTEXT).build();
    List<?> noProjectionResult = QUERY_RUNNER_KIT.run(segmentProvider, noProjectionQuery, indexName).toList();
    Mockito.verify(indexSpy, times(1)).getProjection(cursorCaptor.capture());
    Assertions.assertNull(index.getProjection(cursorCaptor.getValue()));

    final Query<?> projectionquery = queryBuilder.context(FORCE_PROJECTION_CONTEXT).build();
    List<?> projectionResult = QUERY_RUNNER_KIT.run(segmentProvider, projectionquery, indexName).toList();
    Mockito.verify(indexSpy, times(2)).getProjection(cursorCaptor.capture());
    Assertions.assertNotNull(index.getProjection(cursorCaptor.getValue()));

    Assertions.assertEquals(noProjectionResult, projectionResult);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getAllIncrementalIndex")
  void testProjectionMatch_withIncrementalIndex(String indexName, IncrementalIndex index)
  {
    IncrementalIndex indexSpy = Mockito.spy(index);
    ReferenceCountedSegmentProvider segmentProvider = ReferenceCountedSegmentProvider.wrapRootGenerationSegment(
        new IncrementalIndexSegment(indexSpy, TestIndex.SEGMENT_ID));
    ArgumentCaptor<CursorBuildSpec> cursorCaptor = ArgumentCaptor.forClass(CursorBuildSpec.class);
    Druids.TimeseriesQueryBuilder queryBuilder =
        Druids.newTimeseriesQueryBuilder()
              .dataSource(TestIndex.DATA_SOURCE)
              .intervals("2011-01-20/2011-01-22")
              .aggregators(new CountAggregatorFactory("count"));

    final TimeseriesQuery noProjectionQuery = queryBuilder.context(NO_PROJECTIONS_CONTEXT).build();
    List<?> noProjectionResult = QUERY_RUNNER_KIT.run(segmentProvider, noProjectionQuery, indexName).toList();
    Mockito.verify(indexSpy, times(1)).getProjection(cursorCaptor.capture());
    // Query specified not to use projections, so it should return null
    Assertions.assertNull(index.getProjection(cursorCaptor.getValue()));

    final TimeseriesQuery projectionquery = queryBuilder.context(FORCE_PROJECTION_CONTEXT).build();
    List<?> projectionResult = QUERY_RUNNER_KIT.run(segmentProvider, projectionquery, indexName).toList();
    Mockito.verify(indexSpy, times(2)).getProjection(cursorCaptor.capture());
    Assertions.assertNotNull(index.getProjection(cursorCaptor.getValue()));
    // The results should be the same whether projections are used or not
    Assertions.assertEquals(noProjectionResult, projectionResult);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getAllQueryableIndex")
  void testProjectionNoMatch_withQueryableIndexOnUnnest(String indexName, QueryableIndex index)
  {
    QueryableIndex indexSpy = Mockito.spy(index);
    ReferenceCountedSegmentProvider segmentProvider = ReferenceCountedSegmentProvider.wrapRootGenerationSegment(
        new QueryableIndexSegment(indexSpy, TestIndex.SEGMENT_ID));
    ArgumentCaptor<CursorBuildSpec> cursorCaptor = ArgumentCaptor.forClass(CursorBuildSpec.class);
    UnnestDataSource unnestDataSource =
        UnnestDataSource.create(
            new TableDataSource(TestIndex.DATA_SOURCE),
            new ExpressionVirtualColumn(
                QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
                "\"" + QueryRunnerTestHelper.PLACEMENTISH_DIMENSION + "\"",
                null,
                ExprMacroTable.nil()
            ),
            null
        );
    GroupByQuery.Builder queryBuilder = GroupByQuery.builder()
                                                    .setDataSource(unnestDataSource)
                                                    .setInterval("2011-01-20/2011-01-22")
                                                    .setDimensions(new DefaultDimensionSpec("nonexistent0", "alias0"))
                                                    .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                                    .setGranularity(Granularities.ALL);

    GroupByQuery bestEffortProjectionQuery = queryBuilder.build();
    QUERY_RUNNER_KIT.run(segmentProvider, bestEffortProjectionQuery, indexName).toList();
    Mockito.verify(indexSpy, times(1)).getProjection(cursorCaptor.capture());
    // No projection match, so it should return null
    Assertions.assertNull(index.getProjection(cursorCaptor.getValue()));

    GroupByQuery forceProjectionquery = queryBuilder.setContext(FORCE_PROJECTION_CONTEXT).build();
    DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> QUERY_RUNNER_KIT.run(segmentProvider, forceProjectionquery, indexName).toList()
    );
    Assertions.assertEquals("Force projections specified, but none satisfy query", e.getMessage());
    Mockito.verify(indexSpy, times(2)).getProjection(cursorCaptor.capture());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getAllQueryableIndex")
  void testProjectionMatch_withQueryableIndexOnRestricted(String indexName, QueryableIndex index)
  {
    QueryableIndex indexSpy = Mockito.spy(index);
    ReferenceCountedSegmentProvider segmentProvider = ReferenceCountedSegmentProvider.wrapRootGenerationSegment(
        new QueryableIndexSegment(indexSpy, TestIndex.SEGMENT_ID));
    ArgumentCaptor<CursorBuildSpec> cursorCaptor = ArgumentCaptor.forClass(CursorBuildSpec.class);
    RestrictedDataSource restrictDataSource = RestrictedDataSource.create(
        new TableDataSource(TestIndex.DATA_SOURCE),
        RowFilterPolicy.from(new SelectorDimFilter("market", "spot", null))
    );
    TopNQueryBuilder queryBuilder =
        new TopNQueryBuilder()
            .dataSource(restrictDataSource)
            .virtualColumns(new ExpressionVirtualColumn(
                "daily_market",
                "concat(market, '_', timestamp_format(timestamp_floor(__time,'P1D',NULL,'UTC'),'yyyy-MM-dd'))",
                ColumnType.STRING,
                new ExprMacroTable(List.of(new TimestampFloorExprMacro(), new TimestampFormatExprMacro()))
            ))
            .dimension("daily_market")
            .metric("qualityLong")
            .threshold(10)
            .intervals("2011-01-20/2011-01-30")
            .aggregators(new LongMaxAggregatorFactory("qualityLong", "qualityLong"));

    final TopNQuery noProjectionQuery = queryBuilder.context(NO_PROJECTIONS_CONTEXT).build();
    List<?> noProjectionResult = QUERY_RUNNER_KIT.run(segmentProvider, noProjectionQuery, indexName).toList();
    Mockito.verify(indexSpy, times(1)).getProjection(cursorCaptor.capture());
    // Query specified not to use projections, so it should return null
    Assertions.assertNull(index.getProjection(cursorCaptor.getValue()));

    final TopNQuery projectionquery = queryBuilder.context(FORCE_PROJECTION_CONTEXT).build();
    List<?> projectionResult = QUERY_RUNNER_KIT.run(segmentProvider, projectionquery, indexName).toList();
    Mockito.verify(indexSpy, times(2)).getProjection(cursorCaptor.capture());
    Assertions.assertNotNull(index.getProjection(cursorCaptor.getValue()));
    // The results should be the same whether projections are used or not
    Assertions.assertEquals(noProjectionResult, projectionResult);
  }


  @ParameterizedTest(name = "{0}")
  @MethodSource("getAllQueryableIndex")
  void testProjectionNoMatch_withQueryableIndexOnRestricted(String indexName, QueryableIndex index)
  {
    QueryableIndex indexSpy = Mockito.spy(index);
    ReferenceCountedSegmentProvider segmentProvider = ReferenceCountedSegmentProvider.wrapRootGenerationSegment(
        new QueryableIndexSegment(indexSpy, TestIndex.SEGMENT_ID));
    ArgumentCaptor<CursorBuildSpec> cursorCaptor = ArgumentCaptor.forClass(CursorBuildSpec.class);
    RestrictedDataSource restrictDataSource = RestrictedDataSource.create(
        new TableDataSource(TestIndex.DATA_SOURCE),
        RowFilterPolicy.from(new SelectorDimFilter("placement", "preferred", null))
    );
    TopNQueryBuilder queryBuilder =
        new TopNQueryBuilder()
            .dataSource(restrictDataSource)
            .virtualColumns(new ExpressionVirtualColumn(
                "daily_market",
                "concat(market, '_', timestamp_format(timestamp_floor(__time,'P1D',NULL,'UTC'),'yyyy-MM-dd'))",
                ColumnType.STRING,
                new ExprMacroTable(List.of(new TimestampFloorExprMacro(), new TimestampFormatExprMacro()))
            ))
            .dimension("daily_market")
            .metric("qualityLong")
            .threshold(10)
            .intervals("2011-01-20/2011-01-30")
            .aggregators(new LongMaxAggregatorFactory("qualityLong", "qualityLong"));


    TopNQuery bestEffortProjectionQuery = queryBuilder.build();
    QUERY_RUNNER_KIT.run(segmentProvider, bestEffortProjectionQuery, indexName).toList();
    Mockito.verify(indexSpy, times(1)).getProjection(cursorCaptor.capture());
    // No projection match, so it should return null
    Assertions.assertNull(index.getProjection(cursorCaptor.getValue()));

    TopNQuery forceProjectionquery = queryBuilder.context(FORCE_PROJECTION_CONTEXT).build();
    DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> QUERY_RUNNER_KIT.run(segmentProvider, forceProjectionquery, indexName).toList()
    );
    Assertions.assertEquals("Force projections specified, but none satisfy query", e.getMessage());
    Mockito.verify(indexSpy, times(2)).getProjection(cursorCaptor.capture());
  }
}

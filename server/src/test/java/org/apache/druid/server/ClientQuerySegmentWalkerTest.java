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

package org.apache.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FrameBasedInlineDataSource;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChestTestHelper;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.FrameBasedInlineSegmentWrangler;
import org.apache.druid.segment.InlineSegmentWrangler;
import org.apache.druid.segment.MapSegmentWrangler;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.ComparableList;
import org.apache.druid.segment.data.ComparableStringArray;
import org.apache.druid.segment.join.FrameBasedInlineJoinableFactory;
import org.apache.druid.segment.join.InlineJoinableFactory;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.Joinable;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.join.MapJoinableFactory;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.scheduling.ManualQueryPrioritizationStrategy;
import org.apache.druid.server.scheduling.NoQueryLaningStrategy;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Tests ClientQuerySegmentWalker.
 *
 * Note that since SpecificSegmentsQuerySegmentWalker in the druid-sql module also uses ClientQuerySegmentWalker, it's
 * also exercised pretty well by the SQL tests (especially CalciteQueryTest). This class adds an extra layer of testing.
 * In particular, this class makes it easier to add tests that validate queries are *made* in the correct way, not just
 * that they return the correct results.
 */
public class ClientQuerySegmentWalkerTest
{
  private static final Logger log = new Logger(ClientQuerySegmentWalkerTest.class);

  private static final String FOO = "foo";
  private static final String BAR = "bar";
  private static final String MULTI = "multi";
  private static final String GLOBAL = "broadcast";
  private static final String ARRAY = "array";
  private static final String ARRAY_UNKNOWN = "array_nulls";

  private static final Interval INTERVAL = Intervals.of("2000/P1Y");
  private static final String VERSION = "A";
  private static final ShardSpec SHARD_SPEC = new NumberedShardSpec(0, 1);


  private static final InlineDataSource FOO_INLINE = InlineDataSource.fromIterable(
      ImmutableList.<Object[]>builder()
          .add(new Object[]{INTERVAL.getStartMillis(), "x", 1})
          .add(new Object[]{INTERVAL.getStartMillis(), "x", 2})
          .add(new Object[]{INTERVAL.getStartMillis(), "y", 3})
          .add(new Object[]{INTERVAL.getStartMillis(), "z", 4})
          .build(),
      RowSignature.builder()
                  .addTimeColumn()
                  .add("s", ColumnType.STRING)
                  .add("n", ColumnType.LONG)
                  .build()
  );

  private static final InlineDataSource BAR_INLINE = InlineDataSource.fromIterable(
      ImmutableList.<Object[]>builder()
          .add(new Object[]{INTERVAL.getStartMillis(), "a", 1})
          .add(new Object[]{INTERVAL.getStartMillis(), "a", 2})
          .add(new Object[]{INTERVAL.getStartMillis(), "b", 3})
          .add(new Object[]{INTERVAL.getStartMillis(), "c", 4})
          .build(),
      RowSignature.builder()
                  .addTimeColumn()
                  .add("s", ColumnType.STRING)
                  .add("n", ColumnType.LONG)
                  .build()
  );

  private static final InlineDataSource MULTI_VALUE_INLINE = InlineDataSource.fromIterable(
      ImmutableList.<Object[]>builder()
          .add(new Object[]{INTERVAL.getStartMillis(), ImmutableList.of("a", "b"), 1})
          .add(new Object[]{INTERVAL.getStartMillis(), ImmutableList.of("a", "c"), 2})
          .add(new Object[]{INTERVAL.getStartMillis(), ImmutableList.of("b"), 3})
          .add(new Object[]{INTERVAL.getStartMillis(), ImmutableList.of("c"), 4})
          .build(),
      RowSignature.builder()
                  .addTimeColumn()
                  .add("s", ColumnType.STRING)
                  .add("n", ColumnType.LONG)
                  .build()
  );


  private static final List<Object[]> ARRAY_INLINE_ROWS =
      ImmutableList.<Object[]>builder()
          .add(new Object[]{INTERVAL.getStartMillis(), "x", 1, ImmutableList.of(1.0, 2.0), ImmutableList.of(1L, 2L), ImmutableList.of("1.0", "2.0")})
          .add(new Object[]{INTERVAL.getStartMillis(), "x", 2, ImmutableList.of(2.0, 4.0), ImmutableList.of(2L, 4L), ImmutableList.of("2.0", "4.0")})
          .add(new Object[]{INTERVAL.getStartMillis(), "y", 3, ImmutableList.of(3.0, 6.0), ImmutableList.of(3L, 6L), ImmutableList.of("3.0", "6.0")})
          .add(new Object[]{INTERVAL.getStartMillis(), "z", 4, ImmutableList.of(4.0, 8.0), ImmutableList.of(4L, 8L), ImmutableList.of("4.0", "8.0")})
          .build();

  private static final InlineDataSource ARRAY_INLINE = InlineDataSource.fromIterable(
      ARRAY_INLINE_ROWS,
      RowSignature.builder()
                  .addTimeColumn()
                  .add("s", ColumnType.STRING)
                  .add("n", ColumnType.LONG)
                  .add("ad", ColumnType.DOUBLE_ARRAY)
                  .add("al", ColumnType.LONG_ARRAY)
                  .add("as", ColumnType.STRING_ARRAY)
                  .build()
  );


  private static final InlineDataSource ARRAY_INLINE_UNKNOWN = InlineDataSource.fromIterable(
      ARRAY_INLINE_ROWS,
      RowSignature.builder()
                  .addTimeColumn()
                  .add("s", ColumnType.STRING)
                  .add("n", ColumnType.LONG)
                  .add("ad", null)
                  .add("al", null)
                  .add("as", null)
                  .build()
  );

  private static final String DUMMY_QUERY_ID = "dummyQueryId";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private Closer closer;
  private QueryRunnerFactoryConglomerate conglomerate;

  // Queries that are issued; checked by "testQuery" against its "expectedQueries" parameter.
  private final List<ExpectedQuery> issuedQueries = new ArrayList<>();

  // A ClientQuerySegmentWalker that has two segments: one for FOO and one for BAR; each with interval INTERVAL,
  // version VERSION, and shard spec SHARD_SPEC.
  private ClientQuerySegmentWalker walker;

  private ObservableQueryScheduler scheduler;

  @Before
  public void setUp()
  {
    closer = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(closer);
    scheduler = new ObservableQueryScheduler(
        8,
        ManualQueryPrioritizationStrategy.INSTANCE,
        NoQueryLaningStrategy.INSTANCE,
        new ServerConfig(false)
    );
    initWalker(ImmutableMap.of(), scheduler);
  }

  @After
  public void tearDown() throws IOException
  {
    closer.close();
  }

  @Test
  public void testTimeseriesOnTable()
  {
    final TimeseriesQuery query =
        (TimeseriesQuery) Druids.newTimeseriesQueryBuilder()
                                .dataSource(FOO)
                                .granularity(Granularities.ALL)
                                .intervals(Collections.singletonList(INTERVAL))
                                .aggregators(new LongSumAggregatorFactory("sum", "n"))
                                .context(ImmutableMap.of(TimeseriesQuery.CTX_GRAND_TOTAL, false))
                                .build()
                                .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of(new Object[]{INTERVAL.getStartMillis(), 10L})
    );

    Assert.assertEquals(1, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(1, scheduler.getTotalAcquired().get());
    Assert.assertEquals(1, scheduler.getTotalReleased().get());
  }

  @Test
  public void testTimeseriesOnAutomaticGlobalTable()
  {
    final TimeseriesQuery query =
        (TimeseriesQuery) Druids.newTimeseriesQueryBuilder()
                                .dataSource(GLOBAL)
                                .granularity(Granularities.ALL)
                                .intervals(Collections.singletonList(INTERVAL))
                                .aggregators(new LongSumAggregatorFactory("sum", "n"))
                                .context(ImmutableMap.of(TimeseriesQuery.CTX_GRAND_TOTAL, false))
                                .build()
                                .withId(DUMMY_QUERY_ID);

    // expect global/joinable datasource to be automatically translated into a GlobalTableDataSource
    final TimeseriesQuery expectedClusterQuery =
        (TimeseriesQuery) Druids.newTimeseriesQueryBuilder()
                                .dataSource(new GlobalTableDataSource(GLOBAL))
                                .granularity(Granularities.ALL)
                                .intervals(Collections.singletonList(INTERVAL))
                                .aggregators(new LongSumAggregatorFactory("sum", "n"))
                                .context(ImmutableMap.of(TimeseriesQuery.CTX_GRAND_TOTAL, false))
                                .build()
                                .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(expectedClusterQuery)),
        ImmutableList.of(new Object[]{INTERVAL.getStartMillis(), 10L})
    );

    Assert.assertEquals(1, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(1, scheduler.getTotalAcquired().get());
    Assert.assertEquals(1, scheduler.getTotalReleased().get());
  }

  @Test
  public void testTimeseriesOnInline()
  {
    final TimeseriesQuery query =
        (TimeseriesQuery) Druids.newTimeseriesQueryBuilder()
                                .dataSource(FOO_INLINE)
                                .granularity(Granularities.ALL)
                                .intervals(Collections.singletonList(INTERVAL))
                                .aggregators(new LongSumAggregatorFactory("sum", "n"))
                                .build()
                                .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.local(query)),
        ImmutableList.of(new Object[]{INTERVAL.getStartMillis(), 10L})
    );

    Assert.assertEquals(1, scheduler.getTotalRun().get());
    Assert.assertEquals(0, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(1, scheduler.getTotalAcquired().get());
    Assert.assertEquals(1, scheduler.getTotalReleased().get());
  }

  @Test
  public void testTimeseriesOnGroupByOnTable()
  {
    final GroupByQuery subquery =
        GroupByQuery.builder()
                    .setDataSource(FOO)
                    .setGranularity(Granularities.ALL)
                    .setInterval(Collections.singletonList(INTERVAL))
                    .setDimensions(DefaultDimensionSpec.of("s"))
                    .build();

    final TimeseriesQuery query =
        (TimeseriesQuery) Druids.newTimeseriesQueryBuilder()
                                .dataSource(new QueryDataSource(subquery))
                                .granularity(Granularities.ALL)
                                .intervals(Intervals.ONLY_ETERNITY)
                                .aggregators(new CountAggregatorFactory("cnt"))
                                .build()
                                .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        new ArrayList<>(ImmutableList.of(
            ExpectedQuery.cluster(subquery.withId(DUMMY_QUERY_ID).withSubQueryId("1.1")),
            ExpectedQuery.local(
                query.withDataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(new Object[]{"x"}, new Object[]{"y"}, new Object[]{"z"}),
                        RowSignature.builder().add("s", ColumnType.STRING).build()
                    )
                )
            )
        )),
        ImmutableList.of(new Object[]{Intervals.ETERNITY.getStartMillis(), 3L})
    );

    // note: this should really be 1, but in the interim queries that are composed of multiple queries count each
    // invocation of either the cluster or local walker in ClientQuerySegmentWalker
    Assert.assertEquals(2, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(2, scheduler.getTotalAcquired().get());
    Assert.assertEquals(2, scheduler.getTotalReleased().get());
  }

  @Test
  public void testGroupByOnGroupByOnTable()
  {
    final GroupByQuery subquery =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(FOO)
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Collections.singletonList(INTERVAL))
                                   .setDimensions(DefaultDimensionSpec.of("s"))
                                   .build()
                                   .withId("queryId");

    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(new QueryDataSource(subquery))
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Intervals.ONLY_ETERNITY)
                                   .setAggregatorSpecs(new CountAggregatorFactory("cnt"))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        // GroupBy handles its own subqueries; only the inner one will go to the cluster. Also, it gets a subquery id
        ImmutableList.of(ExpectedQuery.cluster(subquery.withSubQueryId("1.1"))),
        ImmutableList.of(new Object[]{3L})
    );

    Assert.assertEquals(1, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(1, scheduler.getTotalAcquired().get());
    Assert.assertEquals(1, scheduler.getTotalReleased().get());
  }

  @Test
  public void testGroupByOnUnionOfTwoTables()
  {
    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(
                                       new UnionDataSource(
                                           ImmutableList.of(
                                               new TableDataSource(FOO),
                                               new TableDataSource(BAR)
                                           )
                                       )
                                   )
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Intervals.ONLY_ETERNITY)
                                   .setDimensions(DefaultDimensionSpec.of("s"))
                                   .setAggregatorSpecs(new CountAggregatorFactory("cnt"))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        ImmutableList.of(
            ExpectedQuery.cluster(query.withDataSource(new TableDataSource(FOO)).withSubQueryId("foo.1")),
            ExpectedQuery.cluster(query.withDataSource(new TableDataSource(BAR)).withSubQueryId("bar.2"))
        ),
        ImmutableList.of(
            new Object[]{"a", 2L},
            new Object[]{"b", 1L},
            new Object[]{"c", 1L},
            new Object[]{"x", 2L},
            new Object[]{"y", 1L},
            new Object[]{"z", 1L}
        )
    );

    // note: this should really be 1, but in the interim queries that are composed of multiple queries count each
    // invocation of either the cluster or local walker in ClientQuerySegmentWalker
    Assert.assertEquals(2, scheduler.getTotalRun().get());
    Assert.assertEquals(2, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(2, scheduler.getTotalAcquired().get());
    Assert.assertEquals(2, scheduler.getTotalReleased().get());
  }

  @Test
  public void testGroupByOnUnionOfOneTable()
  {
    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(
                                       new UnionDataSource(ImmutableList.of(new TableDataSource(FOO)))
                                   )
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Intervals.ONLY_ETERNITY)
                                   .setDimensions(DefaultDimensionSpec.of("s"))
                                   .setAggregatorSpecs(new CountAggregatorFactory("cnt"))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        ImmutableList.of(
            ExpectedQuery.cluster(query.withDataSource(new TableDataSource(FOO)))
        ),
        ImmutableList.of(
            new Object[]{"x", 2L},
            new Object[]{"y", 1L},
            new Object[]{"z", 1L}
        )
    );

    Assert.assertEquals(1, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(1, scheduler.getTotalAcquired().get());
    Assert.assertEquals(1, scheduler.getTotalReleased().get());
  }

  @Test
  public void testJoinOnGroupByOnTable()
  {
    final GroupByQuery subquery =
        GroupByQuery.builder()
                    .setDataSource(FOO)
                    .setGranularity(Granularities.ALL)
                    .setInterval(Collections.singletonList(INTERVAL))
                    .setDimensions(DefaultDimensionSpec.of("s"))
                    .setDimFilter(new SelectorDimFilter("s", "y", null))
                    .build();

    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(
                                       JoinDataSource.create(
                                           new TableDataSource(FOO),
                                           new QueryDataSource(subquery),
                                           "j.",
                                           "\"j.s\" == \"s\"",
                                           JoinType.INNER,
                                           null,
                                           ExprMacroTable.nil(),
                                           new JoinableFactoryWrapper(QueryStackTests.makeJoinableFactoryFromDefault(
                                               null,
                                               null,
                                               null))
                                       )
                                   )
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Intervals.ONLY_ETERNITY)
                                   .setDimensions(DefaultDimensionSpec.of("s"), DefaultDimensionSpec.of("j.s"))
                                   .setAggregatorSpecs(new CountAggregatorFactory("cnt"))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        ImmutableList.of(
            ExpectedQuery.cluster(subquery.withId(DUMMY_QUERY_ID).withSubQueryId("2.1")),
            ExpectedQuery.cluster(
                query.withDataSource(
                    query.getDataSource().withChildren(
                        ImmutableList.of(
                            query.getDataSource().getChildren().get(0),
                            InlineDataSource.fromIterable(
                                ImmutableList.of(new Object[]{"y"}),
                                RowSignature.builder().add("s", ColumnType.STRING).build()
                            )
                        )
                    )
                )
            )
        ),
        ImmutableList.of(new Object[]{"y", "y", 1L})
    );

    // note: this should really be 1, but in the interim queries that are composed of multiple queries count each
    // invocation of either the cluster or local walker in ClientQuerySegmentWalker
    Assert.assertEquals(2, scheduler.getTotalRun().get());
    Assert.assertEquals(2, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(2, scheduler.getTotalAcquired().get());
    Assert.assertEquals(2, scheduler.getTotalReleased().get());
  }

  @Test
  public void testJoinOnGroupByOnUnionOfTables()
  {
    final UnionDataSource unionDataSource = new UnionDataSource(
        ImmutableList.of(
            new TableDataSource(FOO),
            new TableDataSource(BAR)
        )
    );

    final GroupByQuery subquery =
        GroupByQuery.builder()
                    .setDataSource(unionDataSource)
                    .setGranularity(Granularities.ALL)
                    .setInterval(Collections.singletonList(INTERVAL))
                    .setDimensions(DefaultDimensionSpec.of("s"))
                    .setDimFilter(new SelectorDimFilter("s", "y", null))
                    .build();

    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(
                                       JoinDataSource.create(
                                           unionDataSource,
                                           new QueryDataSource(subquery),
                                           "j.",
                                           "\"j.s\" == \"s\"",
                                           JoinType.INNER,
                                           null,
                                           ExprMacroTable.nil(),
                                           new JoinableFactoryWrapper(QueryStackTests.makeJoinableFactoryFromDefault(
                                               null,
                                               null,
                                               null))
                                       )
                                   )
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Intervals.ONLY_ETERNITY)
                                   .setDimensions(DefaultDimensionSpec.of("s"), DefaultDimensionSpec.of("j.s"))
                                   .setAggregatorSpecs(new CountAggregatorFactory("cnt"))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        ImmutableList.of(
            ExpectedQuery.cluster(
                subquery
                    .withDataSource(subquery.getDataSource().getChildren().get(0))
                    .withId(DUMMY_QUERY_ID)
                    .withSubQueryId("2.1.foo.1")
            ),
            ExpectedQuery.cluster(
                subquery
                    .withDataSource(subquery.getDataSource().getChildren().get(1))
                    .withId(DUMMY_QUERY_ID)
                    .withSubQueryId("2.1.bar.2")
            ),
            ExpectedQuery.cluster(
                query.withDataSource(
                    query.getDataSource().withChildren(
                        ImmutableList.of(
                            unionDataSource.getChildren().get(0),
                            InlineDataSource.fromIterable(
                                ImmutableList.of(new Object[]{"y"}),
                                RowSignature.builder().add("s", ColumnType.STRING).build()
                            )
                        )
                    )
                ).withSubQueryId("foo.1")
            ),
            ExpectedQuery.cluster(
                query.withDataSource(
                    query.getDataSource().withChildren(
                        ImmutableList.of(
                            unionDataSource.getChildren().get(1),
                            InlineDataSource.fromIterable(
                                ImmutableList.of(new Object[]{"y"}),
                                RowSignature.builder().add("s", ColumnType.STRING).build()
                            )
                        )
                    )
                ).withSubQueryId("bar.2")
            )
        ),
        ImmutableList.of(new Object[]{"y", "y", 1L})
    );

    // note: this should really be 1, but in the interim queries that are composed of multiple queries count each
    // invocation of either the cluster or local walker in ClientQuerySegmentWalker
    Assert.assertEquals(4, scheduler.getTotalRun().get());
    Assert.assertEquals(4, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(4, scheduler.getTotalAcquired().get());
    Assert.assertEquals(4, scheduler.getTotalReleased().get());
  }

  @Test
  public void testGroupByOnScanMultiValue()
  {
    ScanQuery subquery = new Druids.ScanQueryBuilder().dataSource(MULTI)
                                                      .columns("s", "n")
                                                      .eternityInterval()
                                                      .legacy(false)
                                                      .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                                      .build();
    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(new QueryDataSource(subquery))
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Intervals.ONLY_ETERNITY)
                                   .setDimensions(DefaultDimensionSpec.of("s"))
                                   .setAggregatorSpecs(new LongSumAggregatorFactory("sum_n", "n"))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        // GroupBy handles its own subqueries; only the inner one will go to the cluster.
        ImmutableList.of(
            ExpectedQuery.cluster(subquery.withId(DUMMY_QUERY_ID).withSubQueryId("1.1")),
            ExpectedQuery.local(
                query.withDataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(
                            new Object[]{ImmutableList.of("a", "b"), 1},
                            new Object[]{ImmutableList.of("a", "c"), 2},
                            new Object[]{ImmutableList.of("b"), 3},
                            new Object[]{ImmutableList.of("c"), 4}
                        ),
                        RowSignature.builder().add("s", null).add("n", null).build()
                    )
                )
            )
        ),
        ImmutableList.of(
            new Object[]{"a", 3L},
            new Object[]{"b", 4L},
            new Object[]{"c", 6L}
        )
    );

    Assert.assertEquals(2, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(2, scheduler.getTotalAcquired().get());
    Assert.assertEquals(2, scheduler.getTotalReleased().get());
  }

  @Test
  public void testTopNScanMultiValue()
  {
    ScanQuery subquery = new Druids.ScanQueryBuilder().dataSource(MULTI)
                                                      .columns("s", "n")
                                                      .eternityInterval()
                                                      .legacy(false)
                                                      .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                                      .build();
    final TopNQuery query =
        (TopNQuery) new TopNQueryBuilder().dataSource(new QueryDataSource(subquery))
                                          .granularity(Granularities.ALL)
                                          .intervals(Intervals.ONLY_ETERNITY)
                                          .dimension(DefaultDimensionSpec.of("s"))
                                          .metric("sum_n")
                                          .threshold(100)
                                          .aggregators(new LongSumAggregatorFactory("sum_n", "n"))
                                          .build()
                                          .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        ImmutableList.of(
            ExpectedQuery.cluster(subquery.withId(DUMMY_QUERY_ID).withSubQueryId("1.1")),
            ExpectedQuery.local(
                query.withDataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(
                            new Object[]{ImmutableList.of("a", "b"), 1},
                            new Object[]{ImmutableList.of("a", "c"), 2},
                            new Object[]{ImmutableList.of("b"), 3},
                            new Object[]{ImmutableList.of("c"), 4}
                        ),
                        RowSignature.builder().add("s", null).add("n", null).build()
                    )
                )
            )
        ),
        ImmutableList.of(
            new Object[]{Intervals.ETERNITY.getStartMillis(), "c", 6L},
            new Object[]{Intervals.ETERNITY.getStartMillis(), "b", 4L},
            new Object[]{Intervals.ETERNITY.getStartMillis(), "a", 3L}
        )
    );

    Assert.assertEquals(2, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(2, scheduler.getTotalAcquired().get());
    Assert.assertEquals(2, scheduler.getTotalReleased().get());
  }

  @Test
  public void testJoinOnTableErrorCantInlineTable()
  {
    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(
                                       JoinDataSource.create(
                                           new TableDataSource(FOO),
                                           new TableDataSource(BAR),
                                           "j.",
                                           "\"j.s\" == \"s\"",
                                           JoinType.INNER,
                                           null,
                                           ExprMacroTable.nil(),
                                           null
                                       )
                                   )
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Intervals.ONLY_ETERNITY)
                                   .setDimensions(DefaultDimensionSpec.of("s"), DefaultDimensionSpec.of("j.s"))
                                   .setAggregatorSpecs(new CountAggregatorFactory("cnt"))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Cannot handle subquery structure for dataSource");

    testQuery(query, ImmutableList.of(), ImmutableList.of());
  }

  @Test
  public void testTimeseriesOnGroupByOnTableErrorTooManyRows()
  {
    initWalker(ImmutableMap.of("maxSubqueryRows", "2"));

    final GroupByQuery subquery =
        GroupByQuery.builder()
                    .setDataSource(FOO)
                    .setGranularity(Granularities.ALL)
                    .setInterval(Collections.singletonList(INTERVAL))
                    .setDimensions(DefaultDimensionSpec.of("s"))
                    .build();

    final TimeseriesQuery query =
        (TimeseriesQuery) Druids.newTimeseriesQueryBuilder()
                                .dataSource(new QueryDataSource(subquery))
                                .granularity(Granularities.ALL)
                                .intervals(Intervals.ONLY_ETERNITY)
                                .aggregators(new CountAggregatorFactory("cnt"))
                                .build()
                                .withId(DUMMY_QUERY_ID);

    expectedException.expect(ResourceLimitExceededException.class);
    expectedException.expectMessage("Subquery generated results beyond maximum[2] rows");

    testQuery(query, ImmutableList.of(), ImmutableList.of());
  }

  @Test // Regression test for bug fixed in https://github.com/apache/druid/pull/15300
  public void testScanOnScanWithStringExpression()
  {
    initWalker(
        ImmutableMap.of(QueryContexts.MAX_SUBQUERY_ROWS_KEY, "1", QueryContexts.MAX_SUBQUERY_BYTES_KEY, "1000"),
        scheduler
    );

    final Query<?> subquery =
        Druids.newScanQueryBuilder()
              .dataSource(FOO)
              .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
              .columns("s")
              .legacy(false)
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
              .build()
              .withId(DUMMY_QUERY_ID);

    final Query<?> query =
        Druids.newScanQueryBuilder()
              .dataSource(new QueryDataSource(subquery))
              .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
              .virtualColumns(
                  new ExpressionVirtualColumn(
                      "v",
                      "case_searched(s == 'x',2,3)",
                      ColumnType.LONG,
                      ExprMacroTable.nil()
                  )
              )
              .columns("v")
              .legacy(false)
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
              .build()
              .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        ImmutableList.of(
            ExpectedQuery.cluster(subquery.withId(DUMMY_QUERY_ID).withSubQueryId("1.1")),
            ExpectedQuery.local(
                query.withDataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(
                            new Object[]{"x"},
                            new Object[]{"x"},
                            new Object[]{"y"},
                            new Object[]{"z"}
                        ),
                        RowSignature.builder().add("s", null).build()
                    )
                )
            )
        ),
        ImmutableList.of(
            new Object[]{2L},
            new Object[]{2L},
            new Object[]{3L},
            new Object[]{3L}
        )
    );

    Assert.assertEquals(2, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(2, scheduler.getTotalAcquired().get());
    Assert.assertEquals(2, scheduler.getTotalReleased().get());
  }

  @Test
  public void testTimeseriesOnGroupByOnTableErrorTooLarge()
  {
    final GroupByQuery subquery =
        GroupByQuery.builder()
                    .setDataSource(FOO)
                    .setGranularity(Granularities.ALL)
                    .setInterval(Collections.singletonList(INTERVAL))
                    .setDimensions(DefaultDimensionSpec.of("s"))
                    .build();

    final TimeseriesQuery query =
        (TimeseriesQuery) Druids.newTimeseriesQueryBuilder()
                                .dataSource(new QueryDataSource(subquery))
                                .granularity(Granularities.ALL)
                                .intervals(Intervals.ONLY_ETERNITY)
                                .aggregators(new CountAggregatorFactory("cnt"))
                                .context(ImmutableMap.of(QueryContexts.MAX_SUBQUERY_BYTES_KEY, "1"))
                                .build()
                                .withId(DUMMY_QUERY_ID);

    expectedException.expect(ResourceLimitExceededException.class);
    expectedException.expectMessage("Subquery generated results beyond maximum[1] bytes");

    testQuery(query, ImmutableList.of(), ImmutableList.of());
  }

  @Test
  public void testGroupByOnArraysDoubles()
  {
    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(ARRAY)
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Collections.singletonList(INTERVAL))
                                   .setDimensions(
                                       new DefaultDimensionSpec(
                                           "ad",
                                           "ad",
                                           ColumnType.DOUBLE_ARRAY
                                       ))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of(
            new Object[]{new ComparableList(ImmutableList.of(1.0, 2.0))},
            new Object[]{new ComparableList(ImmutableList.of(2.0, 4.0))},
            new Object[]{new ComparableList(ImmutableList.of(3.0, 6.0))},
            new Object[]{new ComparableList(ImmutableList.of(4.0, 8.0))}
        )
    );
  }

  @Test
  public void testGroupByOnArraysDoublesAsString()
  {
    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(ARRAY)
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Collections.singletonList(INTERVAL))
                                   .setDimensions(DefaultDimensionSpec.of("ad"))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of(
            new Object[]{new ComparableList(ImmutableList.of(1.0, 2.0)).toString()},
            new Object[]{new ComparableList(ImmutableList.of(2.0, 4.0)).toString()},
            new Object[]{new ComparableList(ImmutableList.of(3.0, 6.0)).toString()},
            new Object[]{new ComparableList(ImmutableList.of(4.0, 8.0)).toString()}
        )
    );
  }

  @Test
  public void testGroupByOnArraysUnknownDoubles()
  {
    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(ARRAY_UNKNOWN)
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Collections.singletonList(INTERVAL))
                                   .setDimensions(DefaultDimensionSpec.of("ad"))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);


    // 'unknown' is treated as ColumnType.STRING. this might not always be the case, so this is a test case of wacky
    // behavior of sorts
    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of(
            new Object[]{"1.0"},
            new Object[]{"2.0"},
            new Object[]{"3.0"},
            new Object[]{"4.0"},
            new Object[]{"6.0"},
            new Object[]{"8.0"}
          )
    );

    Assert.assertEquals(1, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(1, scheduler.getTotalAcquired().get());
    Assert.assertEquals(1, scheduler.getTotalReleased().get());
  }

  @Test
  public void testGroupByOnArraysLongs()
  {
    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(ARRAY)
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Collections.singletonList(INTERVAL))
                                   .setDimensions(new DefaultDimensionSpec(
                                       "al",
                                       "al",
                                       ColumnType.LONG_ARRAY
                                   ))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);


    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of(
            new Object[]{new ComparableList(ImmutableList.of(1L, 2L))},
            new Object[]{new ComparableList(ImmutableList.of(2L, 4L))},
            new Object[]{new ComparableList(ImmutableList.of(3L, 6L))},
            new Object[]{new ComparableList(ImmutableList.of(4L, 8L))}
        )
    );
  }

  @Test
  public void testGroupByOnArraysLongsAsString()
  {
    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(ARRAY)
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Collections.singletonList(INTERVAL))
                                   .setDimensions(DefaultDimensionSpec.of("al"))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);

    // when we donot define an outputType, convert {@link ComparableList} to a string
    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of(
            new Object[]{new ComparableList(ImmutableList.of(1L, 2L)).toString()},
            new Object[]{new ComparableList(ImmutableList.of(2L, 4L)).toString()},
            new Object[]{new ComparableList(ImmutableList.of(3L, 6L)).toString()},
            new Object[]{new ComparableList(ImmutableList.of(4L, 8L)).toString()}
        )
    );
  }

  @Test
  public void testGroupByOnArraysUnknownLongs()
  {
    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(ARRAY_UNKNOWN)
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Collections.singletonList(INTERVAL))
                                   .setDimensions(DefaultDimensionSpec.of("al"))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);


    // 'unknown' is treated as ColumnType.STRING. this might not always be the case, so this is a test case of wacky
    // behavior of sorts
    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of(
            new Object[]{"1"},
            new Object[]{"2"},
            new Object[]{"3"},
            new Object[]{"4"},
            new Object[]{"6"},
            new Object[]{"8"}
        )
    );

    Assert.assertEquals(1, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(1, scheduler.getTotalAcquired().get());
    Assert.assertEquals(1, scheduler.getTotalReleased().get());
  }

  @Test
  public void testGroupByOnArraysStrings()
  {
    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(ARRAY)
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Collections.singletonList(INTERVAL))
                                   .setDimensions(new DefaultDimensionSpec("as", "as", ColumnType.STRING_ARRAY))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of(
            new Object[]{ComparableStringArray.of("1.0", "2.0")},
            new Object[]{ComparableStringArray.of("2.0", "4.0")},
            new Object[]{ComparableStringArray.of("3.0", "6.0")},
            new Object[]{ComparableStringArray.of("4.0", "8.0")}
        )
    );
  }

  @Test
  public void testGroupByOnArraysStringsasString()
  {
    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(ARRAY)
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Collections.singletonList(INTERVAL))
                                   .setDimensions(DefaultDimensionSpec.of("as"))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);

    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of(
            new Object[]{ComparableStringArray.of("1.0", "2.0").toString()},
            new Object[]{ComparableStringArray.of("2.0", "4.0").toString()},
            new Object[]{ComparableStringArray.of("3.0", "6.0").toString()},
            new Object[]{ComparableStringArray.of("4.0", "8.0").toString()}
        )
    );
  }

  @Test
  public void testGroupByOnArraysUnknownStrings()
  {
    final GroupByQuery query =
        (GroupByQuery) GroupByQuery.builder()
                                   .setDataSource(ARRAY_UNKNOWN)
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Collections.singletonList(INTERVAL))
                                   .setDimensions(DefaultDimensionSpec.of("as"))
                                   .build()
                                   .withId(DUMMY_QUERY_ID);


    // 'unknown' is treated as ColumnType.STRING. this might not always be the case, so this is a test case of wacky
    // behavior of sorts
    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of(
            new Object[]{"1.0"},
            new Object[]{"2.0"},
            new Object[]{"3.0"},
            new Object[]{"4.0"},
            new Object[]{"6.0"},
            new Object[]{"8.0"}
        )
    );

    Assert.assertEquals(1, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(1, scheduler.getTotalAcquired().get());
    Assert.assertEquals(1, scheduler.getTotalReleased().get());
  }


  @Test
  public void testTopNArraysDoubles()
  {
    final TopNQuery query =
        (TopNQuery) new TopNQueryBuilder().dataSource(ARRAY)
                                          .granularity(Granularities.ALL)
                                          .intervals(Intervals.ONLY_ETERNITY)
                                          .dimension(DefaultDimensionSpec.of("ad"))
                                          .metric("sum_n")
                                          .threshold(1000)
                                          .aggregators(new LongSumAggregatorFactory("sum_n", "n"))
                                          .build()
                                          .withId(DUMMY_QUERY_ID);


    // group by cannot handle true array types, expect this, RuntimeExeception with IAE in stack trace
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Cannot create query type helper from invalid type [ARRAY<DOUBLE>]");

    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of()
    );
  }

  @Test
  public void testTopNOnArraysUnknownDoubles()
  {
    final TopNQuery query =
        (TopNQuery) new TopNQueryBuilder().dataSource(ARRAY_UNKNOWN)
                                          .granularity(Granularities.ALL)
                                          .intervals(Intervals.ONLY_ETERNITY)
                                          .dimension(DefaultDimensionSpec.of("ad"))
                                          .metric("sum_n")
                                          .threshold(1000)
                                          .aggregators(new LongSumAggregatorFactory("sum_n", "n"))
                                          .build()
                                          .withId(DUMMY_QUERY_ID);


    // 'unknown' is treated as ColumnType.STRING. this might not always be the case, so this is a test case of wacky
    // behavior of sorts
    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of(
            new Object[]{946684800000L, "4.0", 6L},
            new Object[]{946684800000L, "8.0", 4L},
            new Object[]{946684800000L, "2.0", 3L},
            new Object[]{946684800000L, "3.0", 3L},
            new Object[]{946684800000L, "6.0", 3L},
            new Object[]{946684800000L, "1.0", 1L}
        )
    );

    Assert.assertEquals(1, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(1, scheduler.getTotalAcquired().get());
    Assert.assertEquals(1, scheduler.getTotalReleased().get());
  }

  @Test
  public void testTopNOnArraysLongs()
  {
    final TopNQuery query =
        (TopNQuery) new TopNQueryBuilder().dataSource(ARRAY)
                                          .granularity(Granularities.ALL)
                                          .intervals(Intervals.ONLY_ETERNITY)
                                          .dimension(DefaultDimensionSpec.of("al"))
                                          .metric("sum_n")
                                          .threshold(1000)
                                          .aggregators(new LongSumAggregatorFactory("sum_n", "n"))
                                          .build()
                                          .withId(DUMMY_QUERY_ID);

    // group by cannot handle true array types, expect this, RuntimeExeception with IAE in stack trace
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Cannot create query type helper from invalid type [ARRAY<LONG>]");

    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of()
    );
  }

  @Test
  public void testTopNOnArraysUnknownLongs()
  {
    final TopNQuery query =
        (TopNQuery) new TopNQueryBuilder().dataSource(ARRAY_UNKNOWN)
                                          .granularity(Granularities.ALL)
                                          .intervals(Intervals.ONLY_ETERNITY)
                                          .dimension(DefaultDimensionSpec.of("al"))
                                          .metric("sum_n")
                                          .threshold(1000)
                                          .aggregators(new LongSumAggregatorFactory("sum_n", "n"))
                                          .build()
                                          .withId(DUMMY_QUERY_ID);


    // 'unknown' is treated as ColumnType.STRING. this might not always be the case, so this is a test case of wacky
    // behavior of sorts
    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of(
            new Object[]{946684800000L, "4", 6L},
            new Object[]{946684800000L, "8", 4L},
            new Object[]{946684800000L, "2", 3L},
            new Object[]{946684800000L, "3", 3L},
            new Object[]{946684800000L, "6", 3L},
            new Object[]{946684800000L, "1", 1L}
        )
    );

    Assert.assertEquals(1, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(1, scheduler.getTotalAcquired().get());
    Assert.assertEquals(1, scheduler.getTotalReleased().get());
  }

  @Test
  public void testTopNOnArraysStrings()
  {
    final TopNQuery query =
        (TopNQuery) new TopNQueryBuilder().dataSource(ARRAY)
                                          .granularity(Granularities.ALL)
                                          .intervals(Intervals.ONLY_ETERNITY)
                                          .dimension(DefaultDimensionSpec.of("as"))
                                          .metric("sum_n")
                                          .threshold(1000)
                                          .aggregators(new LongSumAggregatorFactory("sum_n", "n"))
                                          .build()
                                          .withId(DUMMY_QUERY_ID);


    // group by cannot handle true array types, expect this, RuntimeExeception with IAE in stack trace
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Cannot create query type helper from invalid type [ARRAY<STRING>]");

    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of()
    );
  }

  @Test
  public void testTopNOnArraysUnknownStrings()
  {
    final TopNQuery query =
        (TopNQuery) new TopNQueryBuilder().dataSource(ARRAY_UNKNOWN)
                                          .granularity(Granularities.ALL)
                                          .intervals(Intervals.ONLY_ETERNITY)
                                          .dimension(DefaultDimensionSpec.of("as"))
                                          .metric("sum_n")
                                          .threshold(1000)
                                          .aggregators(new LongSumAggregatorFactory("sum_n", "n"))
                                          .build()
                                          .withId(DUMMY_QUERY_ID);


    // 'unknown' is treated as ColumnType.STRING. this might not always be the case, so this is a test case of wacky
    // behavior of sorts
    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of(
            new Object[]{946684800000L, "4.0", 6L},
            new Object[]{946684800000L, "8.0", 4L},
            new Object[]{946684800000L, "2.0", 3L},
            new Object[]{946684800000L, "3.0", 3L},
            new Object[]{946684800000L, "6.0", 3L},
            new Object[]{946684800000L, "1.0", 1L}
        )
    );

    Assert.assertEquals(1, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(1, scheduler.getTotalAcquired().get());
    Assert.assertEquals(1, scheduler.getTotalReleased().get());
  }

  @Test
  public void testTimeseriesOnArrays()
  {
    final TimeseriesQuery query =
        (TimeseriesQuery) Druids.newTimeseriesQueryBuilder()
                                .dataSource(ARRAY)
                                .granularity(Granularities.ALL)
                                .intervals(Collections.singletonList(INTERVAL))
                                .aggregators(new LongSumAggregatorFactory("sum", "al"))
                                .context(ImmutableMap.of(TimeseriesQuery.CTX_GRAND_TOTAL, false))
                                .build()
                                .withId(DUMMY_QUERY_ID);

    // sum doesn't know what to do with an array, so gets 0
    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of(new Object[]{INTERVAL.getStartMillis(), NullHandling.sqlCompatible() ? null : 0L})
    );

    Assert.assertEquals(1, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(1, scheduler.getTotalAcquired().get());
    Assert.assertEquals(1, scheduler.getTotalReleased().get());
  }

  @Test
  public void testTimeseriesOnArraysUnknown()
  {
    final TimeseriesQuery query =
        (TimeseriesQuery) Druids.newTimeseriesQueryBuilder()
                                .dataSource(ARRAY_UNKNOWN)
                                .granularity(Granularities.ALL)
                                .intervals(Collections.singletonList(INTERVAL))
                                .aggregators(new LongSumAggregatorFactory("sum", "al"))
                                .context(ImmutableMap.of(TimeseriesQuery.CTX_GRAND_TOTAL, false))
                                .build()
                                .withId(DUMMY_QUERY_ID);

    // sum doesn't know what to do with an array also if type is null, so gets 0
    testQuery(
        query,
        ImmutableList.of(ExpectedQuery.cluster(query)),
        ImmutableList.of(new Object[]{INTERVAL.getStartMillis(), NullHandling.sqlCompatible() ? null : 0L})
    );

    Assert.assertEquals(1, scheduler.getTotalRun().get());
    Assert.assertEquals(1, scheduler.getTotalPrioritizedAndLaned().get());
    Assert.assertEquals(1, scheduler.getTotalAcquired().get());
    Assert.assertEquals(1, scheduler.getTotalReleased().get());
  }

  /**
   * Initialize (or reinitialize) our {@link #walker} and {@link #closer} with default scheduler.
   */
  private void initWalker(final Map<String, String> serverProperties)
  {
    initWalker(serverProperties, QueryStackTests.DEFAULT_NOOP_SCHEDULER);
  }

  /**
   * Initialize (or reinitialize) our {@link #walker} and {@link #closer}.
   */
  private void initWalker(final Map<String, String> serverProperties, QueryScheduler schedulerForTest)
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final ServerConfig serverConfig = jsonMapper.convertValue(serverProperties, ServerConfig.class);

    final SegmentWrangler segmentWrangler = new MapSegmentWrangler(
        ImmutableMap.<Class<? extends DataSource>, SegmentWrangler>builder()
                    .put(InlineDataSource.class, new InlineSegmentWrangler())
                    .put(FrameBasedInlineDataSource.class, new FrameBasedInlineSegmentWrangler())
                    .build()
    );

    final JoinableFactory globalFactory = new JoinableFactory()
    {
      @Override
      public boolean isDirectlyJoinable(DataSource dataSource)
      {
        return ((GlobalTableDataSource) dataSource).getName().equals(GLOBAL);
      }

      @Override
      public Optional<Joinable> build(DataSource dataSource, JoinConditionAnalysis condition)
      {
        return Optional.empty();
      }
    };

    final JoinableFactory joinableFactory = new MapJoinableFactory(
        ImmutableSet.of(globalFactory, new InlineJoinableFactory(), new FrameBasedInlineJoinableFactory()),
        ImmutableMap.<Class<? extends JoinableFactory>, Class<? extends DataSource>>builder()
                    .put(InlineJoinableFactory.class, InlineDataSource.class)
                    .put(FrameBasedInlineJoinableFactory.class, FrameBasedInlineDataSource.class)
                    .put(globalFactory.getClass(), GlobalTableDataSource.class)
                    .build()
    );
    final JoinableFactoryWrapper joinableFactoryWrapper = new JoinableFactoryWrapper(joinableFactory);

    class CapturingWalker implements QuerySegmentWalker
    {
      private QuerySegmentWalker baseWalker;
      private ClusterOrLocal how;

      CapturingWalker(QuerySegmentWalker baseWalker, ClusterOrLocal how)
      {
        this.baseWalker = baseWalker;
        this.how = how;
      }

      @Override
      public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
      {
        final QueryRunner<T> baseRunner = baseWalker.getQueryRunnerForIntervals(query, intervals);

        return (queryPlus, responseContext) -> {
          log.info("Query (%s): %s", how, queryPlus.getQuery());
          issuedQueries.add(new ExpectedQuery(queryPlus.getQuery(), how));
          return baseRunner.run(queryPlus, responseContext);
        };
      }

      @Override
      public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
      {
        final QueryRunner<T> baseRunner = baseWalker.getQueryRunnerForSegments(query, specs);

        return (queryPlus, responseContext) -> {
          log.info("Query (%s): %s", how, queryPlus.getQuery());
          issuedQueries.add(new ExpectedQuery(queryPlus.getQuery(), how));
          return baseRunner.run(queryPlus, responseContext);
        };
      }
    }

    Injector injector = QueryStackTests.injectorWithLookup();
    walker = QueryStackTests.createClientQuerySegmentWalker(
        injector,
        new CapturingWalker(
            QueryStackTests.createClusterQuerySegmentWalker(
                ImmutableMap.<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>>builder()
                    .put(FOO, makeTimeline(FOO, FOO_INLINE))
                    .put(BAR, makeTimeline(BAR, BAR_INLINE))
                    .put(MULTI, makeTimeline(MULTI, MULTI_VALUE_INLINE))
                    .put(GLOBAL, makeTimeline(GLOBAL, FOO_INLINE))
                    .put(ARRAY, makeTimeline(ARRAY, ARRAY_INLINE))
                    .put(ARRAY_UNKNOWN, makeTimeline(ARRAY_UNKNOWN, ARRAY_INLINE_UNKNOWN))
                    .build(),
                conglomerate,
                schedulerForTest,
                injector
            ),
            ClusterOrLocal.CLUSTER
        ),
        new CapturingWalker(
            QueryStackTests.createLocalQuerySegmentWalker(
                conglomerate,
                segmentWrangler,
                joinableFactoryWrapper,
                schedulerForTest
                ),
            ClusterOrLocal.LOCAL
        ),
        conglomerate,
        joinableFactory,
        serverConfig
    );
  }

  /**
   * Issue {@code query} through {@link #walker}. Verifies that a specific set of subqueries is issued and that a
   * specific set of results is returned. The results are expected to be in array form; see
   * {@link org.apache.druid.query.QueryToolChest#resultsAsArrays}.
   */
  private <T> void testQuery(
      final Query<T> query,
      final List<ExpectedQuery> expectedQueries,
      final List<Object[]> expectedResults
  )
  {
    issuedQueries.clear();

    final Sequence<T> resultSequence = QueryPlus.wrap(query).run(walker, ResponseContext.createEmpty());

    final List<Object[]> arrays =
        conglomerate.findFactory(query).getToolchest().resultsAsArrays(query, resultSequence).toList();

    for (Object[] array : arrays) {
      log.info("Result: %s", Arrays.toString(array));
    }

    QueryToolChestTestHelper.assertArrayResultsEquals(expectedResults, Sequences.simple(arrays));
    Assert.assertEquals(expectedQueries, issuedQueries);
  }

  private enum ClusterOrLocal
  {
    CLUSTER,
    LOCAL
  }

  private static class ExpectedQuery
  {
    private final Query<?> query;
    private final ClusterOrLocal how;

    ExpectedQuery(Query<?> query, ClusterOrLocal how)
    {
      Query<?> modifiedQuery;
      // Need to blast various parameters that will vary and aren't important to test for.
      modifiedQuery = query.withOverriddenContext(
          ImmutableMap.<String, Object>builder()
              .put(DirectDruidClient.QUERY_FAIL_TIME, 0L)
              .put(QueryContexts.DEFAULT_TIMEOUT_KEY, 0L)
              .put(QueryContexts.FINALIZE_KEY, true)
              .put(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, 0L)
              .put(GroupByQuery.CTX_KEY_SORT_BY_DIMS_FIRST, false)
              .put(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, true)
              .put(GroupByQueryConfig.CTX_KEY_APPLY_LIMIT_PUSH_DOWN, true)
              .put(GroupingEngine.CTX_KEY_OUTERMOST, true)
              .put(GroupingEngine.CTX_KEY_FUDGE_TIMESTAMP, "1979")
              .build()
      );

      if (modifiedQuery.getDataSource() instanceof FrameBasedInlineDataSource) {
        // Do round-trip serialization in order to replace FrameBasedInlineDataSource with InlineDataSource, so
        // comparisons work independently of whether we are using frames or regular inline datasets.
        try {
          modifiedQuery = modifiedQuery.withDataSource(
              TestHelper.JSON_MAPPER.readValue(
                  TestHelper.JSON_MAPPER.writeValueAsBytes(modifiedQuery.getDataSource()),
                  DataSource.class
              )
          );
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      this.query = modifiedQuery;
      this.how = how;
    }

    static ExpectedQuery local(final Query<?> query)
    {
      return new ExpectedQuery(query, ClusterOrLocal.LOCAL);
    }

    static ExpectedQuery cluster(final Query<?> query)
    {
      return new ExpectedQuery(query, ClusterOrLocal.CLUSTER);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ExpectedQuery that = (ExpectedQuery) o;
      return Objects.equals(query, that.query) &&
             how == that.how;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(query, how);
    }

    @Override
    public String toString()
    {
      return "ExpectedQuery{" +
             "query=" + query +
             ", how=" + how +
             '}';
    }
  }

  private static VersionedIntervalTimeline<String, ReferenceCountingSegment> makeTimeline(
      final String name,
      final InlineDataSource dataSource
  )
  {
    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline =
        new VersionedIntervalTimeline<>(Comparator.naturalOrder());

    timeline.add(
        INTERVAL,
        VERSION,
        SHARD_SPEC.createChunk(
            ReferenceCountingSegment.wrapSegment(
                new RowBasedSegment<>(
                    SegmentId.of(name, INTERVAL, VERSION, SHARD_SPEC.getPartitionNum()),
                    Sequences.simple(dataSource.getRows()),
                    dataSource.rowAdapter(),
                    dataSource.getRowSignature()
                ),
                SHARD_SPEC
            )
        )
    );

    return timeline;
  }
}


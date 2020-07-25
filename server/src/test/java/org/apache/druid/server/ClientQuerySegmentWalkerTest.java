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
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
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
import org.apache.druid.query.groupby.GroupByQueryHelper;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV2;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.InlineSegmentWrangler;
import org.apache.druid.segment.MapSegmentWrangler;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.InlineJoinableFactory;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.Joinable;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.MapJoinableFactory;
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
import java.util.UUID;

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
                  .add("s", ValueType.STRING)
                  .add("n", ValueType.LONG)
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
                  .add("s", ValueType.STRING)
                  .add("n", ValueType.LONG)
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
                  .add("s", ValueType.STRING)
                  .add("n", ValueType.LONG)
                  .build()
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private Closer closer;
  private QueryRunnerFactoryConglomerate conglomerate;

  // Queries that are issued; checked by "testQuery" against its "expectedQueries" parameter.
  private List<ExpectedQuery> issuedQueries = new ArrayList<>();

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
        new ServerConfig()
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
                                .withId(UUID.randomUUID().toString());

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
                                .withId("queryId");

    // expect global/joinable datasource to be automatically translated into a GlobalTableDataSource
    final TimeseriesQuery expectedClusterQuery =
        (TimeseriesQuery) Druids.newTimeseriesQueryBuilder()
                                .dataSource(new GlobalTableDataSource(GLOBAL))
                                .granularity(Granularities.ALL)
                                .intervals(Collections.singletonList(INTERVAL))
                                .aggregators(new LongSumAggregatorFactory("sum", "n"))
                                .context(ImmutableMap.of(TimeseriesQuery.CTX_GRAND_TOTAL, false))
                                .build()
                                .withId("queryId");

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
                                .withId(UUID.randomUUID().toString());

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
                                .withId(UUID.randomUUID().toString());

    testQuery(
        query,
        ImmutableList.of(
            ExpectedQuery.cluster(subquery),
            ExpectedQuery.local(
                query.withDataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(new Object[]{"x"}, new Object[]{"y"}, new Object[]{"z"}),
                        RowSignature.builder().add("s", ValueType.STRING).build()
                    )
                )
            )
        ),
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
                                   .withId("queryId");

    testQuery(
        query,
        // GroupBy handles its own subqueries; only the inner one will go to the cluster.
        ImmutableList.of(ExpectedQuery.cluster(subquery)),
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
                                   .withId(UUID.randomUUID().toString());

    testQuery(
        query,
        ImmutableList.of(
            ExpectedQuery.cluster(query.withDataSource(new TableDataSource(FOO))),
            ExpectedQuery.cluster(query.withDataSource(new TableDataSource(BAR)))
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
                                           ExprMacroTable.nil()
                                       )
                                   )
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Intervals.ONLY_ETERNITY)
                                   .setDimensions(DefaultDimensionSpec.of("s"), DefaultDimensionSpec.of("j.s"))
                                   .setAggregatorSpecs(new CountAggregatorFactory("cnt"))
                                   .build()
                                   .withId(UUID.randomUUID().toString());

    testQuery(
        query,
        ImmutableList.of(
            ExpectedQuery.cluster(subquery),
            ExpectedQuery.cluster(
                query.withDataSource(
                    query.getDataSource().withChildren(
                        ImmutableList.of(
                            query.getDataSource().getChildren().get(0),
                            InlineDataSource.fromIterable(
                                ImmutableList.of(new Object[]{"y"}),
                                RowSignature.builder().add("s", ValueType.STRING).build()
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
  public void testGroupByOnScanMultiValue()
  {
    ScanQuery subquery = new Druids.ScanQueryBuilder().dataSource(MULTI)
                                                      .columns("s", "n")
                                                      .intervals(
                                                          new MultipleIntervalSegmentSpec(
                                                              ImmutableList.of(Intervals.ETERNITY)
                                                          )
                                                      )
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
                                   .withId(UUID.randomUUID().toString());

    testQuery(
        query,
        // GroupBy handles its own subqueries; only the inner one will go to the cluster.
        ImmutableList.of(
            ExpectedQuery.cluster(subquery),
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
                                                      .intervals(
                                                          new MultipleIntervalSegmentSpec(
                                                              ImmutableList.of(Intervals.ETERNITY)
                                                          )
                                                      )
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
                                          .withId(UUID.randomUUID().toString());

    testQuery(
        query,
        // GroupBy handles its own subqueries; only the inner one will go to the cluster.
        ImmutableList.of(
            ExpectedQuery.cluster(subquery),
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
                                           ExprMacroTable.nil()
                                       )
                                   )
                                   .setGranularity(Granularities.ALL)
                                   .setInterval(Intervals.ONLY_ETERNITY)
                                   .setDimensions(DefaultDimensionSpec.of("s"), DefaultDimensionSpec.of("j.s"))
                                   .setAggregatorSpecs(new CountAggregatorFactory("cnt"))
                                   .build()
                                   .withId(UUID.randomUUID().toString());

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
                                .withId(UUID.randomUUID().toString());

    expectedException.expect(ResourceLimitExceededException.class);
    expectedException.expectMessage("Subquery generated results beyond maximum[2]");

    testQuery(query, ImmutableList.of(), ImmutableList.of());
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
            .build()
    );

    final JoinableFactory joinableFactory = new MapJoinableFactory(
        ImmutableMap.<Class<? extends DataSource>, JoinableFactory>builder()
            .put(InlineDataSource.class, new InlineJoinableFactory())
            .put(GlobalTableDataSource.class, new JoinableFactory()
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
            })
            .build()
    );

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

    walker = QueryStackTests.createClientQuerySegmentWalker(
        new CapturingWalker(
            QueryStackTests.createClusterQuerySegmentWalker(
                ImmutableMap.of(
                    FOO, makeTimeline(FOO, FOO_INLINE),
                    BAR, makeTimeline(BAR, BAR_INLINE),
                    MULTI, makeTimeline(MULTI, MULTI_VALUE_INLINE),
                    GLOBAL, makeTimeline(GLOBAL, FOO_INLINE)
                ),
                joinableFactory,
                conglomerate,
                schedulerForTest
            ),
            ClusterOrLocal.CLUSTER
        ),
        new CapturingWalker(
            QueryStackTests.createLocalQuerySegmentWalker(
                conglomerate,
                segmentWrangler,
                joinableFactory,
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
      // Need to blast various parameters that will vary and aren't important to test for.
      this.query = query.withOverriddenContext(
          ImmutableMap.<String, Object>builder()
              .put(BaseQuery.SUB_QUERY_ID, "dummy")
              .put(DirectDruidClient.QUERY_FAIL_TIME, 0L)
              .put(QueryContexts.DEFAULT_TIMEOUT_KEY, 0L)
              .put(QueryContexts.FINALIZE_KEY, true)
              .put(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, 0L)
              .put(GroupByQuery.CTX_KEY_SORT_BY_DIMS_FIRST, false)
              .put(GroupByQueryHelper.CTX_KEY_SORT_RESULTS, false)
              .put(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, true)
              .put(GroupByQueryConfig.CTX_KEY_STRATEGY, "X")
              .put(GroupByQueryConfig.CTX_KEY_APPLY_LIMIT_PUSH_DOWN, true)
              .put(GroupByStrategyV2.CTX_KEY_OUTERMOST, true)
              .put(GroupByStrategyV2.CTX_KEY_FUDGE_TIMESTAMP, "1979")
              .build()
      );

      this.how = how;
    }

    static ExpectedQuery local(final Query query)
    {
      return new ExpectedQuery(query, ClusterOrLocal.LOCAL);
    }

    static ExpectedQuery cluster(final Query query)
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
                    dataSource.getRows(),
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


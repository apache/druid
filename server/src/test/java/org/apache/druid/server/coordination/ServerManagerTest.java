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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.testing.fieldbinder.Bind;
import com.google.inject.testing.fieldbinder.BoundFieldModule;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.client.cache.LocalCacheProvider;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.java.util.common.guava.YieldingSequenceBase;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.ConcatQueryRunner;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.Druids;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.context.DefaultResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.planning.ExecutionVertex;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.query.policy.RestrictAllTablesPolicyEnforcer;
import org.apache.druid.query.search.SearchQuery;
import org.apache.druid.query.search.SearchResultValue;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestSegmentUtils;
import org.apache.druid.segment.TestSegmentUtils.SegmentForTesting;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.test.utils.TestSegmentCacheManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ServerManagerTest
{
  private static ImmutableSet<DataSegment> DATA_SEGMENTS = new ImmutableSet.Builder<DataSegment>()
      .add(TestSegmentUtils.makeSegment("test", "1", Intervals.of("P1d/2011-04-01")))
      .add(TestSegmentUtils.makeSegment("test", "1", Intervals.of("P1d/2011-04-02")))
      .add(TestSegmentUtils.makeSegment("test", "2", Intervals.of("P1d/2011-04-02")))
      .add(TestSegmentUtils.makeSegment("test", "1", Intervals.of("P1d/2011-04-03")))
      .add(TestSegmentUtils.makeSegment("test", "1", Intervals.of("P1d/2011-04-04")))
      .add(TestSegmentUtils.makeSegment("test", "1", Intervals.of("P1d/2011-04-05")))
      .add(TestSegmentUtils.makeSegment("test", "2", Intervals.of("PT1h/2011-04-04T01")))
      .add(TestSegmentUtils.makeSegment("test", "2", Intervals.of("PT1h/2011-04-04T02")))
      .add(TestSegmentUtils.makeSegment("test", "2", Intervals.of("PT1h/2011-04-04T03")))
      .add(TestSegmentUtils.makeSegment("test", "2", Intervals.of("PT1h/2011-04-04T05")))
      .add(TestSegmentUtils.makeSegment("test", "2", Intervals.of("PT1h/2011-04-04T06")))
      .add(TestSegmentUtils.makeSegment("test2", "1", Intervals.of("P1d/2011-04-01")))
      .add(TestSegmentUtils.makeSegment("test2", "1", Intervals.of("P1d/2011-04-02")))
      .build();

  @Bind
  private QueryRunnerFactoryConglomerate conglomerate;
  @Bind
  private SegmentManager segmentManager;
  @Bind
  private PolicyEnforcer policyEnforcer;
  @Bind
  private ServerConfig serverConfig;
  @Bind
  private ServiceEmitter serviceEmitter;
  @Bind
  private QueryProcessingPool queryProcessingPool;
  @Bind
  private CachePopulator cachePopulator;
  @Bind
  @Smile
  private ObjectMapper objectMapper;
  @Bind
  private Cache cache;
  @Bind
  private CacheConfig cacheConfig;

  private MyQueryRunnerFactory factory;
  private ExecutorService serverManagerExec;

  @Inject
  private ServerManager serverManager;

  @Before
  public void setUp()
  {
    serviceEmitter = new NoopServiceEmitter();
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    segmentManager = new SegmentManager(new TestSegmentCacheManager(DATA_SEGMENTS));
    for (DataSegment segment : DATA_SEGMENTS) {
      loadQueryable(segment.getDataSource(), segment.getVersion(), segment.getInterval());
    }

    factory = new MyQueryRunnerFactory(new CountDownLatch(1), new CountDownLatch(1), new CountDownLatch(1));
    // Only SearchQuery is supported in this test.
    conglomerate = DefaultQueryRunnerFactoryConglomerate.buildFromQueryRunnerFactories(ImmutableMap.of(
        SearchQuery.class,
        factory
    ));

    serverManagerExec = Execs.multiThreaded(2, "ServerManagerTest-%d");
    queryProcessingPool = new ForwardingQueryProcessingPool(serverManagerExec);
    cachePopulator = new ForegroundCachePopulator(new DefaultObjectMapper(), new CachePopulatorStats(), -1);
    objectMapper = new DefaultObjectMapper();
    cache = new LocalCacheProvider().get();
    cacheConfig = new CacheConfig();
    serverConfig = new ServerConfig();
    policyEnforcer = NoopPolicyEnforcer.instance();

    Guice.createInjector(BoundFieldModule.of(this)).injectMembers(this);
  }

  @Test
  public void testSimpleGet()
  {
    Future future = assertQueryable(
        Granularities.DAY,
        "test",
        Intervals.of("P1d/2011-04-01"),
        ImmutableList.of(new Pair<>("1", Intervals.of("P1d/2011-04-01")))
    );
    waitForTestVerificationAndCleanup(future);

    future = assertQueryable(
        Granularities.DAY,
        "test", Intervals.of("P2d/2011-04-02"),
        ImmutableList.of(
            new Pair<>("1", Intervals.of("P1d/2011-04-01")),
            new Pair<>("2", Intervals.of("P1d/2011-04-02"))
        )
    );
    waitForTestVerificationAndCleanup(future);
  }

  @Test
  public void testSimpleGetTombstone()
  {
    Future future = assertQueryable(
        Granularities.DAY,
        "testTombstone",
        Intervals.of("P1d/2011-04-01"),
        Collections.emptyList() // tombstone returns no data
    );
    waitForTestVerificationAndCleanup(future);
  }

  @Test
  public void testDelete1()
  {
    final String dataSouce = "test";
    final Interval interval = Intervals.of("2011-04-01/2011-04-02");

    Future future = assertQueryable(
        Granularities.DAY,
        dataSouce, interval,
        ImmutableList.of(
            new Pair<>("2", interval)
        )
    );
    waitForTestVerificationAndCleanup(future);

    dropQueryable(dataSouce, "2", interval);
    future = assertQueryable(
        Granularities.DAY,
        dataSouce, interval,
        ImmutableList.of(
            new Pair<>("1", interval)
        )
    );
    waitForTestVerificationAndCleanup(future);
  }

  @Test
  public void testDelete2()
  {
    loadQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));

    Future future = assertQueryable(
        Granularities.DAY,
        "test", Intervals.of("2011-04-04/2011-04-06"),
        ImmutableList.of(
            new Pair<>("3", Intervals.of("2011-04-04/2011-04-05"))
        )
    );
    waitForTestVerificationAndCleanup(future);

    dropQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));
    dropQueryable("test", "1", Intervals.of("2011-04-04/2011-04-05"));

    future = assertQueryable(
        Granularities.HOUR,
        "test", Intervals.of("2011-04-04/2011-04-04T06"),
        ImmutableList.of(
            new Pair<>("2", Intervals.of("2011-04-04T00/2011-04-04T01")),
            new Pair<>("2", Intervals.of("2011-04-04T01/2011-04-04T02")),
            new Pair<>("2", Intervals.of("2011-04-04T02/2011-04-04T03")),
            new Pair<>("2", Intervals.of("2011-04-04T04/2011-04-04T05")),
            new Pair<>("2", Intervals.of("2011-04-04T05/2011-04-04T06"))
        )
    );
    waitForTestVerificationAndCleanup(future);

    future = assertQueryable(
        Granularities.HOUR,
        "test", Intervals.of("2011-04-04/2011-04-04T03"),
        ImmutableList.of(
            new Pair<>("2", Intervals.of("2011-04-04T00/2011-04-04T01")),
            new Pair<>("2", Intervals.of("2011-04-04T01/2011-04-04T02")),
            new Pair<>("2", Intervals.of("2011-04-04T02/2011-04-04T03"))
        )
    );
    waitForTestVerificationAndCleanup(future);

    future = assertQueryable(
        Granularities.HOUR,
        "test", Intervals.of("2011-04-04T04/2011-04-04T06"),
        ImmutableList.of(
            new Pair<>("2", Intervals.of("2011-04-04T04/2011-04-04T05")),
            new Pair<>("2", Intervals.of("2011-04-04T05/2011-04-04T06"))
        )
    );
    waitForTestVerificationAndCleanup(future);
  }

  @Test
  public void testReferenceCounting() throws Exception
  {
    loadQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));

    Future future = assertQueryable(
        Granularities.DAY,
        "test", Intervals.of("2011-04-04/2011-04-06"),
        ImmutableList.of(
            new Pair<>("3", Intervals.of("2011-04-04/2011-04-05"))
        )
    );

    factory.notifyLatch.await(1000, TimeUnit.MILLISECONDS);

    Assert.assertEquals(1, factory.getSegmentReferences().size());

    for (ReferenceCountingSegment referenceCountingSegment : factory.getSegmentReferences()) {
      Assert.assertEquals(1, referenceCountingSegment.getNumReferences());
    }

    factory.waitYieldLatch.countDown();

    Assert.assertEquals(1, factory.getAdapters().size());

    for (TestSegmentUtils.SegmentForTesting segment : factory.getAdapters()) {
      Assert.assertFalse(segment.isClosed());
    }

    factory.waitLatch.countDown();
    future.get();

    dropQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));

    for (TestSegmentUtils.SegmentForTesting segment : factory.getAdapters()) {
      Assert.assertTrue(segment.isClosed());
    }
  }

  @Test
  public void testReferenceCounting_whileQueryExecuting() throws Exception
  {
    loadQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));

    Future future = assertQueryable(
        Granularities.DAY,
        "test", Intervals.of("2011-04-04/2011-04-06"),
        ImmutableList.of(
            new Pair<>("3", Intervals.of("2011-04-04/2011-04-05"))
        )
    );

    factory.notifyLatch.await(1000, TimeUnit.MILLISECONDS);

    Assert.assertEquals(1, factory.getSegmentReferences().size());

    for (ReferenceCountingSegment referenceCountingSegment : factory.getSegmentReferences()) {
      Assert.assertEquals(1, referenceCountingSegment.getNumReferences());
    }

    factory.waitYieldLatch.countDown();

    Assert.assertEquals(1, factory.getAdapters().size());

    for (TestSegmentUtils.SegmentForTesting segment : factory.getAdapters()) {
      Assert.assertFalse(segment.isClosed());
    }

    dropQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));

    for (TestSegmentUtils.SegmentForTesting segment : factory.getAdapters()) {
      Assert.assertFalse(segment.isClosed());
    }

    factory.waitLatch.countDown();
    future.get();

    for (TestSegmentUtils.SegmentForTesting segment : factory.getAdapters()) {
      Assert.assertTrue(segment.isClosed());
    }
  }

  @Test
  public void testReferenceCounting_multipleDrops() throws Exception
  {
    loadQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));

    Future future = assertQueryable(
        Granularities.DAY,
        "test", Intervals.of("2011-04-04/2011-04-06"),
        ImmutableList.of(
            new Pair<>("3", Intervals.of("2011-04-04/2011-04-05"))
        )
    );

    factory.notifyLatch.await(1000, TimeUnit.MILLISECONDS);

    Assert.assertEquals(1, factory.getSegmentReferences().size());

    for (ReferenceCountingSegment referenceCountingSegment : factory.getSegmentReferences()) {
      Assert.assertEquals(1, referenceCountingSegment.getNumReferences());
    }

    factory.waitYieldLatch.countDown();

    Assert.assertEquals(1, factory.getAdapters().size());

    for (TestSegmentUtils.SegmentForTesting segment : factory.getAdapters()) {
      Assert.assertFalse(segment.isClosed());
    }

    dropQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));
    dropQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));

    for (TestSegmentUtils.SegmentForTesting segment : factory.getAdapters()) {
      Assert.assertFalse(segment.isClosed());
    }

    factory.waitLatch.countDown();
    future.get();

    for (TestSegmentUtils.SegmentForTesting segment : factory.getAdapters()) {
      Assert.assertTrue(segment.isClosed());
    }
  }

  @Test
  public void testReferenceCounting_restrictedSegment() throws Exception
  {
    factory = new MyQueryRunnerFactory(new CountDownLatch(1), new CountDownLatch(1), new CountDownLatch(2));
    conglomerate = DefaultQueryRunnerFactoryConglomerate.buildFromQueryRunnerFactories(ImmutableMap.of(
        SearchQuery.class,
        factory
    ));
    serverManager = Guice.createInjector(BoundFieldModule.of(this)).getInstance(ServerManager.class);

    Interval interval = Intervals.of("P1d/2011-04-01");
    loadQueryable("test", "1", interval);
    SearchQuery query = searchQuery("test", interval, Granularities.ALL);
    SearchQuery queryOnRestricted = searchQuery(RestrictedDataSource.create(
        TableDataSource.create("test"),
        NoRestrictionPolicy.instance()
    ), interval, Granularities.ALL);

    Future<?> future = assertQuery(query, interval, ImmutableList.of(new Pair<>("1", interval)));
    // sleep for 1s to make sure the first query hits/finishes factory.createRunner first, since we can't test adapter
    // and segmentReference for RestrictedSegment unless there's already a ReferenceCountingSegment.
    Thread.sleep(1000L);
    Future<?> futureOnRestricted = assertQuery(
        queryOnRestricted,
        interval,
        ImmutableList.of(new Pair<>("1", interval))
    );

    Assert.assertTrue(factory.notifyLatch.await(1000, TimeUnit.MILLISECONDS));
    Assert.assertEquals(1, factory.getSegmentReferences().size());
    // Expect 2 references here: 1 for query and 1 for queryOnRestricted
    Assert.assertEquals(2, factory.getSegmentReferences().get(0).getNumReferences());

    factory.waitYieldLatch.countDown();
    factory.waitLatch.countDown();
    future.get();
    futureOnRestricted.get();
    Assert.assertEquals(1, factory.getSegmentReferences().size());
    // no references since both query are finished
    Assert.assertEquals(0, factory.getSegmentReferences().get(0).getNumReferences());
  }

  @Test
  public void testGetQueryRunnerForIntervals_whenTimelineIsMissingReturningNoopQueryRunner()
  {
    final Interval interval = Intervals.of("0000-01-01/P1D");
    final QueryRunner<Result<SearchResultValue>> queryRunner = serverManager.getQueryRunnerForIntervals(
        searchQuery("unknown_datasource", interval, Granularities.ALL),
        Collections.singletonList(interval)
    );
    Assert.assertSame(NoopQueryRunner.class, queryRunner.getClass());
  }

  @Test
  public void testGetQueryRunnerForSegments_whenTimelineIsMissingReportingMissingSegmentsOnQueryDataSource()
  {
    final Interval interval = Intervals.of("0000-01-01/P1D");
    final SearchQuery query = searchQueryWithQueryDataSource("unknown_datasource", interval, Granularities.ALL);
    final List<SegmentDescriptor> unknownSegments = Collections.singletonList(
        new SegmentDescriptor(interval, "unknown_version", 0)
    );
    DruidException e = assertThrows(
        DruidException.class,
        () -> serverManager.getQueryRunnerForSegments(query, unknownSegments)
    );
    Assert.assertTrue(e.getMessage().startsWith("Base dataSource"));
    Assert.assertTrue(e.getMessage().endsWith("is not a table!"));
  }

  @Test
  public void testGetQueryRunnerForSegments_whenTimelineIsMissingReportingMissingSegments()
  {
    final Interval interval = Intervals.of("0000-01-01/P1D");
    final SearchQuery query = searchQuery("unknown_datasource", interval, Granularities.ALL);
    final List<SegmentDescriptor> unknownSegments = Collections.singletonList(
        new SegmentDescriptor(interval, "unknown_version", 0)
    );
    final ResponseContext responseContext = DefaultResponseContext.createEmpty();

    final List<Result<SearchResultValue>> results = serverManager.getQueryRunnerForSegments(query, unknownSegments)
                                                                 .run(QueryPlus.wrap(query), responseContext)
                                                                 .toList();

    Assert.assertTrue(results.isEmpty());
    Assert.assertNotNull(responseContext.getMissingSegments());
    Assert.assertEquals(unknownSegments, responseContext.getMissingSegments());
  }

  @Test
  public void testGetQueryRunnerForSegments_whenTimelineEntryIsMissingReportingMissingSegments()
  {
    final Interval interval = Intervals.of("P1d/2011-04-01");
    final SearchQuery query = searchQuery("test", interval, Granularities.ALL);
    final List<SegmentDescriptor> unknownSegments = Collections.singletonList(
        new SegmentDescriptor(interval, "unknown_version", 0)
    );
    final ResponseContext responseContext = DefaultResponseContext.createEmpty();

    final List<Result<SearchResultValue>> results = serverManager.getQueryRunnerForSegments(query, unknownSegments)
                                                                 .run(QueryPlus.wrap(query), responseContext)
                                                                 .toList();
    Assert.assertTrue(results.isEmpty());
    Assert.assertNotNull(responseContext.getMissingSegments());
    Assert.assertEquals(unknownSegments, responseContext.getMissingSegments());
  }

  @Test
  public void testGetQueryRunnerForSegments_whenTimelinePartitionChunkIsMissingReportingMissingSegments()
  {
    final Interval interval = Intervals.of("P1d/2011-04-01");
    final int unknownPartitionId = 1000;
    final SearchQuery query = searchQuery("test", interval, Granularities.ALL);
    final List<SegmentDescriptor> unknownSegments = Collections.singletonList(
        new SegmentDescriptor(interval, "1", unknownPartitionId)
    );
    final ResponseContext responseContext = DefaultResponseContext.createEmpty();
    final List<Result<SearchResultValue>> results = serverManager.getQueryRunnerForSegments(query, unknownSegments)
                                                                 .run(QueryPlus.wrap(query), responseContext)
                                                                 .toList();
    Assert.assertTrue(results.isEmpty());
    Assert.assertNotNull(responseContext.getMissingSegments());
    Assert.assertEquals(unknownSegments, responseContext.getMissingSegments());
  }

  @Test
  public void testGetQueryRunnerForSegments_whenSegmentIsClosedReportingMissingSegments()
  {
    final Interval interval = Intervals.of("P1d/2011-04-01");
    final SearchQuery query = searchQuery("test", interval, Granularities.ALL);
    final Optional<VersionedIntervalTimeline<String, ReferenceCountingSegment>> maybeTimeline = segmentManager
        .getTimeline(ExecutionVertex.of(query).getBaseTableDataSource());
    Assume.assumeTrue(maybeTimeline.isPresent());
    // close all segments in interval
    final List<TimelineObjectHolder<String, ReferenceCountingSegment>> holders = maybeTimeline.get().lookup(interval);
    final List<SegmentDescriptor> closedSegments = new ArrayList<>();
    for (TimelineObjectHolder<String, ReferenceCountingSegment> holder : holders) {
      for (PartitionChunk<ReferenceCountingSegment> chunk : holder.getObject()) {
        final ReferenceCountingSegment segment = chunk.getObject();
        Assert.assertNotNull(segment.getId());
        closedSegments.add(
            new SegmentDescriptor(segment.getDataInterval(), segment.getVersion(), segment.getId().getPartitionNum())
        );
        segment.close();
      }
    }
    final ResponseContext responseContext = DefaultResponseContext.createEmpty();

    final List<Result<SearchResultValue>> results = serverManager.getQueryRunnerForSegments(query, closedSegments)
                                                                 .run(QueryPlus.wrap(query), responseContext)
                                                                 .toList();
    Assert.assertTrue(results.isEmpty());
    Assert.assertNotNull(responseContext.getMissingSegments());
    Assert.assertEquals(closedSegments, responseContext.getMissingSegments());
  }

  @Test
  public void testGetQueryRunnerForSegments_forUnknownQueryThrowingException()
  {
    final Interval interval = Intervals.of("P1d/2011-04-01");
    final List<SegmentDescriptor> descriptors = Collections.singletonList(new SegmentDescriptor(interval, "1", 0));
    Query<?> query = Druids.newTimeBoundaryQueryBuilder()
                           .dataSource("random-ds")
                           .intervals(interval.toString())
                           .build();
    // We only have QueryRunnerFactory for SearchQuery in test.
    QueryUnsupportedException e = Assert.assertThrows(
        QueryUnsupportedException.class,
        () -> serverManager.getQueryRunnerForSegments(query, descriptors)
    );
    Assert.assertTrue(e.getMessage().startsWith("Unknown query type"));
  }

  @Test
  public void testGetQueryRunnerForSegments_restricted() throws Exception
  {
    conglomerate = DefaultQueryRunnerFactoryConglomerate.buildFromQueryRunnerFactories(ImmutableMap.of(
        SearchQuery.class,
        factory
    ));
    serverManager = Guice.createInjector(BoundFieldModule.of(this)).getInstance(ServerManager.class);
    Interval interval = Intervals.of("P1d/2011-04-01");
    SearchQuery query = searchQuery("test", interval, Granularities.ALL);
    SearchQuery queryOnRestricted = searchQuery(RestrictedDataSource.create(
        TableDataSource.create("test"),
        NoRestrictionPolicy.instance()
    ), interval, Granularities.ALL);

    serverManager.getQueryRunnerForIntervals(query, ImmutableList.of(interval)).run(QueryPlus.wrap(query)).toList();
    // switch to a policy enforcer that restricts all tables
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(ImmutableList.of(NoRestrictionPolicy.class.getName()));
    serverManager = Guice.createInjector(BoundFieldModule.of(this)).getInstance(ServerManager.class);
    // fail on query
    DruidException e = Assert.assertThrows(
        DruidException.class,
        () -> serverManager.getQueryRunnerForIntervals(query, ImmutableList.of(interval))
                           .run(QueryPlus.wrap(query))
                           .toList()
    );
    Assert.assertEquals(DruidException.Category.FORBIDDEN, e.getCategory());
    Assert.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
    Assert.assertEquals(
        "Failed security validation with segment [test_2011-03-31T00:00:00.000Z_2011-04-01T00:00:00.000Z_1]",
        e.getMessage()
    );
    // succeed on queryOnRestricted
    serverManager.getQueryRunnerForIntervals(queryOnRestricted, ImmutableList.of(interval))
                 .run(QueryPlus.wrap(queryOnRestricted))
                 .toList();
  }

  private void waitForTestVerificationAndCleanup(Future future)
  {
    try {
      factory.notifyLatch.await(1000, TimeUnit.MILLISECONDS);
      factory.waitLatch.countDown();
      future.get();
      factory.clearAdapters();
    }
    catch (Exception e) {
      throw new RuntimeException(e.getCause());
    }
  }

  private static SearchQuery searchQueryWithQueryDataSource(
      String datasource,
      Interval interval,
      Granularity granularity
  )
  {
    final ImmutableList<SegmentDescriptor> descriptors = ImmutableList.of(
        new SegmentDescriptor(Intervals.of("2000/3000"), "0", 0),
        new SegmentDescriptor(Intervals.of("2000/3000"), "0", 1)
    );
    return searchQuery(new QueryDataSource(Druids.newTimeseriesQueryBuilder()
                                                 .dataSource(datasource)
                                                 .intervals(new MultipleSpecificSegmentSpec(descriptors))
                                                 .granularity(Granularities.ALL)
                                                 .build()), interval, granularity);
  }

  private static SearchQuery searchQuery(String datasource, Interval interval, Granularity granularity)
  {
    return searchQuery(TableDataSource.create(datasource), interval, granularity);
  }

  private static SearchQuery searchQuery(DataSource datasource, Interval interval, Granularity granularity)
  {
    return Druids.newSearchQueryBuilder()
                 .dataSource(datasource)
                 .intervals(Collections.singletonList(interval))
                 .granularity(granularity)
                 .limit(10000)
                 .query("wow")
                 .build();
  }

  private Future<?> assertQueryable(
      Granularity granularity,
      String dataSource,
      Interval interval,
      List<Pair<String, Interval>> expected
  )
  {
    final SearchQuery query = searchQuery(dataSource, interval, granularity);
    return assertQuery(query, interval, expected);
  }

  private Future<?> assertQuery(
      SearchQuery query,
      Interval interval,
      List<Pair<String, Interval>> expected
  )
  {
    final Iterator<Pair<String, Interval>> expectedIter = expected.iterator();
    final List<Interval> intervals = Collections.singletonList(interval);
    final QueryRunner<Result<SearchResultValue>> runner = serverManager.getQueryRunnerForIntervals(query, intervals);
    return serverManagerExec.submit(
        () -> {
          Sequence<Result<SearchResultValue>> seq = runner.run(QueryPlus.wrap(query));
          seq.toList();
          Iterator<SegmentForTesting> adaptersIter = factory.getAdapters().iterator();

          while (expectedIter.hasNext() && adaptersIter.hasNext()) {
            Pair<String, Interval> expectedVals = expectedIter.next();
            SegmentForTesting value = adaptersIter.next();

            Assert.assertEquals(expectedVals.lhs, value.getVersion());
            Assert.assertEquals(expectedVals.rhs, value.getInterval());
          }

          Assert.assertFalse(expectedIter.hasNext());
          Assert.assertFalse(adaptersIter.hasNext());
        }
    );
  }

  private void loadQueryable(String dataSource, String version, Interval interval)
  {
    try {
      DataSegment segment = "testTombstone".equals(dataSource) ?
                            TestSegmentUtils.makeTombstoneSegment(dataSource, version, interval) :
                            TestSegmentUtils.makeSegment(dataSource, version, interval);
      segmentManager.loadSegment(segment);
    }
    catch (SegmentLoadingException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void dropQueryable(String dataSource, String version, Interval interval)
  {
    segmentManager.dropSegment(TestSegmentUtils.makeSegment(dataSource, version, interval));
  }

  private static class MyQueryRunnerFactory implements QueryRunnerFactory<Result<SearchResultValue>, SearchQuery>
  {
    private final CountDownLatch waitLatch;
    private final CountDownLatch waitYieldLatch;
    private final CountDownLatch notifyLatch;
    private final List<TestSegmentUtils.SegmentForTesting> adapters = new ArrayList<>();
    private final List<ReferenceCountingSegment> segmentReferences = new ArrayList<>();


    public MyQueryRunnerFactory(
        CountDownLatch waitLatch,
        CountDownLatch waitYieldLatch,
        CountDownLatch notifyLatch
    )
    {
      this.waitLatch = waitLatch;
      this.waitYieldLatch = waitYieldLatch;
      this.notifyLatch = notifyLatch;
    }

    @Override
    public QueryRunner<Result<SearchResultValue>> createRunner(Segment adapter)
    {
      final ReferenceCountingSegment segment;
      if (this.adapters.stream()
                       .map(SegmentForTesting::getId)
                       .anyMatch(segmentId -> adapter.getId().equals(segmentId))) {
        // Already have adapter for this segment, skip.
        // For RestrictedSegment, we don't have access to RestrictedSegment.delegate, but it'd be recorded in segmentReferences.
        // This means we can't test adapter and segmentReference unless there's already a ReferenceCountingSegment.
      } else if (adapter instanceof ReferenceCountingSegment) {
        segment = (ReferenceCountingSegment) adapter;
        Assert.assertTrue(segment.getNumReferences() > 0);
        segmentReferences.add(segment);
        adapters.add((SegmentForTesting) segment.getBaseSegment());
      } else {
        throw new IAE("Unsupported segment instance: [%s]", adapter.getClass());
      }

      return new BlockingQueryRunner<>(new NoopQueryRunner<>(), waitLatch, waitYieldLatch, notifyLatch);
    }

    @Override
    public QueryRunner<Result<SearchResultValue>> mergeRunners(
        QueryProcessingPool queryProcessingPool,
        Iterable<QueryRunner<Result<SearchResultValue>>> queryRunners
    )
    {
      return new ConcatQueryRunner<>(Sequences.simple(queryRunners));
    }

    @Override
    public QueryToolChest<Result<SearchResultValue>, SearchQuery> getToolchest()
    {
      return new NoopQueryToolChest<>();
    }

    public List<SegmentForTesting> getAdapters()
    {
      return adapters;
    }

    public List<ReferenceCountingSegment> getSegmentReferences()
    {
      return segmentReferences;
    }

    public void clearAdapters()
    {
      adapters.clear();
    }
  }

  public static class NoopQueryToolChest<T, QueryType extends Query<T>> extends QueryToolChest<T, QueryType>
  {
    @Override
    public QueryRunner<T> mergeResults(QueryRunner<T> runner)
    {
      return runner;
    }

    @Override
    public QueryMetrics<Query<?>> makeMetrics(QueryType query)
    {
      return new DefaultQueryMetrics<>();
    }

    @Override
    public Function<T, T> makePreComputeManipulatorFn(QueryType query, MetricManipulationFn fn)
    {
      return Functions.identity();
    }

    @Override
    public TypeReference<T> getResultTypeReference()
    {
      return new TypeReference<>()
      {
      };
    }
  }

  private static class BlockingQueryRunner<T> implements QueryRunner<T>
  {
    private final QueryRunner<T> runner;
    private final CountDownLatch waitLatch;
    private final CountDownLatch waitYieldLatch;
    private final CountDownLatch notifyLatch;

    public BlockingQueryRunner(
        QueryRunner<T> runner,
        CountDownLatch waitLatch,
        CountDownLatch waitYieldLatch,
        CountDownLatch notifyLatch
    )
    {
      this.runner = runner;
      this.waitLatch = waitLatch;
      this.waitYieldLatch = waitYieldLatch;
      this.notifyLatch = notifyLatch;
    }

    @Override
    public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
    {
      return new BlockingSequence<>(runner.run(queryPlus, responseContext), waitLatch, waitYieldLatch, notifyLatch);
    }
  }

  /**
   * A Sequence that count-down {@code notifyLatch} when {@link #toYielder} is called, and the returned Yielder waits
   * for {@code waitYieldLatch} and {@code waitLatch} count-down.
   */
  private static class BlockingSequence<T> extends YieldingSequenceBase<T>
  {
    private final Sequence<T> baseSequence;
    private final CountDownLatch waitLatch;
    private final CountDownLatch waitYieldLatch;
    private final CountDownLatch notifyLatch;


    private BlockingSequence(
        Sequence<T> baseSequence,
        CountDownLatch waitLatch,
        CountDownLatch waitYieldLatch,
        CountDownLatch notifyLatch
    )
    {
      this.baseSequence = baseSequence;
      this.waitLatch = waitLatch;
      this.waitYieldLatch = waitYieldLatch;
      this.notifyLatch = notifyLatch;
    }

    @Override
    public <OutType> Yielder<OutType> toYielder(
        final OutType initValue,
        final YieldingAccumulator<OutType, T> accumulator
    )
    {
      notifyLatch.countDown();

      try {
        waitYieldLatch.await(1000, TimeUnit.MILLISECONDS);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }

      final Yielder<OutType> baseYielder = baseSequence.toYielder(initValue, accumulator);
      return new Yielder<>()
      {
        @Override
        public OutType get()
        {
          try {
            waitLatch.await(1000, TimeUnit.MILLISECONDS);
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
          return baseYielder.get();
        }

        @Override
        public Yielder<OutType> next(OutType initValue)
        {
          return baseYielder.next(initValue);
        }

        @Override
        public boolean isDone()
        {
          return baseYielder.isDone();
        }

        @Override
        public void close() throws IOException
        {
          baseYielder.close();
        }
      };
    }
  }
}

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
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.client.cache.LocalCacheProvider;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.java.util.common.guava.YieldingSequenceBase;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.ConcatQueryRunner;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.Druids;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.context.DefaultResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.context.ResponseContext.Key;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.search.SearchQuery;
import org.apache.druid.query.search.SearchResultValue;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 */
public class ServerManagerTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private ServerManager serverManager;
  private MyQueryRunnerFactory factory;
  private CountDownLatch queryWaitLatch;
  private CountDownLatch queryWaitYieldLatch;
  private CountDownLatch queryNotifyLatch;
  private ExecutorService serverManagerExec;
  private SegmentManager segmentManager;

  @Before
  public void setUp()
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());

    queryWaitLatch = new CountDownLatch(1);
    queryWaitYieldLatch = new CountDownLatch(1);
    queryNotifyLatch = new CountDownLatch(1);
    factory = new MyQueryRunnerFactory(queryWaitLatch, queryWaitYieldLatch, queryNotifyLatch);
    serverManagerExec = Executors.newFixedThreadPool(2);
    segmentManager = new SegmentManager(
        new SegmentLoader()
        {
          @Override
          public boolean isSegmentLoaded(DataSegment segment)
          {
            return false;
          }

          @Override
          public Segment getSegment(final DataSegment segment, boolean lazy)
          {
            return new SegmentForTesting(
                MapUtils.getString(segment.getLoadSpec(), "version"),
                (Interval) segment.getLoadSpec().get("interval")
            );
          }

          @Override
          public File getSegmentFiles(DataSegment segment)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public void cleanup(DataSegment segment)
          {

          }
        }
    );
    serverManager = new ServerManager(
        new QueryRunnerFactoryConglomerate()
        {
          @Override
          public <T, QueryType extends Query<T>> QueryRunnerFactory<T, QueryType> findFactory(QueryType query)
          {
            if (query instanceof SearchQuery) {
              return (QueryRunnerFactory) factory;
            } else {
              return null;
            }
          }
        },
        new NoopServiceEmitter(),
        serverManagerExec,
        new ForegroundCachePopulator(new DefaultObjectMapper(), new CachePopulatorStats(), -1),
        new DefaultObjectMapper(),
        new LocalCacheProvider().get(),
        new CacheConfig(),
        segmentManager,
        NoopJoinableFactory.INSTANCE,
        new ServerConfig()
    );

    loadQueryable("test", "1", Intervals.of("P1d/2011-04-01"));
    loadQueryable("test", "1", Intervals.of("P1d/2011-04-02"));
    loadQueryable("test", "2", Intervals.of("P1d/2011-04-02"));
    loadQueryable("test", "1", Intervals.of("P1d/2011-04-03"));
    loadQueryable("test", "1", Intervals.of("P1d/2011-04-04"));
    loadQueryable("test", "1", Intervals.of("P1d/2011-04-05"));
    loadQueryable("test", "2", Intervals.of("PT1h/2011-04-04T01"));
    loadQueryable("test", "2", Intervals.of("PT1h/2011-04-04T02"));
    loadQueryable("test", "2", Intervals.of("PT1h/2011-04-04T03"));
    loadQueryable("test", "2", Intervals.of("PT1h/2011-04-04T05"));
    loadQueryable("test", "2", Intervals.of("PT1h/2011-04-04T06"));
    loadQueryable("test2", "1", Intervals.of("P1d/2011-04-01"));
    loadQueryable("test2", "1", Intervals.of("P1d/2011-04-02"));
  }

  @Test
  public void testSimpleGet()
  {
    Future future = assertQueryable(
        Granularities.DAY,
        "test",
        Intervals.of("P1d/2011-04-01"),
        ImmutableList.of(
            new Pair<String, Interval>("1", Intervals.of("P1d/2011-04-01"))
        )
    );
    waitForTestVerificationAndCleanup(future);


    future = assertQueryable(
        Granularities.DAY,
        "test", Intervals.of("P2d/2011-04-02"),
        ImmutableList.of(
            new Pair<String, Interval>("1", Intervals.of("P1d/2011-04-01")),
            new Pair<String, Interval>("2", Intervals.of("P1d/2011-04-02"))
        )
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
            new Pair<String, Interval>("2", interval)
        )
    );
    waitForTestVerificationAndCleanup(future);

    dropQueryable(dataSouce, "2", interval);
    future = assertQueryable(
        Granularities.DAY,
        dataSouce, interval,
        ImmutableList.of(
            new Pair<String, Interval>("1", interval)
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
            new Pair<String, Interval>("3", Intervals.of("2011-04-04/2011-04-05"))
        )
    );
    waitForTestVerificationAndCleanup(future);

    dropQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));
    dropQueryable("test", "1", Intervals.of("2011-04-04/2011-04-05"));

    future = assertQueryable(
        Granularities.HOUR,
        "test", Intervals.of("2011-04-04/2011-04-04T06"),
        ImmutableList.of(
            new Pair<String, Interval>("2", Intervals.of("2011-04-04T00/2011-04-04T01")),
            new Pair<String, Interval>("2", Intervals.of("2011-04-04T01/2011-04-04T02")),
            new Pair<String, Interval>("2", Intervals.of("2011-04-04T02/2011-04-04T03")),
            new Pair<String, Interval>("2", Intervals.of("2011-04-04T04/2011-04-04T05")),
            new Pair<String, Interval>("2", Intervals.of("2011-04-04T05/2011-04-04T06"))
        )
    );
    waitForTestVerificationAndCleanup(future);

    future = assertQueryable(
        Granularities.HOUR,
        "test", Intervals.of("2011-04-04/2011-04-04T03"),
        ImmutableList.of(
            new Pair<String, Interval>("2", Intervals.of("2011-04-04T00/2011-04-04T01")),
            new Pair<String, Interval>("2", Intervals.of("2011-04-04T01/2011-04-04T02")),
            new Pair<String, Interval>("2", Intervals.of("2011-04-04T02/2011-04-04T03"))
        )
    );
    waitForTestVerificationAndCleanup(future);

    future = assertQueryable(
        Granularities.HOUR,
        "test", Intervals.of("2011-04-04T04/2011-04-04T06"),
        ImmutableList.of(
            new Pair<String, Interval>("2", Intervals.of("2011-04-04T04/2011-04-04T05")),
            new Pair<String, Interval>("2", Intervals.of("2011-04-04T05/2011-04-04T06"))
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
            new Pair<String, Interval>("3", Intervals.of("2011-04-04/2011-04-05"))
        )
    );

    queryNotifyLatch.await(1000, TimeUnit.MILLISECONDS);

    Assert.assertEquals(1, factory.getSegmentReferences().size());

    for (ReferenceCountingSegment referenceCountingSegment : factory.getSegmentReferences()) {
      Assert.assertEquals(1, referenceCountingSegment.getNumReferences());
    }

    queryWaitYieldLatch.countDown();

    Assert.assertTrue(factory.getAdapters().size() == 1);

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertFalse(segmentForTesting.isClosed());
    }

    queryWaitLatch.countDown();
    future.get();

    dropQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertTrue(segmentForTesting.isClosed());
    }
  }

  @Test
  public void testReferenceCountingWhileQueryExecuting() throws Exception
  {
    loadQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));

    Future future = assertQueryable(
        Granularities.DAY,
        "test", Intervals.of("2011-04-04/2011-04-06"),
        ImmutableList.of(
            new Pair<String, Interval>("3", Intervals.of("2011-04-04/2011-04-05"))
        )
    );

    queryNotifyLatch.await(1000, TimeUnit.MILLISECONDS);

    Assert.assertEquals(1, factory.getSegmentReferences().size());

    for (ReferenceCountingSegment referenceCountingSegment : factory.getSegmentReferences()) {
      Assert.assertEquals(1, referenceCountingSegment.getNumReferences());
    }

    queryWaitYieldLatch.countDown();

    Assert.assertEquals(1, factory.getAdapters().size());

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertFalse(segmentForTesting.isClosed());
    }

    dropQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertFalse(segmentForTesting.isClosed());
    }

    queryWaitLatch.countDown();
    future.get();

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertTrue(segmentForTesting.isClosed());
    }
  }

  @Test
  public void testMultipleDrops() throws Exception
  {
    loadQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));

    Future future = assertQueryable(
        Granularities.DAY,
        "test", Intervals.of("2011-04-04/2011-04-06"),
        ImmutableList.of(
            new Pair<String, Interval>("3", Intervals.of("2011-04-04/2011-04-05"))
        )
    );

    queryNotifyLatch.await(1000, TimeUnit.MILLISECONDS);

    Assert.assertEquals(1, factory.getSegmentReferences().size());

    for (ReferenceCountingSegment referenceCountingSegment : factory.getSegmentReferences()) {
      Assert.assertEquals(1, referenceCountingSegment.getNumReferences());
    }

    queryWaitYieldLatch.countDown();

    Assert.assertEquals(1, factory.getAdapters().size());

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertFalse(segmentForTesting.isClosed());
    }

    dropQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));
    dropQueryable("test", "3", Intervals.of("2011-04-04/2011-04-05"));

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertFalse(segmentForTesting.isClosed());
    }

    queryWaitLatch.countDown();
    future.get();

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertTrue(segmentForTesting.isClosed());
    }
  }

  @Test
  public void testGetQueryRunnerForIntervalsWhenTimelineIsMissingReturningNoopQueryRunner()
  {
    final Interval interval = Intervals.of("0000-01-01/P1D");
    final QueryRunner<Result<SearchResultValue>> queryRunner = serverManager.getQueryRunnerForIntervals(
        searchQuery("unknown_datasource", interval, Granularities.ALL),
        Collections.singletonList(interval)
    );
    Assert.assertSame(NoopQueryRunner.class, queryRunner.getClass());
  }

  @Test
  public void testGetQueryRunnerForSegmentsWhenTimelineIsMissingReportingMissingSegments()
  {
    final Interval interval = Intervals.of("0000-01-01/P1D");
    final SearchQuery query = searchQuery("unknown_datasource", interval, Granularities.ALL);
    final List<SegmentDescriptor> unknownSegments = Collections.singletonList(
        new SegmentDescriptor(interval, "unknown_version", 0)
    );
    final QueryRunner<Result<SearchResultValue>> queryRunner = serverManager.getQueryRunnerForSegments(
        query,
        unknownSegments
    );
    final ResponseContext responseContext = DefaultResponseContext.createEmpty();
    final List<Result<SearchResultValue>> results = queryRunner.run(QueryPlus.wrap(query), responseContext).toList();
    Assert.assertTrue(results.isEmpty());
    Assert.assertNotNull(responseContext.get(Key.MISSING_SEGMENTS));
    Assert.assertEquals(unknownSegments, responseContext.get(Key.MISSING_SEGMENTS));
  }

  @Test
  public void testGetQueryRunnerForSegmentsWhenTimelineEntryIsMissingReportingMissingSegments()
  {
    final Interval interval = Intervals.of("P1d/2011-04-01");
    final SearchQuery query = searchQuery("test", interval, Granularities.ALL);
    final List<SegmentDescriptor> unknownSegments = Collections.singletonList(
        new SegmentDescriptor(interval, "unknown_version", 0)
    );
    final QueryRunner<Result<SearchResultValue>> queryRunner = serverManager.getQueryRunnerForSegments(
        query,
        unknownSegments
    );
    final ResponseContext responseContext = DefaultResponseContext.createEmpty();
    final List<Result<SearchResultValue>> results = queryRunner.run(QueryPlus.wrap(query), responseContext).toList();
    Assert.assertTrue(results.isEmpty());
    Assert.assertNotNull(responseContext.get(Key.MISSING_SEGMENTS));
    Assert.assertEquals(unknownSegments, responseContext.get(Key.MISSING_SEGMENTS));
  }

  @Test
  public void testGetQueryRunnerForSegmentsWhenTimelinePartitionChunkIsMissingReportingMissingSegments()
  {
    final Interval interval = Intervals.of("P1d/2011-04-01");
    final int unknownPartitionId = 1000;
    final SearchQuery query = searchQuery("test", interval, Granularities.ALL);
    final List<SegmentDescriptor> unknownSegments = Collections.singletonList(
        new SegmentDescriptor(interval, "1", unknownPartitionId)
    );
    final QueryRunner<Result<SearchResultValue>> queryRunner = serverManager.getQueryRunnerForSegments(
        query,
        unknownSegments
    );
    final ResponseContext responseContext = DefaultResponseContext.createEmpty();
    final List<Result<SearchResultValue>> results = queryRunner.run(QueryPlus.wrap(query), responseContext).toList();
    Assert.assertTrue(results.isEmpty());
    Assert.assertNotNull(responseContext.get(Key.MISSING_SEGMENTS));
    Assert.assertEquals(unknownSegments, responseContext.get(Key.MISSING_SEGMENTS));
  }

  @Test
  public void testGetQueryRunnerForSegmentsWhenSegmentIsClosedReportingMissingSegments()
  {
    final Interval interval = Intervals.of("P1d/2011-04-01");
    final SearchQuery query = searchQuery("test", interval, Granularities.ALL);
    final Optional<VersionedIntervalTimeline<String, ReferenceCountingSegment>> maybeTimeline = segmentManager
        .getTimeline(DataSourceAnalysis.forDataSource(query.getDataSource()));
    Assert.assertTrue(maybeTimeline.isPresent());
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
    final QueryRunner<Result<SearchResultValue>> queryRunner = serverManager.getQueryRunnerForSegments(
        query,
        closedSegments
    );
    final ResponseContext responseContext = DefaultResponseContext.createEmpty();
    final List<Result<SearchResultValue>> results = queryRunner.run(QueryPlus.wrap(query), responseContext).toList();
    Assert.assertTrue(results.isEmpty());
    Assert.assertNotNull(responseContext.get(Key.MISSING_SEGMENTS));
    Assert.assertEquals(closedSegments, responseContext.get(Key.MISSING_SEGMENTS));
  }

  @Test
  public void testGetQueryRunnerForSegmentsForUnknownQueryThrowingException()
  {
    final Interval interval = Intervals.of("P1d/2011-04-01");
    final List<SegmentDescriptor> descriptors = Collections.singletonList(new SegmentDescriptor(interval, "1", 0));
    expectedException.expect(QueryUnsupportedException.class);
    expectedException.expectMessage("Unknown query type");
    serverManager.getQueryRunnerForSegments(
        new BaseQuery<Object>(
            new TableDataSource("test"),
            new MultipleSpecificSegmentSpec(descriptors),
            false,
            new HashMap<>()
        )
        {
          @Override
          public boolean hasFilters()
          {
            return false;
          }

          @Override
          public DimFilter getFilter()
          {
            return null;
          }

          @Override
          public String getType()
          {
            return null;
          }

          @Override
          public Query<Object> withOverriddenContext(Map<String, Object> contextOverride)
          {
            return null;
          }

          @Override
          public Query<Object> withQuerySegmentSpec(QuerySegmentSpec spec)
          {
            return null;
          }

          @Override
          public Query<Object> withDataSource(DataSource dataSource)
          {
            return null;
          }
        },
        descriptors
    );
  }

  private void waitForTestVerificationAndCleanup(Future future)
  {
    try {
      queryNotifyLatch.await(1000, TimeUnit.MILLISECONDS);
      queryWaitYieldLatch.countDown();
      queryWaitLatch.countDown();
      future.get();
      factory.clearAdapters();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private SearchQuery searchQuery(String datasource, Interval interval, Granularity granularity)
  {
    return Druids.newSearchQueryBuilder()
                 .dataSource(datasource)
                 .intervals(Collections.singletonList(interval))
                 .granularity(granularity)
                 .limit(10000)
                 .query("wow")
                 .build();
  }

  private Future assertQueryable(
      Granularity granularity,
      String dataSource,
      Interval interval,
      List<Pair<String, Interval>> expected
  )
  {
    final Iterator<Pair<String, Interval>> expectedIter = expected.iterator();
    final List<Interval> intervals = Collections.singletonList(interval);
    final SearchQuery query = searchQuery(dataSource, interval, granularity);
    final QueryRunner<Result<SearchResultValue>> runner = serverManager.getQueryRunnerForIntervals(
        query,
        intervals
    );
    return serverManagerExec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
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
        }
    );
  }

  public void loadQueryable(String dataSource, String version, Interval interval)
  {
    try {
      segmentManager.loadSegment(
          new DataSegment(
              dataSource,
              interval,
              version,
              ImmutableMap.of("version", version, "interval", interval),
              Arrays.asList("dim1", "dim2", "dim3"),
              Arrays.asList("metric1", "metric2"),
              NoneShardSpec.instance(),
              IndexIO.CURRENT_VERSION_ID,
              123L
          ),
          false
      );
    }
    catch (SegmentLoadingException e) {
      throw new RuntimeException(e);
    }
  }

  public void dropQueryable(String dataSource, String version, Interval interval)
  {
    segmentManager.dropSegment(
        new DataSegment(
            dataSource,
            interval,
            version,
            ImmutableMap.of("version", version, "interval", interval),
            Arrays.asList("dim1", "dim2", "dim3"),
            Arrays.asList("metric1", "metric2"),
            NoneShardSpec.instance(),
            IndexIO.CURRENT_VERSION_ID,
            123L
        )
    );
  }

  public static class MyQueryRunnerFactory implements QueryRunnerFactory<Result<SearchResultValue>, SearchQuery>
  {
    private final CountDownLatch waitLatch;
    private final CountDownLatch waitYieldLatch;
    private final CountDownLatch notifyLatch;
    private List<SegmentForTesting> adapters = new ArrayList<>();
    private List<ReferenceCountingSegment> segmentReferences = new ArrayList<>();


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
      if (!(adapter instanceof ReferenceCountingSegment)) {
        throw new IAE("Expected instance of ReferenceCountingSegment, got %s", adapter.getClass());
      }
      final ReferenceCountingSegment segment = (ReferenceCountingSegment) adapter;

      Assert.assertTrue(segment.getNumReferences() > 0);
      segmentReferences.add(segment);
      adapters.add((SegmentForTesting) segment.getBaseSegment());
      return new BlockingQueryRunner<>(new NoopQueryRunner<>(), waitLatch, waitYieldLatch, notifyLatch);
    }

    @Override
    public QueryRunner<Result<SearchResultValue>> mergeRunners(
        ExecutorService queryExecutor,
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
      return new TypeReference<T>()
      {
      };
    }
  }

  private static class SegmentForTesting implements Segment
  {
    private final String version;
    private final Interval interval;
    private final Object lock = new Object();
    private volatile boolean closed = false;

    SegmentForTesting(
        String version,
        Interval interval
    )
    {
      this.version = version;
      this.interval = interval;
    }

    public String getVersion()
    {
      return version;
    }

    public Interval getInterval()
    {
      return interval;
    }

    @Override
    public SegmentId getId()
    {
      return SegmentId.dummy(version);
    }

    public boolean isClosed()
    {
      return closed;
    }

    @Override
    public Interval getDataInterval()
    {
      return interval;
    }

    @Override
    public QueryableIndex asQueryableIndex()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public StorageAdapter asStorageAdapter()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
      synchronized (lock) {
        closed = true;
      }
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
      return new Yielder<OutType>()
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

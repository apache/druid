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

package io.druid.server.coordination;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceMetricEvent;

import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.LocalCacheProvider;
import io.druid.granularity.QueryGranularities;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.java.util.common.guava.YieldingSequenceBase;
import io.druid.query.ConcatQueryRunner;
import io.druid.query.Druids;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.search.SearchResultValue;
import io.druid.query.search.search.SearchQuery;
import io.druid.segment.AbstractSegment;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.loading.SegmentLoader;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;

import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 */
public class ServerManagerTest
{
  private ServerManager serverManager;
  private MyQueryRunnerFactory factory;
  private CountDownLatch queryWaitLatch;
  private CountDownLatch queryWaitYieldLatch;
  private CountDownLatch queryNotifyLatch;
  private ExecutorService serverManagerExec;

  @Before
  public void setUp() throws IOException
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());

    queryWaitLatch = new CountDownLatch(1);
    queryWaitYieldLatch = new CountDownLatch(1);
    queryNotifyLatch = new CountDownLatch(1);
    factory = new MyQueryRunnerFactory(queryWaitLatch, queryWaitYieldLatch, queryNotifyLatch);
    serverManagerExec = Executors.newFixedThreadPool(2);
    serverManager = new ServerManager(
        new SegmentLoader()
        {
          @Override
          public boolean isSegmentLoaded(DataSegment segment) throws SegmentLoadingException
          {
            return false;
          }

          @Override
          public Segment getSegment(final DataSegment segment)
          {
            return new SegmentForTesting(
                MapUtils.getString(segment.getLoadSpec(), "version"),
                (Interval) segment.getLoadSpec().get("interval")
            );
          }

          @Override
          public File getSegmentFiles(DataSegment segment) throws SegmentLoadingException
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public void cleanup(DataSegment segment) throws SegmentLoadingException
          {

          }
        },
        new QueryRunnerFactoryConglomerate()
        {
          @Override
          public <T, QueryType extends Query<T>> QueryRunnerFactory<T, QueryType> findFactory(QueryType query)
          {
            return (QueryRunnerFactory) factory;
          }
        },
        new NoopServiceEmitter(),
        serverManagerExec,
        MoreExecutors.sameThreadExecutor(),
        new DefaultObjectMapper(),
        new LocalCacheProvider().get(),
        new CacheConfig()
    );

    loadQueryable("test", "1", new Interval("P1d/2011-04-01"));
    loadQueryable("test", "1", new Interval("P1d/2011-04-02"));
    loadQueryable("test", "2", new Interval("P1d/2011-04-02"));
    loadQueryable("test", "1", new Interval("P1d/2011-04-03"));
    loadQueryable("test", "1", new Interval("P1d/2011-04-04"));
    loadQueryable("test", "1", new Interval("P1d/2011-04-05"));
    loadQueryable("test", "2", new Interval("PT1h/2011-04-04T01"));
    loadQueryable("test", "2", new Interval("PT1h/2011-04-04T02"));
    loadQueryable("test", "2", new Interval("PT1h/2011-04-04T03"));
    loadQueryable("test", "2", new Interval("PT1h/2011-04-04T05"));
    loadQueryable("test", "2", new Interval("PT1h/2011-04-04T06"));
    loadQueryable("test2", "1", new Interval("P1d/2011-04-01"));
    loadQueryable("test2", "1", new Interval("P1d/2011-04-02"));
  }

  @Test
  public void testSimpleGet()
  {
    Future future = assertQueryable(
        QueryGranularities.DAY,
        "test",
        new Interval("P1d/2011-04-01"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("1", new Interval("P1d/2011-04-01"))
        )
    );
    waitForTestVerificationAndCleanup(future);


    future = assertQueryable(
        QueryGranularities.DAY,
        "test", new Interval("P2d/2011-04-02"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("1", new Interval("P1d/2011-04-01")),
            new Pair<String, Interval>("2", new Interval("P1d/2011-04-02"))
        )
    );
    waitForTestVerificationAndCleanup(future);
  }

  @Test
  public void testDelete1() throws Exception
  {
    final String dataSouce = "test";
    final Interval interval = new Interval("2011-04-01/2011-04-02");

    Future future = assertQueryable(
        QueryGranularities.DAY,
        dataSouce, interval,
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("2", interval)
        )
    );
    waitForTestVerificationAndCleanup(future);

    dropQueryable(dataSouce, "2", interval);
    future = assertQueryable(
        QueryGranularities.DAY,
        dataSouce, interval,
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("1", interval)
        )
    );
    waitForTestVerificationAndCleanup(future);
  }

  @Test
  public void testDelete2() throws Exception
  {
    loadQueryable("test", "3", new Interval("2011-04-04/2011-04-05"));

    Future future = assertQueryable(
        QueryGranularities.DAY,
        "test", new Interval("2011-04-04/2011-04-06"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("3", new Interval("2011-04-04/2011-04-05"))
        )
    );
    waitForTestVerificationAndCleanup(future);

    dropQueryable("test", "3", new Interval("2011-04-04/2011-04-05"));
    dropQueryable("test", "1", new Interval("2011-04-04/2011-04-05"));

    future = assertQueryable(
        QueryGranularities.HOUR,
        "test", new Interval("2011-04-04/2011-04-04T06"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("2", new Interval("2011-04-04T00/2011-04-04T01")),
            new Pair<String, Interval>("2", new Interval("2011-04-04T01/2011-04-04T02")),
            new Pair<String, Interval>("2", new Interval("2011-04-04T02/2011-04-04T03")),
            new Pair<String, Interval>("2", new Interval("2011-04-04T04/2011-04-04T05")),
            new Pair<String, Interval>("2", new Interval("2011-04-04T05/2011-04-04T06"))
        )
    );
    waitForTestVerificationAndCleanup(future);

    future = assertQueryable(
        QueryGranularities.HOUR,
        "test", new Interval("2011-04-04/2011-04-04T03"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("2", new Interval("2011-04-04T00/2011-04-04T01")),
            new Pair<String, Interval>("2", new Interval("2011-04-04T01/2011-04-04T02")),
            new Pair<String, Interval>("2", new Interval("2011-04-04T02/2011-04-04T03"))
        )
    );
    waitForTestVerificationAndCleanup(future);

    future = assertQueryable(
        QueryGranularities.HOUR,
        "test", new Interval("2011-04-04T04/2011-04-04T06"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("2", new Interval("2011-04-04T04/2011-04-04T05")),
            new Pair<String, Interval>("2", new Interval("2011-04-04T05/2011-04-04T06"))
        )
    );
    waitForTestVerificationAndCleanup(future);
  }

  @Test
  public void testReferenceCounting() throws Exception
  {
    loadQueryable("test", "3", new Interval("2011-04-04/2011-04-05"));

    Future future = assertQueryable(
        QueryGranularities.DAY,
        "test", new Interval("2011-04-04/2011-04-06"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("3", new Interval("2011-04-04/2011-04-05"))
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

    dropQueryable("test", "3", new Interval("2011-04-04/2011-04-05"));

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertTrue(segmentForTesting.isClosed());
    }
  }

  @Test
  public void testReferenceCountingWhileQueryExecuting() throws Exception
  {
    loadQueryable("test", "3", new Interval("2011-04-04/2011-04-05"));

    Future future = assertQueryable(
        QueryGranularities.DAY,
        "test", new Interval("2011-04-04/2011-04-06"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("3", new Interval("2011-04-04/2011-04-05"))
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

    dropQueryable("test", "3", new Interval("2011-04-04/2011-04-05"));

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
    loadQueryable("test", "3", new Interval("2011-04-04/2011-04-05"));

    Future future = assertQueryable(
        QueryGranularities.DAY,
        "test", new Interval("2011-04-04/2011-04-06"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("3", new Interval("2011-04-04/2011-04-05"))
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

    dropQueryable("test", "3", new Interval("2011-04-04/2011-04-05"));
    dropQueryable("test", "3", new Interval("2011-04-04/2011-04-05"));

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertFalse(segmentForTesting.isClosed());
    }

    queryWaitLatch.countDown();
    future.get();

    for (SegmentForTesting segmentForTesting : factory.getAdapters()) {
      Assert.assertTrue(segmentForTesting.isClosed());
    }
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
      throw Throwables.propagate(e);
    }
  }

  private <T> Future assertQueryable(
      QueryGranularity granularity,
      String dataSource,
      Interval interval,
      List<Pair<String, Interval>> expected
  )
  {
    final Iterator<Pair<String, Interval>> expectedIter = expected.iterator();
    final List<Interval> intervals = Arrays.asList(interval);
    final SearchQuery query = Druids.newSearchQueryBuilder()
                                    .dataSource(dataSource)
                                    .intervals(intervals)
                                    .granularity(granularity)
                                    .limit(10000)
                                    .query("wow")
                                    .build();
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
            Map<String, Object> context = new HashMap<String, Object>();
            Sequence<Result<SearchResultValue>> seq = runner.run(query, context);
            Sequences.toList(seq, Lists.<Result<SearchResultValue>>newArrayList());
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

  public void loadQueryable(String dataSource, String version, Interval interval) throws IOException
  {
    try {
      serverManager.loadSegment(
          new DataSegment(
              dataSource,
              interval,
              version,
              ImmutableMap.<String, Object>of("version", version, "interval", interval),
              Arrays.asList("dim1", "dim2", "dim3"),
              Arrays.asList("metric1", "metric2"),
              NoneShardSpec.instance(),
              IndexIO.CURRENT_VERSION_ID,
              123L
          )
      );
    }
    catch (SegmentLoadingException e) {
      throw new RuntimeException(e);
    }
  }

  public void dropQueryable(String dataSource, String version, Interval interval)
  {
    try {
      serverManager.dropSegment(
          new DataSegment(
              dataSource,
              interval,
              version,
              ImmutableMap.<String, Object>of("version", version, "interval", interval),
              Arrays.asList("dim1", "dim2", "dim3"),
              Arrays.asList("metric1", "metric2"),
              NoneShardSpec.instance(),
              IndexIO.CURRENT_VERSION_ID,
              123L
          )
      );
    }
    catch (SegmentLoadingException e) {
      throw new RuntimeException(e);
    }
  }

  public static class MyQueryRunnerFactory implements QueryRunnerFactory<Result<SearchResultValue>, SearchQuery>
  {
    private final CountDownLatch waitLatch;
    private final CountDownLatch waitYieldLatch;
    private final CountDownLatch notifyLatch;
    private List<SegmentForTesting> adapters = Lists.newArrayList();
    private List<ReferenceCountingSegment> segmentReferences = Lists.newArrayList();


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
      return new BlockingQueryRunner<Result<SearchResultValue>>(
          new NoopQueryRunner<Result<SearchResultValue>>(), waitLatch, waitYieldLatch, notifyLatch
      );
    }

    @Override
    public QueryRunner<Result<SearchResultValue>> mergeRunners(
        ExecutorService queryExecutor, Iterable<QueryRunner<Result<SearchResultValue>>> queryRunners
    )
    {
      return new ConcatQueryRunner<Result<SearchResultValue>>(Sequences.simple(queryRunners));
    }

    @Override
    public QueryToolChest<Result<SearchResultValue>, SearchQuery> getToolchest()
    {
      return new NoopQueryToolChest<Result<SearchResultValue>, SearchQuery>();
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
    public ServiceMetricEvent.Builder makeMetricBuilder(QueryType query)
    {
      return new ServiceMetricEvent.Builder();
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

  private static class SegmentForTesting extends AbstractSegment
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
    public String getIdentifier()
    {
      return version;
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
    public void close() throws IOException
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
    public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
    {
      return new BlockingSequence<T>(runner.run(query, responseContext), waitLatch, waitYieldLatch, notifyLatch);
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
        final OutType initValue, final YieldingAccumulator<OutType, T> accumulator
    )
    {
      notifyLatch.countDown();

      try {
        waitYieldLatch.await(1000, TimeUnit.MILLISECONDS);
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
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
            throw Throwables.propagate(e);
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

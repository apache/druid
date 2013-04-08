/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.coordination;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.MapUtils;
import com.metamx.common.Pair;
import com.metamx.common.guava.ConcatSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.Druids;
import com.metamx.druid.Query;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.QueryableIndex;
import com.metamx.druid.index.Segment;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.loading.SegmentLoader;
import com.metamx.druid.loading.SegmentLoadingException;
import com.metamx.druid.metrics.NoopServiceEmitter;
import com.metamx.druid.query.CacheStrategy;
import com.metamx.druid.query.ConcatQueryRunner;
import com.metamx.druid.query.MetricManipulationFn;
import com.metamx.druid.query.NoopQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.QueryRunnerFactoryConglomerate;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.search.SearchQuery;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.SearchResultValue;
import com.metamx.druid.shard.NoneShardSpec;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceMetricEvent;

import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 */
public class ServerManagerTest
{
  ServerManager serverManager;
  MyQueryRunnerFactory factory;

  @Before
  public void setUp() throws IOException
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());

    factory = new MyQueryRunnerFactory();
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
        MoreExecutors.sameThreadExecutor()
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
    assertQueryable(
        QueryGranularity.DAY,
        "test",
        new Interval("P1d/2011-04-01"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("1", new Interval("P1d/2011-04-01"))
        )
    );

    assertQueryable(
        QueryGranularity.DAY,
        "test", new Interval("P2d/2011-04-02"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("1", new Interval("P1d/2011-04-01")),
            new Pair<String, Interval>("2", new Interval("P1d/2011-04-02"))
        )
    );
  }

  @Test
  public void testDelete1() throws Exception
  {
    final String dataSouce = "test";
    final Interval interval = new Interval("2011-04-01/2011-04-02");

    assertQueryable(
        QueryGranularity.DAY,
        dataSouce, interval,
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("2", interval)
        )
    );

    dropQueryable(dataSouce, "2", interval);
    assertQueryable(
        QueryGranularity.DAY,
        dataSouce, interval,
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("1", interval)
        )
    );
  }

  @Test
  public void testDelete2() throws Exception
  {
    loadQueryable("test", "3", new Interval("2011-04-04/2011-04-05"));

    assertQueryable(
        QueryGranularity.DAY,
        "test", new Interval("2011-04-04/2011-04-06"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("3", new Interval("2011-04-04/2011-04-05"))
        )
    );

    dropQueryable("test", "3", new Interval("2011-04-04/2011-04-05"));
    dropQueryable("test", "1", new Interval("2011-04-04/2011-04-05"));

    assertQueryable(
        QueryGranularity.HOUR,
        "test", new Interval("2011-04-04/2011-04-04T06"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("2", new Interval("2011-04-04T00/2011-04-04T01")),
            new Pair<String, Interval>("2", new Interval("2011-04-04T01/2011-04-04T02")),
            new Pair<String, Interval>("2", new Interval("2011-04-04T02/2011-04-04T03")),
            new Pair<String, Interval>("2", new Interval("2011-04-04T04/2011-04-04T05")),
            new Pair<String, Interval>("2", new Interval("2011-04-04T05/2011-04-04T06"))
        )
    );

    assertQueryable(
        QueryGranularity.HOUR,
        "test", new Interval("2011-04-04/2011-04-04T03"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("2", new Interval("2011-04-04T00/2011-04-04T01")),
            new Pair<String, Interval>("2", new Interval("2011-04-04T01/2011-04-04T02")),
            new Pair<String, Interval>("2", new Interval("2011-04-04T02/2011-04-04T03"))
        )
    );

    assertQueryable(
        QueryGranularity.HOUR,
        "test", new Interval("2011-04-04T04/2011-04-04T06"),
        ImmutableList.<Pair<String, Interval>>of(
            new Pair<String, Interval>("2", new Interval("2011-04-04T04/2011-04-04T05")),
            new Pair<String, Interval>("2", new Interval("2011-04-04T05/2011-04-04T06"))
        )
    );
  }

  private void loadQueryable(String dataSource, String version, Interval interval) throws IOException
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
              new NoneShardSpec(),
              IndexIO.CURRENT_VERSION_ID,
              123l
          )
      );
    }
    catch (SegmentLoadingException e) {
      throw new RuntimeException(e);
    }
  }

  private void dropQueryable(String dataSource, String version, Interval interval)
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
              new NoneShardSpec(),
              IndexIO.CURRENT_VERSION_ID,
              123l
          )
      );
    }
    catch (SegmentLoadingException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> void assertQueryable(
      QueryGranularity granularity,
      String dataSource,
      Interval interval,
      List<Pair<String, Interval>> expected
  )
  {
    Iterator<Pair<String, Interval>> expectedIter = expected.iterator();
    final List<Interval> intervals = Arrays.asList(interval);
    final SearchQuery query = Druids.newSearchQueryBuilder()
                                  .dataSource(dataSource)
                                  .intervals(intervals)
                                  .granularity(granularity)
                                  .limit(10000)
                                  .query("wow")
                                  .build();
    QueryRunner<Result<SearchResultValue>> runner = serverManager.getQueryRunnerForIntervals(query, intervals);
    final Sequence<Result<SearchResultValue>> seq = runner.run(query);
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

    factory.clearAdapters();
  }

  private static class SegmentForTesting implements Segment
  {
    private final String version;
    private final Interval interval;

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
  }

  public static class MyQueryRunnerFactory implements QueryRunnerFactory<Result<SearchResultValue>, SearchQuery>
  {
    private List<SegmentForTesting> adapters = Lists.newArrayList();

    @Override
    public QueryRunner<Result<SearchResultValue>> createRunner(Segment adapter)
    {
      adapters.add((SegmentForTesting) adapter);
      return new NoopQueryRunner<Result<SearchResultValue>>();
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
    public Sequence<T> mergeSequences(Sequence<Sequence<T>> seqOfSequences)
    {
      return new ConcatSequence<T>(seqOfSequences);
    }

    @Override
    public ServiceMetricEvent.Builder makeMetricBuilder(QueryType query)
    {
      return new ServiceMetricEvent.Builder();
    }

    @Override
    public Function<T, T> makeMetricManipulatorFn(QueryType query, MetricManipulationFn fn)
    {
      return Functions.identity();
    }

    @Override
    public TypeReference<T> getResultTypeReference()
    {
      return new TypeReference<T>(){};
    }
  }
}

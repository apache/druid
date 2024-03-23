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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.ProvisionException;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.FluentQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.having.HavingSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.scheduling.HiLoQueryLaningStrategy;
import org.apache.druid.server.scheduling.ManualQueryPrioritizationStrategy;
import org.apache.druid.server.scheduling.NoQueryLaningStrategy;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class QuerySchedulerTest
{
  private static final int NUM_QUERIES = 10000;
  private static final int NUM_ROWS = 10000;
  private static final int TEST_HI_CAPACITY = 5;
  private static final int TEST_LO_CAPACITY = 2;
  private static final ServerConfig SERVER_CONFIG_WITHOUT_TOTAL = new ServerConfig();
  private static final ServerConfig SERVER_CONFIG_WITH_TOTAL = new ServerConfig(false);

  private ListeningExecutorService executorService;
  private ObservableQueryScheduler scheduler;

  @Before
  public void setup()
  {
    executorService = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(64, "test_query_scheduler_%s")
    );
    scheduler = new ObservableQueryScheduler(
        TEST_HI_CAPACITY,
        ManualQueryPrioritizationStrategy.INSTANCE,
        new HiLoQueryLaningStrategy(40),
        // Test with total laning turned on
        SERVER_CONFIG_WITH_TOTAL
    );
  }

  @After
  public void teardown()
  {
    executorService.shutdownNow();
  }

  @Test
  public void testHiLoHi() throws ExecutionException, InterruptedException
  {
    TopNQuery interactive = makeInteractiveQuery();
    ListenableFuture<?> future = executorService.submit(() -> {
      try {
        Query<?> scheduled = scheduler.prioritizeAndLaneQuery(QueryPlus.wrap(interactive), ImmutableSet.of());

        Assert.assertNotNull(scheduled);

        Sequence<Integer> underlyingSequence = makeSequence(10);
        underlyingSequence = Sequences.wrap(underlyingSequence, new SequenceWrapper()
        {
          @Override
          public void before()
          {
            Assert.assertEquals(4, scheduler.getTotalAvailableCapacity());
            Assert.assertEquals(2, scheduler.getLaneAvailableCapacity(HiLoQueryLaningStrategy.LOW));
          }
        });
        Sequence<Integer> results = scheduler.run(scheduled, underlyingSequence);
        int rowCount = consumeAndCloseSequence(results);

        Assert.assertEquals(10, rowCount);
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    });
    future.get();
    Assert.assertEquals(TEST_HI_CAPACITY, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(QueryScheduler.UNAVAILABLE, scheduler.getLaneAvailableCapacity("non-existent"));
  }

  @Test
  public void testHiLoLo() throws ExecutionException, InterruptedException
  {
    TopNQuery report = makeReportQuery();
    ListenableFuture<?> future = executorService.submit(() -> {
      try {
        Query<?> scheduledReport = scheduler.prioritizeAndLaneQuery(QueryPlus.wrap(report), ImmutableSet.of());
        Assert.assertNotNull(scheduledReport);
        Assert.assertEquals(HiLoQueryLaningStrategy.LOW, scheduledReport.context().getLane());

        Sequence<Integer> underlyingSequence = makeSequence(10);
        underlyingSequence = Sequences.wrap(underlyingSequence, new SequenceWrapper()
        {
          @Override
          public void before()
          {
            Assert.assertEquals(4, scheduler.getTotalAvailableCapacity());
            Assert.assertEquals(1, scheduler.getLaneAvailableCapacity(HiLoQueryLaningStrategy.LOW));
          }
        });
        Sequence<Integer> results = scheduler.run(scheduledReport, underlyingSequence);

        int rowCount = consumeAndCloseSequence(results);
        Assert.assertEquals(10, rowCount);
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    });
    future.get();
    assertHiLoHasAllCapacity(TEST_HI_CAPACITY, TEST_LO_CAPACITY);
    Assert.assertEquals(QueryScheduler.UNAVAILABLE, scheduler.getLaneAvailableCapacity("non-existent"));
  }

  @Test
  public void testHiLoReleaseLaneWhenSequenceExplodes()
  {
    TopNQuery interactive = makeInteractiveQuery();
    ListenableFuture<?> future = executorService.submit(() -> {
      try {
        Query<?> scheduled = scheduler.prioritizeAndLaneQuery(QueryPlus.wrap(interactive), ImmutableSet.of());

        Assert.assertNotNull(scheduled);

        Sequence<Integer> underlyingSequence = makeExplodingSequence(10);
        underlyingSequence = Sequences.wrap(underlyingSequence, new SequenceWrapper()
        {
          @Override
          public void before()
          {
            Assert.assertEquals(4, scheduler.getTotalAvailableCapacity());
          }
        });
        Sequence<Integer> results = scheduler.run(scheduled, underlyingSequence);

        consumeAndCloseSequence(results);
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    });
    Throwable t = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals("java.lang.RuntimeException: exploded", t.getMessage());
    Assert.assertEquals(5, scheduler.getTotalAvailableCapacity());
  }

  @Test
  public void testHiLoFailsWhenOutOfLaneCapacity()
  {
    Query<?> report1 = scheduler.prioritizeAndLaneQuery(QueryPlus.wrap(makeReportQuery()), ImmutableSet.of());
    Sequence<?> sequence = scheduler.run(report1, Sequences.empty());
    // making the sequence doesn't count, only running it does
    Assert.assertEquals(5, scheduler.getTotalAvailableCapacity());
    // this counts though since we are doing stuff
    Yielders.each(sequence);
    Assert.assertNotNull(report1);
    Assert.assertEquals(4, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(1, scheduler.getLaneAvailableCapacity(HiLoQueryLaningStrategy.LOW));

    Query<?> report2 = scheduler.prioritizeAndLaneQuery(QueryPlus.wrap(makeReportQuery()), ImmutableSet.of());
    Yielders.each(scheduler.run(report2, Sequences.empty()));
    Assert.assertNotNull(report2);
    Assert.assertEquals(3, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(0, scheduler.getLaneAvailableCapacity(HiLoQueryLaningStrategy.LOW));

    // too many reports
    Throwable t = Assert.assertThrows(
        QueryCapacityExceededException.class,
        () -> Yielders.each(
            scheduler.run(
                scheduler.prioritizeAndLaneQuery(QueryPlus.wrap(makeReportQuery()), ImmutableSet.of()),
                Sequences.empty()
            )
        )
    );
    Assert.assertEquals(
        "Too many concurrent queries for lane 'low', query capacity of 2 exceeded. Please try your query again later.",
        t.getMessage()
    );
  }

  @Test
  public void testHiLoFailsWhenOutOfTotalCapacity()
  {
    Query<?> interactive1 = scheduler.prioritizeAndLaneQuery(QueryPlus.wrap(makeInteractiveQuery()), ImmutableSet.of());
    Sequence<?> sequence = scheduler.run(interactive1, Sequences.empty());
    // making the sequence doesn't count, only running it does
    Assert.assertEquals(5, scheduler.getTotalAvailableCapacity());
    // this counts tho
    Yielders.each(sequence);
    Assert.assertNotNull(interactive1);
    Assert.assertEquals(4, scheduler.getTotalAvailableCapacity());

    Query<?> report1 = scheduler.prioritizeAndLaneQuery(QueryPlus.wrap(makeReportQuery()), ImmutableSet.of());
    Yielders.each(scheduler.run(report1, Sequences.empty()));
    Assert.assertNotNull(report1);
    Assert.assertEquals(3, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(1, scheduler.getLaneAvailableCapacity(HiLoQueryLaningStrategy.LOW));

    Query<?> interactive2 = scheduler.prioritizeAndLaneQuery(QueryPlus.wrap(makeInteractiveQuery()), ImmutableSet.of());
    Yielders.each(scheduler.run(interactive2, Sequences.empty()));
    Assert.assertNotNull(interactive2);
    Assert.assertEquals(2, scheduler.getTotalAvailableCapacity());

    Query<?> report2 = scheduler.prioritizeAndLaneQuery(QueryPlus.wrap(makeReportQuery()), ImmutableSet.of());
    Yielders.each(scheduler.run(report2, Sequences.empty()));
    Assert.assertNotNull(report2);
    Assert.assertEquals(1, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(0, scheduler.getLaneAvailableCapacity(HiLoQueryLaningStrategy.LOW));

    Query<?> interactive3 = scheduler.prioritizeAndLaneQuery(QueryPlus.wrap(makeInteractiveQuery()), ImmutableSet.of());
    Yielders.each(scheduler.run(interactive3, Sequences.empty()));
    Assert.assertNotNull(interactive3);
    Assert.assertEquals(0, scheduler.getTotalAvailableCapacity());

    // one too many
    Throwable t = Assert.assertThrows(
        QueryCapacityExceededException.class,
        () -> Yielders.each(scheduler.run(
            scheduler.prioritizeAndLaneQuery(QueryPlus.wrap(makeInteractiveQuery()), ImmutableSet.of()),
            Sequences.empty()
        ))
    );
    Assert.assertEquals(
        "Too many concurrent queries, total query capacity of 5 exceeded. Please try your query again later.",
        t.getMessage()
    );
  }

  @Test
  public void testConcurrency() throws Exception
  {
    List<Future<?>> futures = new ArrayList<>(NUM_QUERIES);
    for (int i = 0; i < NUM_QUERIES; i++) {
      futures.add(makeQueryFuture(executorService, scheduler, makeRandomQuery(), NUM_ROWS));
      maybeDelayNextIteration(i);
    }
    getFuturesAndAssertAftermathIsChill(futures, scheduler, false, false);
    assertHiLoHasAllCapacity(TEST_HI_CAPACITY, TEST_LO_CAPACITY);
  }

  @Test
  public void testConcurrencyLo() throws Exception
  {
    List<Future<?>> futures = new ArrayList<>(NUM_QUERIES);
    for (int i = 0; i < NUM_QUERIES; i++) {
      futures.add(makeQueryFuture(executorService, scheduler, makeReportQuery(), NUM_ROWS));
      maybeDelayNextIteration(i);
    }
    getFuturesAndAssertAftermathIsChill(futures, scheduler, false, false);
    assertHiLoHasAllCapacity(TEST_HI_CAPACITY, TEST_LO_CAPACITY);
  }

  @Test
  public void testConcurrencyHi() throws Exception
  {
    List<Future<?>> futures = new ArrayList<>(NUM_QUERIES);
    for (int i = 0; i < NUM_QUERIES; i++) {
      futures.add(makeQueryFuture(executorService, scheduler, makeInteractiveQuery(), NUM_ROWS));
      maybeDelayNextIteration(i);
    }
    getFuturesAndAssertAftermathIsChill(futures, scheduler, true, false);
    assertHiLoHasAllCapacity(TEST_HI_CAPACITY, TEST_LO_CAPACITY);
  }

  @Test
  public void testNotLimitedByDefaultLimiterIfNoTotalIsSet()
  {
    scheduler = new ObservableQueryScheduler(
        0,
        ManualQueryPrioritizationStrategy.INSTANCE,
        new NoQueryLaningStrategy(),
        SERVER_CONFIG_WITHOUT_TOTAL
    );
    List<Future<?>> futures = new ArrayList<>(NUM_QUERIES);
    for (int i = 0; i < NUM_QUERIES; i++) {
      futures.add(makeQueryFuture(executorService, scheduler, makeInteractiveQuery(), NUM_ROWS));
    }
    getFuturesAndAssertAftermathIsChill(futures, scheduler, true, true);
  }

  @Test
  public void testTotalLimitWithoutQueryQueuing()
  {
    ServerConfig serverConfig = SERVER_CONFIG_WITH_TOTAL;
    QueryScheduler queryScheduler = new QueryScheduler(
        serverConfig.getNumThreads() - 1,
        ManualQueryPrioritizationStrategy.INSTANCE,
        new NoQueryLaningStrategy(),
        serverConfig
    );
    Assert.assertEquals(serverConfig.getNumThreads() - 1, queryScheduler.getTotalAvailableCapacity());
  }

  @Test
  public void testTotalLimitWithQueryQueuing()
  {
    ServerConfig serverConfig = SERVER_CONFIG_WITHOUT_TOTAL;
    QueryScheduler queryScheduler = new QueryScheduler(
        serverConfig.getNumThreads() - 1,
        ManualQueryPrioritizationStrategy.INSTANCE,
        new NoQueryLaningStrategy(),
        serverConfig
    );
    Assert.assertEquals(-1, queryScheduler.getTotalAvailableCapacity());
  }

  @Test
  public void testExplodingWrapperDoesNotLeakLocks()
  {
    scheduler = new ObservableQueryScheduler(
        5,
        ManualQueryPrioritizationStrategy.INSTANCE,
        new NoQueryLaningStrategy(),
        SERVER_CONFIG_WITH_TOTAL
    );

    QueryRunnerFactory factory = GroupByQueryRunnerTest.makeQueryRunnerFactory(
        new GroupByQueryConfig()
        {

          @Override
          public String toString()
          {
            return "v2";
          }
        }
    );
    Future<?> f = makeMergingQueryFuture(
        executorService,
        scheduler,
        GroupByQuery.builder()
                    .setDataSource("foo")
                    .setInterval("2020-01-01/2020-01-02")
                    .setDimensions(DefaultDimensionSpec.of("bar"))
                    .setAggregatorSpecs(new CountAggregatorFactory("chocula"))
                    .setGranularity(Granularities.ALL)
                    .setHavingSpec(
                        new HavingSpec()
                        {
                          @Override
                          public void setQuery(GroupByQuery query)
                          {
                            throw new RuntimeException("exploded");
                          }

                          @Override
                          public boolean eval(ResultRow row)
                          {
                            return false;
                          }

                          @Override
                          public byte[] getCacheKey()
                          {
                            return new byte[0];
                          }
                        }
                    )
                    .build(),
        factory.getToolchest(),
        NUM_ROWS
    );

    Assert.assertEquals(5, scheduler.getTotalAvailableCapacity());
    Throwable t = Assert.assertThrows(Throwable.class, f::get);
    Assert.assertEquals("java.lang.RuntimeException: exploded", t.getMessage());
    Assert.assertEquals(5, scheduler.getTotalAvailableCapacity());
  }

  @Test
  public void testConfigNone()
  {
    final Injector injector = createInjector();
    final String propertyPrefix = "druid.query.scheduler";
    final JsonConfigProvider<QuerySchedulerProvider> provider = JsonConfigProvider.of(
        propertyPrefix,
        QuerySchedulerProvider.class
    );
    final Properties properties = new Properties();
    properties.setProperty(propertyPrefix + ".numThreads", "10");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final QueryScheduler scheduler = provider.get().get();
    Assert.assertEquals(10, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(QueryScheduler.UNAVAILABLE, scheduler.getLaneAvailableCapacity(HiLoQueryLaningStrategy.LOW));
    Assert.assertEquals(QueryScheduler.UNAVAILABLE, scheduler.getLaneAvailableCapacity("non-existent"));
  }

  @Test
  public void testConfigHiLo()
  {
    final Injector injector = createInjector();
    final String propertyPrefix = "druid.query.scheduler";
    final JsonConfigProvider<QuerySchedulerProvider> provider = JsonConfigProvider.of(
        propertyPrefix,
        QuerySchedulerProvider.class
    );
    final Properties properties = new Properties();
    properties.setProperty(propertyPrefix + ".numThreads", "10");
    properties.setProperty(propertyPrefix + ".laning.strategy", "hilo");
    properties.setProperty(propertyPrefix + ".laning.maxLowPercent", "20");

    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final QueryScheduler scheduler = provider.get().get();
    Assert.assertEquals(10, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(2, scheduler.getLaneAvailableCapacity(HiLoQueryLaningStrategy.LOW));
    Assert.assertEquals(QueryScheduler.UNAVAILABLE, scheduler.getLaneAvailableCapacity("non-existent"));
  }


  @Test
  public void testMisConfigHiLo()
  {
    final Injector injector = createInjector();
    final String propertyPrefix = "druid.query.scheduler";
    final JsonConfigProvider<QuerySchedulerProvider> provider = JsonConfigProvider.of(
        propertyPrefix,
        QuerySchedulerProvider.class
    );
    final Properties properties = new Properties();
    properties.setProperty(propertyPrefix + ".laning.strategy", "hilo");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    Throwable t = Assert.assertThrows(ProvisionException.class, () -> provider.get().get());
    Assert.assertEquals(
        "Unable to provision, see the following errors:\n"
        + "\n"
        + "1) Problem parsing object at prefix[druid.query.scheduler]: Cannot construct instance of `org.apache.druid.server.scheduling.HiLoQueryLaningStrategy`, problem: maxLowPercent must be set\n"
        + " at [Source: UNKNOWN; line: -1, column: -1] (through reference chain: org.apache.druid.server.QuerySchedulerProvider[\"laning\"]).\n"
        + "\n"
        + "1 error",
        t.getMessage()
    );
  }

  @Test
  public void testConfigHiLoWithThreshold()
  {
    final Injector injector = createInjector();
    final String propertyPrefix = "druid.query.scheduler";
    final JsonConfigProvider<QuerySchedulerProvider> provider = JsonConfigProvider.of(
        propertyPrefix,
        QuerySchedulerProvider.class
    );
    final Properties properties = new Properties();
    properties.setProperty(propertyPrefix + ".numThreads", "10");
    properties.setProperty(propertyPrefix + ".laning.strategy", "hilo");
    properties.setProperty(propertyPrefix + ".laning.maxLowPercent", "20");
    properties.setProperty(propertyPrefix + ".prioritization.strategy", "threshold");
    properties.setProperty(propertyPrefix + ".prioritization.adjustment", "5");
    properties.setProperty(propertyPrefix + ".prioritization.segmentCountThreshold", "1");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final QueryScheduler scheduler = provider.get().get();
    Assert.assertEquals(10, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(2, scheduler.getLaneAvailableCapacity(HiLoQueryLaningStrategy.LOW));
    Assert.assertEquals(QueryScheduler.UNAVAILABLE, scheduler.getLaneAvailableCapacity("non-existent"));

    Query<?> query = scheduler.prioritizeAndLaneQuery(
        QueryPlus.wrap(makeDefaultQuery()),
        ImmutableSet.of(
            EasyMock.createMock(SegmentServerSelector.class),
            EasyMock.createMock(SegmentServerSelector.class)
        )
    );
    Assert.assertEquals(-5, query.context().getPriority());
    Assert.assertEquals(HiLoQueryLaningStrategy.LOW, query.context().getLane());
  }

  @Test
  public void testMisConfigThreshold()
  {
    final Injector injector = createInjector();
    final String propertyPrefix = "druid.query.scheduler";
    final JsonConfigProvider<QuerySchedulerProvider> provider = JsonConfigProvider.of(
        propertyPrefix,
        QuerySchedulerProvider.class
    );
    final Properties properties = new Properties();
    properties.setProperty(propertyPrefix + ".prioritization.strategy", "threshold");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    Throwable t = Assert.assertThrows(ProvisionException.class, () -> provider.get().get());
    Assert.assertEquals(
        "Unable to provision, see the following errors:\n"
        + "\n"
        + "1) Problem parsing object at prefix[druid.query.scheduler]: Cannot construct instance of `org.apache.druid.server.scheduling.ThresholdBasedQueryPrioritizationStrategy`, problem: periodThreshold, durationThreshold, or segmentCountThreshold must be set\n"
        + " at [Source: UNKNOWN; line: -1, column: -1] (through reference chain: org.apache.druid.server.QuerySchedulerProvider[\"prioritization\"]).\n"
        + "\n"
        + "1 error",
        t.getMessage()
    );
  }


  @Test
  public void testConfigManual()
  {
    final Injector injector = createInjector();
    final String propertyPrefix = "druid.query.scheduler";
    final JsonConfigProvider<QuerySchedulerProvider> provider = JsonConfigProvider.of(
        propertyPrefix,
        QuerySchedulerProvider.class
    );
    final Properties properties = new Properties();
    properties.put(propertyPrefix + ".numThreads", "10");
    properties.put(propertyPrefix + ".laning.strategy", "manual");
    properties.put(propertyPrefix + ".laning.lanes.one", "1");
    properties.put(propertyPrefix + ".laning.lanes.two", "2");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final QueryScheduler scheduler = provider.get().get();
    Assert.assertEquals(10, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(1, scheduler.getLaneAvailableCapacity("one"));
    Assert.assertEquals(2, scheduler.getLaneAvailableCapacity("two"));
    Assert.assertEquals(QueryScheduler.UNAVAILABLE, scheduler.getLaneAvailableCapacity("non-existent"));
  }

  @Test
  public void testConfigManualPercent()
  {
    final Injector injector = createInjector();
    final String propertyPrefix = "druid.query.scheduler";
    final JsonConfigProvider<QuerySchedulerProvider> provider = JsonConfigProvider.of(
        propertyPrefix,
        QuerySchedulerProvider.class
    );
    final Properties properties = new Properties();
    properties.put(propertyPrefix + ".numThreads", "10");
    properties.put(propertyPrefix + ".laning.strategy", "manual");
    properties.put(propertyPrefix + ".laning.isLimitPercent", "true");
    properties.put(propertyPrefix + ".laning.lanes.one", "1");
    properties.put(propertyPrefix + ".laning.lanes.twenty", "20");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final QueryScheduler scheduler = provider.get().get();
    Assert.assertEquals(10, scheduler.getTotalAvailableCapacity());
    Assert.assertEquals(1, scheduler.getLaneAvailableCapacity("one"));
    Assert.assertEquals(2, scheduler.getLaneAvailableCapacity("twenty"));
    Assert.assertEquals(QueryScheduler.UNAVAILABLE, scheduler.getLaneAvailableCapacity("non-existent"));
  }

  private void maybeDelayNextIteration(int i) throws InterruptedException
  {
    if (i > 0 && i % 10 == 0) {
      Thread.sleep(2);
    }
  }

  private TopNQuery makeRandomQuery()
  {
    return ThreadLocalRandom.current().nextBoolean() ? makeInteractiveQuery() : makeReportQuery();
  }

  private TopNQuery makeDefaultQuery()
  {
    return makeBaseBuilder()
        .context(ImmutableMap.of("queryId", "default-" + UUID.randomUUID()))
        .build();
  }

  private TopNQuery makeInteractiveQuery()
  {
    return makeBaseBuilder()
        .context(ImmutableMap.of("priority", 10, "queryId", "high-" + UUID.randomUUID()))
        .build();
  }

  private TopNQuery makeReportQuery()
  {
    return makeBaseBuilder()
        .context(ImmutableMap.of("priority", -1, "queryId", "low-" + UUID.randomUUID()))
        .build();
  }

  private TopNQueryBuilder makeBaseBuilder()
  {
    return new TopNQueryBuilder()
        .dataSource("foo")
        .intervals("2020-01-01/2020-01-02")
        .dimension("bar")
        .metric("chocula")
        .aggregators(new CountAggregatorFactory("chocula"))
        .threshold(10);
  }

  private <T> int consumeAndCloseSequence(Sequence<T> sequence) throws IOException
  {
    Yielder<T> yielder = Yielders.each(sequence);
    int rowCount = 0;
    while (!yielder.isDone()) {
      rowCount++;
      yielder = yielder.next(yielder.get());
    }
    yielder.close();
    return rowCount;
  }

  private Sequence<Integer> makeSequence(int count)
  {
    return new LazySequence<>(() -> {
      return new BaseSequence<>(
          new BaseSequence.IteratorMaker<Integer, Iterator<Integer>>()
          {
            @Override
            public Iterator<Integer> make()
            {
              return new Iterator<Integer>()
              {
                int rowCounter = 0;

                @Override
                public boolean hasNext()
                {
                  return rowCounter < count;
                }

                @Override
                public Integer next()
                {
                  rowCounter++;
                  return rowCounter;
                }
              };
            }

            @Override
            public void cleanup(Iterator<Integer> iterFromMake)
            {
              // nothing to cleanup
            }
          }
      );
    });
  }

  private Sequence<Integer> makeExplodingSequence(int explodeAfter)
  {
    final int explodeAt = explodeAfter + 1;
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<Integer, Iterator<Integer>>()
        {
          @Override
          public Iterator<Integer> make()
          {
            return new Iterator<Integer>()
            {
              int rowCounter = 0;

              @Override
              public boolean hasNext()
              {
                return rowCounter < explodeAt;
              }

              @Override
              public Integer next()
              {
                if (rowCounter == explodeAfter) {
                  throw new RuntimeException("exploded");
                }

                rowCounter++;
                return rowCounter;
              }
            };
          }

          @Override
          public void cleanup(Iterator<Integer> iterFromMake)
          {
            // nothing to cleanup
          }
        }
    );
  }

  private ListenableFuture<?> makeQueryFuture(
      ListeningExecutorService executorService,
      QueryScheduler scheduler,
      Query<?> query,
      int numRows
  )
  {
    return executorService.submit(() -> {
      try {
        Query<?> scheduled = scheduler.prioritizeAndLaneQuery(QueryPlus.wrap(query), ImmutableSet.of());

        Assert.assertNotNull(scheduled);

        Sequence<Integer> underlyingSequence = makeSequence(numRows);
        Sequence<Integer> results = scheduler.run(scheduled, underlyingSequence);

        final int actualNumRows = consumeAndCloseSequence(results);
        Assert.assertEquals(actualNumRows, numRows);
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    });
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private ListenableFuture<?> makeMergingQueryFuture(
      ListeningExecutorService executorService,
      QueryScheduler scheduler,
      Query<?> query,
      QueryToolChest toolChest,
      int numRows
  )
  {
    return executorService.submit(() -> {
      try {
        Query<?> scheduled = scheduler.prioritizeAndLaneQuery(QueryPlus.wrap(query), ImmutableSet.of());

        Assert.assertNotNull(scheduled);

        FluentQueryRunner runner = FluentQueryRunner
            .create(
                (queryPlus, responseContext) -> {
                  Sequence<Integer> underlyingSequence = makeSequence(numRows);
                  Sequence<Integer> results = scheduler.run(scheduled, underlyingSequence);
                  return (Sequence) results;
                },
                toolChest
            )
            .applyPreMergeDecoration()
            .mergeResults()
            .applyPostMergeDecoration();

        final int actualNumRows = consumeAndCloseSequence(runner.run(QueryPlus.wrap(query)));
        Assert.assertEquals(actualNumRows, numRows);
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    });
  }


  private void getFuturesAndAssertAftermathIsChill(
      List<Future<?>> futures,
      ObservableQueryScheduler scheduler,
      boolean successEqualsTotal,
      boolean expectNoneLimited
  )
  {
    int success = 0;
    int denied = 0;
    int other = 0;
    for (Future<?> f : futures) {
      try {
        f.get();
        success++;
      }
      catch (ExecutionException ex) {
        if (ex.getCause() instanceof QueryCapacityExceededException) {
          denied++;
        } else {
          other++;
        }
      }
      catch (Exception ex) {
        other++;
      }
    }
    Assert.assertEquals(0, other);
    if (expectNoneLimited) {
      Assert.assertEquals(0, denied);
      Assert.assertEquals(NUM_QUERIES, success);
      Assert.assertEquals(0, scheduler.getTotalAcquired().get());
      Assert.assertEquals(0, scheduler.getLaneAcquired().get());
    } else {
      Assert.assertTrue(denied > 0);
      if (successEqualsTotal) {
        Assert.assertEquals(success, scheduler.getTotalAcquired().get());
      } else {
        Assert.assertTrue(success > 0 && success <= scheduler.getTotalAcquired().get());
      }
      Assert.assertEquals(scheduler.getTotalReleased().get(), scheduler.getTotalAcquired().get());
      Assert.assertEquals(
          scheduler.getLaneReleased().get(),
          scheduler.getLaneAcquired().get() + scheduler.getLaneNotAcquired().get()
      );
    }
  }

  private void assertHiLoHasAllCapacity(int hi, int lo)
  {
    Assert.assertEquals(lo, scheduler.getLaneAvailableCapacity(HiLoQueryLaningStrategy.LOW));
    Assert.assertEquals(hi, scheduler.getTotalAvailableCapacity());
  }

  private Injector createInjector()
  {
    Injector injector = GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.of(
            binder -> {
              binder.bind(ServerConfig.class).toInstance(SERVER_CONFIG_WITH_TOTAL);
              binder.bind(ServiceEmitter.class).toInstance(new ServiceEmitter("test", "localhost", new NoopEmitter()));
              JsonConfigProvider.bind(binder, "druid.query.scheduler", QuerySchedulerProvider.class, Global.class);
            }
        )
    );
    ObjectMapper mapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ServerConfig.class, injector.getInstance(ServerConfig.class))
            .addValue(ServiceEmitter.class, injector.getInstance(ServiceEmitter.class))
    );
    return injector;
  }
}

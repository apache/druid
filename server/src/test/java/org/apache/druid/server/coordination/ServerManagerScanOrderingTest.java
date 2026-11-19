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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
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
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.Druids;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.Order;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.DefaultResponseContext;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.loading.AcquireMode;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.ServerManager;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.test.utils.TestSegmentCacheManager;
import org.apache.druid.test.utils.TestSegmentManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Tests for time-ordered scan queries through {@link ServerManager} when some segments are loaded on demand.
 */
public class ServerManagerScanOrderingTest
{
  private static final ScanQuery.ResultFormat RESULT_FORMAT = ScanQuery.ResultFormat.RESULT_FORMAT_LIST;
  private static final List<Interval> DAYS = List.of(
      Intervals.of("2011-04-01/2011-04-02"),
      Intervals.of("2011-04-02/2011-04-03"),
      Intervals.of("2011-04-03/2011-04-04")
  );
  private static final int ROWS_PER_DAY = 3;

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

  @Inject
  private ServerManager serverManager;

  private OnDemandSegmentCacheManager cacheManager;
  private ExecutorService exec;
  private List<SegmentDescriptor> descriptors;
  private List<DataSegment> dataSegments;

  @BeforeEach
  public void setUp() throws Exception
  {
    serviceEmitter = new NoopServiceEmitter();
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    cacheManager = new OnDemandSegmentCacheManager();
    segmentManager = new SegmentManager(cacheManager);

    descriptors = new ArrayList<>();
    dataSegments = new ArrayList<>();
    for (final Interval day : DAYS) {
      final SegmentId segmentId = SegmentId.of("test", day, "1", 0);
      final DataSegment dataSegment = TestSegmentManager.createDataSegmentForTest(segmentId);
      final List<InputRow> rows = new ArrayList<>();
      for (int hour = 0; hour < ROWS_PER_DAY; hour++) {
        rows.add(
            new MapBasedInputRow(
                day.getStart().plusHours(hour),
                List.of("dim"),
                Map.of("dim", "x")
            )
        );
      }
      cacheManager.registerSegment(
          dataSegment,
          new IncrementalIndexSegment(IndexBuilder.create().rows(rows).buildIncrementalIndex(), segmentId)
      );
      segmentManager.loadSegment(dataSegment);
      dataSegments.add(dataSegment);
      descriptors.add(segmentId.toDescriptor());
    }

    conglomerate = DefaultQueryRunnerFactoryConglomerate.buildFromQueryRunnerFactories(
        ImmutableMap.of(
            ScanQuery.class,
            new ScanQueryRunnerFactory(
                new ScanQueryQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
                new ScanQueryEngine(),
                new ScanQueryConfig()
            )
        )
    );
    exec = Execs.multiThreaded(2, "ServerManagerScanOrderingTest-%d");
    queryProcessingPool = new ForwardingQueryProcessingPool(exec);
    cachePopulator = new ForegroundCachePopulator(new DefaultObjectMapper(), new CachePopulatorStats(), -1);
    objectMapper = new DefaultObjectMapper();
    cache = new LocalCacheProvider().get();
    cacheConfig = new CacheConfig();
    serverConfig = new ServerConfig();
    policyEnforcer = NoopPolicyEnforcer.instance();

    Guice.createInjector(BoundFieldModule.of(this)).injectMembers(this);
  }

  @AfterEach
  public void tearDown()
  {
    exec.shutdownNow();
  }

  @Test
  public void testAscendingUnlimited_allCached()
  {
    Assertions.assertEquals(expectedTimes(Order.ASCENDING, Long.MAX_VALUE), runScan(Order.ASCENDING, Long.MAX_VALUE));
  }

  @Test
  public void testAscendingUnlimited_firstDayOnDemand()
  {
    cacheManager.evictFromCache(dataSegments.get(0));
    Assertions.assertEquals(expectedTimes(Order.ASCENDING, Long.MAX_VALUE), runScan(Order.ASCENDING, Long.MAX_VALUE));
  }

  @Test
  public void testDescendingUnlimited_firstDayOnDemand()
  {
    cacheManager.evictFromCache(dataSegments.get(0));
    Assertions.assertEquals(
        expectedTimes(Order.DESCENDING, Long.MAX_VALUE),
        runScan(Order.DESCENDING, Long.MAX_VALUE)
    );
  }

  @Test
  public void testAscendingLimited_allCached()
  {
    Assertions.assertEquals(expectedTimes(Order.ASCENDING, 5), runScan(Order.ASCENDING, 5));
  }

  @Test
  public void testAscendingLimited_firstDayOnDemand()
  {
    cacheManager.evictFromCache(dataSegments.get(0));
    Assertions.assertEquals(expectedTimes(Order.ASCENDING, 5), runScan(Order.ASCENDING, 5));
  }

  @Test
  public void testDescendingLimited_firstDayOnDemand()
  {
    cacheManager.evictFromCache(dataSegments.get(0));
    Assertions.assertEquals(expectedTimes(Order.DESCENDING, 5), runScan(Order.DESCENDING, 5));
  }

  @Test
  public void testAscendingUnlimited_missingSegment()
  {
    final SegmentDescriptor missing = new SegmentDescriptor(DAYS.get(1), "unknown_version", 0);
    descriptors.add(1, missing);
    final DefaultResponseContext responseContext = DefaultResponseContext.createEmpty();
    Assertions.assertEquals(
        expectedTimes(Order.ASCENDING, Long.MAX_VALUE),
        runScan(Order.ASCENDING, Long.MAX_VALUE, responseContext)
    );
    Assertions.assertEquals(List.of(missing), responseContext.getMissingSegments());
  }

  @Test
  public void testDescendingLimited_missingSegment()
  {
    final SegmentDescriptor missing = new SegmentDescriptor(DAYS.get(1), "unknown_version", 0);
    descriptors.add(1, missing);
    final DefaultResponseContext responseContext = DefaultResponseContext.createEmpty();
    Assertions.assertEquals(expectedTimes(Order.DESCENDING, 5), runScan(Order.DESCENDING, 5, responseContext));
    Assertions.assertEquals(List.of(missing), responseContext.getMissingSegments());
  }

  @Test
  public void testAscendingUnlimited_onDemandAndMissingSegments()
  {
    cacheManager.evictFromCache(dataSegments.get(0));
    final SegmentDescriptor missing = new SegmentDescriptor(DAYS.get(1), "unknown_version", 0);
    descriptors.add(1, missing);
    final DefaultResponseContext responseContext = DefaultResponseContext.createEmpty();
    Assertions.assertEquals(
        expectedTimes(Order.ASCENDING, Long.MAX_VALUE),
        runScan(Order.ASCENDING, Long.MAX_VALUE, responseContext)
    );
    Assertions.assertEquals(List.of(missing), responseContext.getMissingSegments());
  }

  @Test
  public void testDescendingLimited_onDemandAndMissingSegments()
  {
    cacheManager.evictFromCache(dataSegments.get(0));
    final SegmentDescriptor missing = new SegmentDescriptor(DAYS.get(1), "unknown_version", 0);
    descriptors.add(1, missing);
    final DefaultResponseContext responseContext = DefaultResponseContext.createEmpty();
    Assertions.assertEquals(expectedTimes(Order.DESCENDING, 5), runScan(Order.DESCENDING, 5, responseContext));
    Assertions.assertEquals(List.of(missing), responseContext.getMissingSegments());
  }

  private List<Long> runScan(final Order order, final long limit)
  {
    return runScan(order, limit, DefaultResponseContext.createEmpty());
  }

  private List<Long> runScan(final Order order, final long limit, final DefaultResponseContext responseContext)
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource("test")
                                  .intervals(new MultipleSpecificSegmentSpec(descriptors))
                                  .columns("__time", "dim")
                                  .resultFormat(RESULT_FORMAT)
                                  .order(order)
                                  .limit(limit)
                                  .batchSize(1)
                                  .build();
    final List<ScanResultValue> results = serverManager.getQueryRunnerForSegments(query, descriptors)
                                                       .run(QueryPlus.wrap(query), responseContext)
                                                       .toList();
    final List<Long> times = new ArrayList<>();
    for (final ScanResultValue result : results) {
      for (final ScanResultValue single : result.toSingleEventScanResultValues()) {
        times.add(single.getFirstEventTimestamp(RESULT_FORMAT));
      }
    }
    return times;
  }

  private static List<Long> expectedTimes(final Order order, final long limit)
  {
    final List<Long> times = new ArrayList<>();
    for (final Interval day : DAYS) {
      for (int hour = 0; hour < ROWS_PER_DAY; hour++) {
        times.add(day.getStart().plusHours(hour).getMillis());
      }
    }
    if (order == Order.DESCENDING) {
      Collections.reverse(times);
    }
    return new ArrayList<>(times.subList(0, (int) Math.min(limit, times.size())));
  }

  /**
   * Cache manager that can report specific segments as absent from the cache but loadable on demand.
   */
  private static class OnDemandSegmentCacheManager extends TestSegmentCacheManager
  {
    private final Set<SegmentId> evicted = new HashSet<>();

    void evictFromCache(final DataSegment dataSegment)
    {
      evicted.add(dataSegment.getId());
    }

    @Override
    public Optional<Segment> acquireCachedSegment(final SegmentId segmentId, final AcquireMode acquireMode)
    {
      if (evicted.contains(segmentId)) {
        return Optional.empty();
      }
      return super.acquireCachedSegment(segmentId, acquireMode);
    }

    @Override
    public boolean canLoadSegmentsOnDemand()
    {
      return true;
    }

    @Override
    public boolean canLoadSegmentOnDemand(final DataSegment dataSegment)
    {
      return true;
    }
  }
}

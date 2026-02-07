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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.google.inject.util.Providers;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.SegmentWranglerModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.CompactionIntervalSpec;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.CompactionTaskRunBase;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.TuningConfigBuilder;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.indexing.IndexerControllerContextFactory;
import org.apache.druid.msq.indexing.MSQCompactionRunner;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.test.MSQTestControllerContext;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.query.rowsandcols.serde.WireTransferableContext;
import org.apache.druid.segment.DataSegmentsWithSchemas;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.util.LookylooModule;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for CompactionTask using MSQCompactionRunner.
 * Extends CompactionTaskRunTest to reuse all test infrastructure.
 */
@RunWith(Parameterized.class)
public class MSQCompactionTaskRunTest extends CompactionTaskRunBase
{
  private final ConcurrentHashMap<String, TaskActionClient> taskActionClients = new ConcurrentHashMap<>();
  private Injector injector;

  @Parameterized.Parameters(name = "name: {0}, inputInterval={5}, segmentGran={6}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    for (LockGranularity lockGranularity : new LockGranularity[]{LockGranularity.TIME_CHUNK}) {
      for (boolean useCentralizedDatasourceSchema : new boolean[]{false}) {
        for (boolean batchSegmentAllocation : new boolean[]{false, true}) {
          for (boolean useSegmentMetadataCache : new boolean[]{false, true}) {
            for (boolean useConcurrentLocks : new boolean[]{false, true}) {
              for (Interval inputInterval : new Interval[]{TEST_INTERVAL}) {
                for (Granularity segmentGran : new Granularity[]{Granularities.SIX_HOUR}) {
                  String name = StringUtils.format(
                      "lockGranularity=%s, useCentralizedDatasourceSchema=%s, batchSegmentAllocation=%s, useSegmentMetadataCache=%s, useConcurrentLocks=%s",
                      lockGranularity,
                      useCentralizedDatasourceSchema,
                      batchSegmentAllocation,
                      useSegmentMetadataCache,
                      useConcurrentLocks
                  );
                  constructors.add(new Object[]{
                      name,
                      lockGranularity,
                      useCentralizedDatasourceSchema,
                      batchSegmentAllocation,
                      useSegmentMetadataCache,
                      useConcurrentLocks,
                      inputInterval,
                      segmentGran
                  });
                }
              }
            }
          }
        }
      }
    }
    return constructors;
  }

  public MSQCompactionTaskRunTest(
      String name,
      LockGranularity lockGranularity,
      boolean useCentralizedDatasourceSchema,
      boolean batchSegmentAllocation,
      boolean useSegmentMetadataCache,
      boolean useConcurrentLocks,
      Interval inputInterval,
      Granularity compactionGranularities
  ) throws IOException
  {
    super(
        name,
        lockGranularity,
        useCentralizedDatasourceSchema,
        batchSegmentAllocation,
        useSegmentMetadataCache,
        useConcurrentLocks,
        inputInterval,
        compactionGranularities
    );
  }

  @Override
  public void registerTaskActionClient(String taskId, TaskActionClient taskActionClient)
  {
    Preconditions.checkState(taskActionClients.put(taskId, taskActionClient) == null);
  }

  @Before
  public void setUpMSQ()
  {
    objectMapper.registerModules(new MSQIndexingModule().getJacksonModules());
    ComplexMetrics.registerSerde(NestedDataComplexTypeSerde.TYPE_NAME, NestedDataComplexTypeSerde.INSTANCE);

    SegmentCacheManager segmentCacheManager = mock(SegmentCacheManager.class);
    when(segmentCacheManager.acquireSegment(any())).thenAnswer(invocation -> {
      DataSegment segment = invocation.getArgument(0);
      QueryableIndexSegment index = new QueryableIndexSegment(
          new TestUtils().getTestIndexIO().loadIndex(new File((String) segment.getLoadSpec().get("path"))),
          segment.getId()
      );
      return new AcquireSegmentAction(
          () -> Futures.immediateFuture(new AcquireSegmentResult(new ReferenceCountedSegmentProvider(index), 0, 0, 0)),
          null
      );
    });
    when(segmentCacheManager.acquireCachedSegment(any())).thenAnswer(invocation -> {
      DataSegment segment = invocation.getArgument(0);
      QueryableIndexSegment index = new QueryableIndexSegment(
          new TestUtils().getTestIndexIO().loadIndex(new File((String) segment.getLoadSpec().get("path"))),
          segment.getId()
      );
      return Optional.of(index);
    });
    GroupingEngine groupingEngine = GroupByQueryRunnerTest.makeQueryRunnerFactory(
        new GroupByQueryConfig(),
        TestGroupByBuffers.createDefault()
    ).getGroupingEngine();

    Module modules = Modules.combine(
        new DruidGuiceExtensions(),
        new LifecycleModule(),
        new SegmentWranglerModule(),
        new LookylooModule(),
        new MSQIndexingModule(),
        binder -> binder.bind(PolicyEnforcer.class).toInstance(NoopPolicyEnforcer.instance()),
        binder -> binder.bind(WireTransferableContext.class).toInstance(new WireTransferableContext(null, null, true)),
        binder -> binder.bind(DataSegmentPusher.class)
                        .toInstance(new LocalDataSegmentPusher(new LocalDataSegmentPusherConfig())),
        binder -> binder.bind(DataServerQueryHandlerFactory.class).toProvider(Providers.of(null)),
        binder -> binder.bind(Escalator.class).toProvider(Providers.of(null)),
        binder -> binder.bind(QueryProcessingPool.class)
                        .toInstance(new ForwardingQueryProcessingPool(Execs.singleThreaded("Test-runner-processing-pool"))),
        binder -> binder.bind(ObjectMapper.class).annotatedWith(Json.class).toInstance(objectMapper),
        binder -> binder.bind(SegmentCacheManager.class).toInstance(segmentCacheManager),
        binder -> binder.bind(GroupingEngine.class).toInstance(groupingEngine)
    );
    injector = Guice.createInjector(modules);
    // bind IndexerControllerContextFactory to build a MSQTestControllerContext, overriding the one in MSQIndexingModule
    injector = Guice.createInjector(
        Modules.override(modules)
               .with(binder -> binder.bind(IndexerControllerContextFactory.class)
                                     .toInstance(new IndexerControllerContextFactory(null, null, null)
                                     {
                                       @Override
                                       public ControllerContext buildWithTask(
                                           MSQControllerTask cTask,
                                           TaskToolbox unused
                                       )
                                       {
                                         return new MSQTestControllerContext(
                                             cTask.getId(),
                                             objectMapper,
                                             injector,
                                             taskActionClients.get(cTask.getId()),
                                             MSQTestBase.makeTestWorkerMemoryParameters(),
                                             cTask.getTaskLockType(),
                                             cTask.getQuerySpec().getContext(),
                                             new StubServiceEmitter(),
                                             coordinatorClient
                                         );
                                       }
                                     })));
  }

  protected MSQCompactionRunner getMSQCompactionRunner()
  {
    return new MSQCompactionRunner(
        objectMapper,
        TestExprMacroTable.INSTANCE,
        injector
    );
  }

  @Override
  @Ignore("Hash paritioning is not supported in MSQ")
  @Test
  public void testRunWithHashPartitioning()
  {
  }

  @Override
  @Ignore("dropExisting must set to true in MSQ")
  @Test
  public void testPartialIntervalCompactWithFinerSegmentGranularityThenFullIntervalCompactWithDropExistingFalse()
  {
  }

  @Override
  @Ignore("allowNonAlignedInterval is not supported in MSQ")
  @Test
  public void testWithSegmentGranularityMisalignedIntervalAllowed()
  {
  }

  @Override
  @Ignore("allowNonAlignedInterval is not supported in MSQ")
  @Test
  public void testWithSegmentGranularityMisalignedIntervalAllowed2()
  {
  }

  @Override
  @Test
  public void testCompactionWithNewMetricInMetricsSpec() throws Exception
  {
    // MSQ doesn't support count aggregator
    Assume.assumeTrue(segmentGranularity != null && !segmentGranularity.isFinerThan(Granularities.SIX_HOUR));
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    final CompactionTask compactionTask =
        compactionTaskBuilder(new ClientCompactionTaskGranularitySpec(segmentGranularity, null, true))
            .interval(TEST_INTERVAL_DAY, true)
            .metricsSpec(new AggregatorFactory[]{new LongMaxAggregatorFactory("val", "val")})
            .build();

    Pair<TaskStatus, DataSegmentsWithSchemas> resultPair = runTask(compactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair, TOTAL_TEST_ROWS);

    List<DataSegment> segments = new ArrayList<>(resultPair.rhs.getSegments());
    Assert.assertEquals(1, segments.size());

    Assert.assertEquals(inputInterval, segments.get(0).getInterval());
    Assert.assertEquals(new NumberedShardSpec(0, 1), segments.get(0).getShardSpec());

    CompactionState expectedCompactionState =
        getDefaultCompactionState(segmentGranularity, Granularities.MINUTE, List.of(TEST_INTERVAL))
            .toBuilder()
            .metricsSpec(List.of(new LongMaxAggregatorFactory("val", "val")))
            .build();
    validateCompactionState(expectedCompactionState, segments.get(0).getLastCompactionState());
  }

  @Override
  @Test
  public void testPartialIntervalCompactWithFinerSegmentGranularityThanFullIntervalCompactWithDropExistingTrue()
      throws Exception
  {
    // This test is almost identical to base, except for fullCompactionTask, since MSQ doesn't allow disjoint intervals.
    // This test fails with segment lock because of the bug reported in https://github.com/apache/druid/issues/10911.
    Assume.assumeTrue(lockGranularity != LockGranularity.SEGMENT);
    Assume.assumeTrue(
        "test with defined segment granularity in this test",
        Granularities.SIX_HOUR.equals(segmentGranularity)
    );

    // The following task creates (several, more than three, last time I checked, six) HOUR segments with intervals of
    // - 2014-01-01T00:00:00/2014-01-01T01:00:00
    // - 2014-01-01T01:00:00/2014-01-01T02:00:00
    // - 2014-01-01T02:00:00/2014-01-01T03:00:00
    // The six segments are:
    // three rows in hour 00:
    // 2014-01-01T00:00:00.000Z_2014-01-01T01:00:00.000Z with two rows
    // 2014-01-01T00:00:00.000Z_2014-01-01T01:00:00.000Z_1 with one row
    // three rows in hour 01:
    // 2014-01-01T01:00:00.000Z_2014-01-01T02:00:00.000Z with two rows
    // 2014-01-01T01:00:00.000Z_2014-01-01T02:00:00.000Z_1 with one row
    // four rows in hour 02:
    // 2014-01-01T02:00:00.000Z_2014-01-01T03:00:00.000Z with two rows
    // 2014-01-01T02:00:00.000Z_2014-01-01T03:00:00.000Z_1 with two rows
    // there are 10 rows total in data set

    // maxRowsPerSegment is set to 2 inside the runIndexTask methods
    Pair<TaskStatus, DataSegmentsWithSchemas> result = runIndexTask();
    verifyTaskSuccessRowsAndSchemaMatch(result, TOTAL_TEST_ROWS);
    Assert.assertEquals(6, result.rhs.getSegments().size());

    // Setup partial compaction:
    // Change the granularity from HOUR to MINUTE through compaction for hour 01, there are three rows in the compaction interval,
    // all three in the same timestamp (see TEST_ROWS), this should generate one segments (task will now use
    // the default rows per segments since compaction's tuning config is null) in same minute and
    // 59 tombstones to completely overshadow the existing hour 01 segment. Since the segments outside the
    // compaction interval should remanin unchanged there should be a total of 1 + (2 + 59) + 2 = 64 segments

    // **** PARTIAL COMPACTION: hour -> minute ****
    final Interval compactionPartialInterval = Intervals.of("2014-01-01T01:00:00/2014-01-01T02:00:00");
    final CompactionTask partialCompactionTask =
        compactionTaskBuilder(Granularities.MINUTE)
            // Set dropExisting to true
            .inputSpec(new CompactionIntervalSpec(compactionPartialInterval, null), true)
            .build();
    final Pair<TaskStatus, DataSegmentsWithSchemas> partialCompactionResult = runTask(partialCompactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(partialCompactionResult, 3);

    // Segments that did not belong in the compaction interval (hours 00 and 02) are expected unchanged
    // add 2 unchanged segments for hour 00:
    final Set<DataSegment> expectedSegments = new HashSet<>();
    expectedSegments.addAll(
        coordinatorClient.fetchUsedSegments(
            DATA_SOURCE,
            List.of(Intervals.of("2014-01-01T00:00:00/2014-01-01T01:00:00"))
        ).get());
    // add 2 unchanged segments for hour 02:
    expectedSegments.addAll(
        coordinatorClient.fetchUsedSegments(
            DATA_SOURCE,
            List.of(Intervals.of("2014-01-01T02:00:00/2014-01-01T03:00:00"))
        ).get());
    expectedSegments.addAll(partialCompactionResult.rhs.getSegments());
    Assert.assertEquals(64, expectedSegments.size());

    // New segments that were compacted are expected. However, old segments of the compacted interval should be
    // overshadowed by the new tombstones (59) being created for all minutes other than 01:01
    final Set<DataSegment> segmentsAfterPartialCompaction = new HashSet<>(
        coordinatorClient.fetchUsedSegments(DATA_SOURCE, List.of(Intervals.of("2014-01-01/2014-01-02"))).get());
    Assert.assertEquals(expectedSegments, segmentsAfterPartialCompaction);
    final List<DataSegment> realSegmentsAfterPartialCompaction =
        segmentsAfterPartialCompaction.stream().filter(s -> !s.isTombstone()).collect(Collectors.toList());
    final List<DataSegment> tombstonesAfterPartialCompaction =
        segmentsAfterPartialCompaction.stream().filter(s -> s.isTombstone()).collect(Collectors.toList());
    Assert.assertEquals(59, tombstonesAfterPartialCompaction.size());
    Assert.assertEquals(5, realSegmentsAfterPartialCompaction.size());
    Assert.assertEquals(64, segmentsAfterPartialCompaction.size());

    // Setup full compaction:
    // Reindex with new MINUTE segment granularity. MSQ engine doesn't support disjoint intervals.
    // each hour has 59 tombstones, and 1 real segment.
    final CompactionTask fullCompactionTask =
        compactionTaskBuilder(Granularities.MINUTE).interval(TEST_INTERVAL, true).build();

    // **** FULL COMPACTION ****
    final Pair<TaskStatus, DataSegmentsWithSchemas> fullCompactionResult = runTask(fullCompactionTask);
    verifyTaskSuccessRowsAndSchemaMatch(fullCompactionResult, TOTAL_TEST_ROWS);

    final List<DataSegment> segmentsAfterFullCompaction = new ArrayList<>(
        coordinatorClient.fetchUsedSegments(DATA_SOURCE, List.of(TEST_INTERVAL)).get());
    segmentsAfterFullCompaction.sort(
        (s1, s2) -> Comparators.intervalsByStartThenEnd().compare(s1.getInterval(), s2.getInterval())
    );
    Assert.assertEquals(180, segmentsAfterFullCompaction.size());

    final List<DataSegment> tombstonesAfterFullCompaction =
        segmentsAfterFullCompaction.stream().filter(s -> s.isTombstone()).collect(Collectors.toList());
    Assert.assertEquals(177, tombstonesAfterFullCompaction.size());

    final List<DataSegment> realSegmentsAfterFullCompaction =
        segmentsAfterFullCompaction.stream().filter(s -> !s.isTombstone()).collect(Collectors.toList());
    Assert.assertEquals(3, realSegmentsAfterFullCompaction.size());

    Assert.assertEquals(
        Intervals.of("2014-01-01T00:00:00.000Z/2014-01-01T00:01:00.000Z"),
        realSegmentsAfterFullCompaction.get(0).getInterval()
    );
    Assert.assertEquals(
        Intervals.of("2014-01-01T01:00:00.000Z/2014-01-01T01:01:00.000Z"),
        realSegmentsAfterFullCompaction.get(1).getInterval()
    );
    Assert.assertEquals(
        Intervals.of("2014-01-01T02:00:00.000Z/2014-01-01T02:01:00.000Z"),
        realSegmentsAfterFullCompaction.get(2).getInterval()
    );
  }

  @Test
  public void testMSQCompactionWithConcurrentAppendCompactionLocksFirst() throws Exception
  {
    Assume.assumeTrue(useConcurrentLocks);
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    final CompactionTask compactionTask =
        compactionTaskBuilder(segmentGranularity).interval(inputInterval, true).build();

    // make sure that compactionTask becomes ready first, then the appendTask becomes ready, then compactionTask runs
    final CountDownLatch appendTaskReadyLatch = new CountDownLatch(1);
    final CountDownLatch compactionTaskStartLatch = new CountDownLatch(1);
    final Future<Pair<TaskStatus, DataSegmentsWithSchemas>> compactionFuture = exec.submit(
        () -> runTask(compactionTask, appendTaskReadyLatch, compactionTaskStartLatch)
    );

    List<String> rows = new ArrayList<>();
    rows.add("2014-01-01T00:00:10Z,a1,11\n");
    rows.add("2014-01-01T00:00:10Z,b1,12\n");
    rows.add("2014-01-01T00:00:10Z,c1,13\n");
    rows.add("2014-01-01T01:00:20Z,a1,11\n");
    rows.add("2014-01-01T01:00:20Z,b1,12\n");
    rows.add("2014-01-01T01:00:20Z,c1,13\n");
    rows.add("2014-01-01T02:00:30Z,a1,11\n");
    rows.add("2014-01-01T02:00:30Z,b1,12\n");
    rows.add("2014-01-01T02:00:30Z,c1,13\n");
    final IndexTask appendTask = buildIndexTask(
        DEFAULT_PARSE_SPEC,
        rows,
        Intervals.of("2014-01-01T00:00:00Z/2014-01-01T03:00:00Z"),
        true
    );
    final Future<Pair<TaskStatus, DataSegmentsWithSchemas>> appendFuture = exec.submit(
        () -> {
          appendTaskReadyLatch.await();
          return runTask(appendTask, compactionTaskStartLatch, appendTaskReadyLatch);
        }
    );

    verifyTaskSuccessRowsAndSchemaMatch(appendFuture.get(), 9);
    List<DataSegment> segments = new ArrayList<>(appendFuture.get().rhs.getSegments());
    Assert.assertEquals(6, segments.size());

    final Pair<TaskStatus, DataSegmentsWithSchemas> compactionResult = compactionFuture.get();
    verifyTaskSuccessRowsAndSchemaMatch(compactionResult, TOTAL_TEST_ROWS);
    Assert.assertEquals(1, compactionResult.rhs.getSegments().size());

    final Set<DataSegment> usedSegments = new HashSet<>(
        coordinatorClient.fetchUsedSegments(DATA_SOURCE, List.of(Intervals.of("2014-01-01/2014-01-02"))).get());
    Assert.assertEquals(7, usedSegments.size());
    final String version = Iterables.getOnlyElement(compactionResult.rhs.getSegments()).getVersion();
    Assert.assertTrue(usedSegments.stream().allMatch(segment -> segment.getVersion().equals(version)));

    CompactionTask finalTask = compactionTaskBuilder(segmentGranularity).interval(inputInterval, true).build();
    Pair<TaskStatus, DataSegmentsWithSchemas> finalResult = runTask(finalTask);
    verifyTaskSuccessRowsAndSchemaMatch(finalResult, 19);
  }


  @Test
  public void testMSQCompactionWithConcurrentAppendAppendLocksFirst() throws Exception
  {
    Assume.assumeTrue(useConcurrentLocks);
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    final CompactionTask compactionTask =
        compactionTaskBuilder(segmentGranularity).interval(inputInterval, true).build();

    // make sure that appendTask becomes ready first, then compactionTask becomes ready, then indexTask runs
    final CountDownLatch appendTaskReadyLatch = new CountDownLatch(1);
    final CountDownLatch compactionTaskStartLatch = new CountDownLatch(1);
    final Future<Pair<TaskStatus, DataSegmentsWithSchemas>> compactionFuture = exec.submit(
        () -> {
          compactionTaskStartLatch.await();
          return runTask(compactionTask, appendTaskReadyLatch, compactionTaskStartLatch);
        }
    );

    List<String> rows = new ArrayList<>();
    rows.add("2014-01-01T00:00:10Z,a1,11\n");
    rows.add("2014-01-01T00:00:10Z,b1,12\n");
    rows.add("2014-01-01T00:00:10Z,c1,13\n");
    rows.add("2014-01-01T01:00:20Z,a1,11\n");
    rows.add("2014-01-01T01:00:20Z,b1,12\n");
    rows.add("2014-01-01T01:00:20Z,c1,13\n");
    rows.add("2014-01-01T02:00:30Z,a1,11\n");
    rows.add("2014-01-01T02:00:30Z,b1,12\n");
    rows.add("2014-01-01T02:00:30Z,c1,13\n");
    final IndexTask appendTask = buildIndexTask(
        DEFAULT_PARSE_SPEC,
        rows,
        Intervals.of("2014-01-01T00:00:00Z/2014-01-01T03:00:00Z"),
        true
    );
    final Future<Pair<TaskStatus, DataSegmentsWithSchemas>> appendFuture = exec.submit(
        () -> runTask(appendTask, compactionTaskStartLatch, appendTaskReadyLatch)
    );

    verifyTaskSuccessRowsAndSchemaMatch(appendFuture.get(), 9);
    List<DataSegment> segments = new ArrayList<>(appendFuture.get().rhs.getSegments());
    Assert.assertEquals(6, segments.size());

    final Pair<TaskStatus, DataSegmentsWithSchemas> compactionResult = compactionFuture.get();
    verifyTaskSuccessRowsAndSchemaMatch(compactionResult, TOTAL_TEST_ROWS);
    Assert.assertEquals(1, compactionResult.rhs.getSegments().size());

    final Set<DataSegment> usedSegments = new HashSet<>(
        coordinatorClient.fetchUsedSegments(DATA_SOURCE, List.of(Intervals.of("2014-01-01/2014-01-02"))).get());
    Assert.assertEquals(7, usedSegments.size());
    final String version = Iterables.getOnlyElement(compactionResult.rhs.getSegments()).getVersion();
    Assert.assertTrue(usedSegments.stream().allMatch(segment -> segment.getVersion().equals(version)));

    CompactionTask finalTask = compactionTaskBuilder(segmentGranularity).interval(inputInterval, true).build();
    Pair<TaskStatus, DataSegmentsWithSchemas> finalResult = runTask(finalTask);
    verifyTaskSuccessRowsAndSchemaMatch(finalResult, 19);
  }

  @Test
  public void testIncrementalCompaction() throws Exception
  {
    Assume.assumeTrue(lockGranularity == LockGranularity.TIME_CHUNK);
    Assume.assumeTrue("Incremental compaction depends on concurrent lock", useConcurrentLocks);
    verifyTaskSuccessRowsAndSchemaMatch(runIndexTask(), TOTAL_TEST_ROWS);

    final CompactionTask compactionTask1 =
        compactionTaskBuilder(segmentGranularity).interval(inputInterval, true).build();

    final Pair<TaskStatus, DataSegmentsWithSchemas> resultPair1 = runTask(compactionTask1);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair1, TOTAL_TEST_ROWS);
    verifyCompactedSegment(List.copyOf(resultPair1.rhs.getSegments()), segmentGranularity, DEFAULT_QUERY_GRAN, false);
    Assert.assertEquals(1, resultPair1.rhs.getSegments().size());
    final DataSegment compactedSegment1 = Iterables.getOnlyElement(resultPair1.rhs.getSegments());

    Pair<TaskStatus, DataSegmentsWithSchemas> appendTask = runAppendTask();
    verifyTaskSuccessRowsAndSchemaMatch(appendTask, TOTAL_TEST_ROWS);

    List<SegmentDescriptor> uncompacted = appendTask.rhs.getSegments()
                                                        .stream()
                                                        .map(DataSegment::toDescriptor)
                                                        .collect(Collectors.toList());
    final CompactionTask compactionTask2 =
        compactionTaskBuilder(segmentGranularity)
            .inputSpec(new CompactionIntervalSpec(inputInterval, uncompacted, null), true)
            .build();
    final Pair<TaskStatus, DataSegmentsWithSchemas> resultPair2 = runTask(compactionTask2);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair2, TOTAL_TEST_ROWS);
    Assert.assertEquals(1, resultPair2.rhs.getSegments().size());
    final DataSegment compactedSegment2 = Iterables.getOnlyElement(resultPair2.rhs.getSegments());

    final List<String> usedSegments =
        coordinatorClient.fetchUsedSegments(DATA_SOURCE, List.of(Intervals.of("2014-01-01/2014-01-02")))
                         .get()
                         .stream()
                         .map(DataSegment::toString)
                         .collect(Collectors.toList());
    Assert.assertEquals(
        List.of(
            compactedSegment2.withShardSpec(new NumberedShardSpec(0, 2)).toString(),
            // shard spec in compactedSegment2 has been updated
            compactedSegment1.toBuilder()
                             .shardSpec(new NumberedShardSpec(1, 2))
                             .version(compactedSegment2.getVersion())
                             .build()
                             .toString() // compactedSegment1 has been upgraded with the new version & shardSpec
        ), usedSegments);
  }

  @Test
  public void testIncrementalCompactionRangePartition() throws Exception
  {
    List<String> rows = ImmutableList.of(
        "2014-01-01T00:00:10Z,a,1\n",
        "2014-01-01T00:00:10Z,b,2\n",
        "2014-01-01T00:00:10Z,c,3\n",
        "2014-01-01T01:00:20Z,a,1\n",
        "2014-01-01T01:00:20Z,b,2\n",
        "2014-01-01T01:00:20Z,c,3\n",
        "2014-01-01T02:00:30Z,a,1\n",
        "2014-01-01T02:00:30Z,b,2\n",
        "2014-01-01T02:00:30Z,c,3\n"
    );
    Assume.assumeTrue(lockGranularity == LockGranularity.TIME_CHUNK);
    Assume.assumeTrue("Incremental compaction depends on concurrent lock", useConcurrentLocks);
    verifyTaskSuccessRowsAndSchemaMatch(
        runTask(buildIndexTask(DEFAULT_PARSE_SPEC, rows, inputInterval, false)),
        9
    );

    PartitionsSpec rangePartitionSpec = new DimensionRangePartitionsSpec(null, 3, List.of("dim"), false);
    TuningConfig tuningConfig = TuningConfigBuilder.forCompactionTask()
                                                   .withMaxTotalRows(Long.MAX_VALUE)
                                                   .withPartitionsSpec(rangePartitionSpec)
                                                   .withForceGuaranteedRollup(true)
                                                   .build();
    final CompactionTask compactionTask1 =
        compactionTaskBuilder(segmentGranularity).interval(inputInterval, true).tuningConfig(tuningConfig).build();

    final Pair<TaskStatus, DataSegmentsWithSchemas> resultPair1 = runTask(compactionTask1);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair1, 9);
    Assert.assertEquals(3, resultPair1.rhs.getSegments().size());

    Pair<TaskStatus, DataSegmentsWithSchemas> appendTask =
        runTask(buildIndexTask(DEFAULT_PARSE_SPEC, rows, inputInterval, true));
    verifyTaskSuccessRowsAndSchemaMatch(appendTask, 9);

    List<SegmentDescriptor> uncompacted = appendTask.rhs.getSegments()
                                                        .stream()
                                                        .map(DataSegment::toDescriptor)
                                                        .collect(Collectors.toList());
    final CompactionTask compactionTask2 =
        compactionTaskBuilder(segmentGranularity)
            .inputSpec(new CompactionIntervalSpec(inputInterval, uncompacted, null), true)
            .tuningConfig(tuningConfig)
            .build();
    final Pair<TaskStatus, DataSegmentsWithSchemas> resultPair2 = runTask(compactionTask2);
    verifyTaskSuccessRowsAndSchemaMatch(resultPair2, 9);
    Assert.assertEquals(3, resultPair2.rhs.getSegments().size());

    final List<DataSegment> usedSegments =
        coordinatorClient.fetchUsedSegments(DATA_SOURCE, List.of(Intervals.of("2014-01-01/2014-01-02"))).get();
    Assert.assertEquals(6, usedSegments.size());
    final List<ShardSpec> shards = usedSegments.stream().map(DataSegment::getShardSpec).collect(Collectors.toList());
    Assert.assertEquals(Set.of("range"), shards.stream().map(ShardSpec::getType).collect(Collectors.toSet()));
  }

  @Test
  public void testIncrementalCompactionOverlappingInterval() throws Exception
  {
    Assume.assumeTrue(lockGranularity == LockGranularity.TIME_CHUNK);
    Assume.assumeTrue("Incremental compaction depends on concurrent lock", useConcurrentLocks);

    List<String> rows = new ArrayList<>();
    rows.add("2014-01-01T00:00:10Z,a1,11\n");
    rows.add("2014-01-01T00:00:10Z,b1,12\n");
    rows.add("2014-01-01T00:00:10Z,c1,13\n");
    rows.add("2014-01-01T06:00:20Z,a1,11\n");
    rows.add("2014-01-01T06:00:20Z,b1,12\n");
    rows.add("2014-01-01T06:00:20Z,c1,13\n");
    rows.add("2014-01-01T08:00:20Z,b1,12\n");
    rows.add("2014-01-01T08:00:20Z,c1,13\n");
    rows.add("2014-01-01T10:00:20Z,b1,12\n");
    rows.add("2014-01-01T10:00:20Z,c1,13\n");
    final IndexTask indexTask = buildIndexTask(
        Granularities.SIX_HOUR,
        DEFAULT_PARSE_SPEC,
        rows,
        TEST_INTERVAL_DAY,
        true
    );
    Pair<TaskStatus, DataSegmentsWithSchemas> indexTaskResult = runTask(indexTask);
    // created 2 segments in HOUR 0 -> HOUR 6, and 4 segments in HOUR 6 -> HOUR12
    Assert.assertEquals(6, indexTaskResult.rhs.getSegments().size());
    verifyTaskSuccessRowsAndSchemaMatch(indexTaskResult, 10);

    // First compaction task to only compact 6 segments from indexTask.
    final Interval compactionInterval = Intervals.of("2014-01-01T00:00:00Z/2014-01-01T08:00:00Z");
    final List<SegmentDescriptor> uncompactedFromIndexTask =
        indexTaskResult.rhs.getSegments()
                             .stream()
                             .filter(s -> compactionInterval.contains(s.getInterval()))
                             .map(DataSegment::toDescriptor)
                             .collect(Collectors.toList());

    final CompactionTask compactionTask1 =
        compactionTaskBuilder(Granularities.EIGHT_HOUR)
            .inputSpec(new CompactionIntervalSpec(compactionInterval, uncompactedFromIndexTask, null), true)
            .build();
    final Pair<TaskStatus, DataSegmentsWithSchemas> compactionResult1 = runTask(compactionTask1);
    verifyTaskSuccessRowsAndSchemaMatch(compactionResult1, 6);
    Assert.assertEquals(1, compactionResult1.rhs.getSegments().size());

    DataSegment segment = Iterables.getOnlyElement(compactionResult1.rhs.getSegments());
    Assert.assertEquals(compactionInterval, segment.getInterval());
    Assert.assertEquals(
        getDefaultCompactionState(Granularities.EIGHT_HOUR, DEFAULT_QUERY_GRAN, List.of(compactionInterval)),
        segment.getLastCompactionState()
    );
    Assert.assertEquals(new NumberedShardSpec(0, 1), segment.getShardSpec());

    // 1 compacted segment + 4 segments from initial index task in HOUR 6 -> HOUR 12
    Assert.assertEquals(5, coordinatorClient.fetchUsedSegments(DATA_SOURCE, List.of(TEST_INTERVAL_DAY)).get().size());
    //  Final compaction task, we should get 10 total rows.
    final CompactionTask compactionTask2 =
        compactionTaskBuilder(Granularities.DAY)
            .inputSpec(new CompactionIntervalSpec(TEST_INTERVAL_DAY, null, null), true)
            .build();
    final Pair<TaskStatus, DataSegmentsWithSchemas> compactionResult2 = runTask(compactionTask2);
    verifyTaskSuccessRowsAndSchemaMatch(compactionResult2, 10);
  }

  @Override
  protected CompactionState getDefaultCompactionState(
      Granularity segmentGranularity,
      Granularity queryGranularity,
      List<Interval> intervals,
      DimensionsSpec expectedDims,
      List<AggregatorFactory> expectedMetrics
  )
  {
    // Expected compaction state to exist after compaction as we store compaction state by default
    return new CompactionState(
        new DynamicPartitionsSpec(5000000, Long.MAX_VALUE),
        expectedDims.withDimensionExclusions(Set.of("__time"))
                    .withDimensionExclusions(expectedMetrics.stream()
                                                            .map(AggregatorFactory::getName)
                                                            .collect(Collectors.toSet())),
        expectedMetrics,
        null,
        IndexSpec.getDefault().getEffectiveSpec(),
        new UniformGranularitySpec(
            segmentGranularity,
            queryGranularity == null ? Granularities.MINUTE : queryGranularity,
            true,
            intervals
        ),
        null
    );
  }

  @Override
  protected CompactionTask.Builder compactionTaskBuilder(ClientCompactionTaskGranularitySpec granularitySpec)
  {
    return new CompactionTask.Builder(DATA_SOURCE, segmentCacheManagerFactory)
        .compactionRunner(getMSQCompactionRunner())
        .granularitySpec(granularitySpec)
        .context(Map.of(Tasks.USE_CONCURRENT_LOCKS, useConcurrentLocks, Tasks.STORE_COMPACTION_STATE_KEY, true));
  }
}

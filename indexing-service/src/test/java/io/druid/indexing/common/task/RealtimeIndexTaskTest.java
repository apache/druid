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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.MapCache;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.discovery.DataNodeService;
import io.druid.discovery.DruidNodeAnnouncer;
import io.druid.discovery.LookupNodeService;
import io.druid.indexer.TaskState;
import io.druid.indexing.common.SegmentLoaderFactory;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.LocalTaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.overlord.HeapMemoryTaskStorage;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.supervisor.SupervisorManager;
import io.druid.indexing.test.TestDataSegmentAnnouncer;
import io.druid.indexing.test.TestDataSegmentKiller;
import io.druid.indexing.test.TestDataSegmentPusher;
import io.druid.indexing.test.TestIndexerMetadataStorageCoordinator;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.jackson.JacksonUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.core.NoopEmitter;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.metrics.MonitorScheduler;
import io.druid.math.expr.ExprMacroTable;
import io.druid.metadata.EntryExistsException;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.Druids;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.TestHelper;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoaderLocalCacheManager;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.segment.realtime.plumber.ServerTimeRejectionPolicyFactory;
import io.druid.segment.transform.ExpressionTransform;
import io.druid.segment.transform.TransformSpec;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DataSegmentServerAnnouncer;
import io.druid.server.coordination.ServerType;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public class RealtimeIndexTaskTest
{
  private static final Logger log = new Logger(RealtimeIndexTaskTest.class);
  private static final ServiceEmitter emitter = new ServiceEmitter(
      "service",
      "host",
      new NoopEmitter()
  );

  private static final String FAIL_DIM = "__fail__";

  private static class TestFirehose implements Firehose
  {
    private final InputRowParser<Map<String, Object>> parser;
    private final List<Map<String, Object>> queue = new LinkedList<>();
    private boolean closed = false;

    public TestFirehose(final InputRowParser<Map<String, Object>> parser)
    {
      this.parser = parser;
    }

    public void addRows(List<Map<String, Object>> rows)
    {
      synchronized (this) {
        queue.addAll(rows);
        notifyAll();
      }
    }

    @Override
    public boolean hasMore()
    {
      try {
        synchronized (this) {
          while (queue.isEmpty() && !closed) {
            wait();
          }
          return !queue.isEmpty();
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw Throwables.propagate(e);
      }
    }

    @Override
    public InputRow nextRow()
    {
      synchronized (this) {
        final InputRow row = parser.parseBatch(queue.remove(0)).get(0);
        if (row != null && row.getRaw(FAIL_DIM) != null) {
          throw new ParseException(FAIL_DIM);
        }
        return row;
      }
    }

    @Override
    public Runnable commit()
    {
      return () -> {};
    }

    @Override
    public void close()
    {
      synchronized (this) {
        closed = true;
        notifyAll();
      }
    }
  }

  private static class TestFirehoseFactory implements FirehoseFactory<InputRowParser>
  {
    public TestFirehoseFactory()
    {
    }

    @Override
    @SuppressWarnings("unchecked")
    public Firehose connect(InputRowParser parser, File temporaryDirectory) throws ParseException
    {
      return new TestFirehose(parser);
    }
  }

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private DateTime now;
  private ListeningExecutorService taskExec;
  private Map<SegmentDescriptor, Pair<Executor, Runnable>> handOffCallbacks;

  @Before
  public void setUp()
  {
    EmittingLogger.registerEmitter(emitter);
    emitter.start();
    taskExec = MoreExecutors.listeningDecorator(Execs.singleThreaded("realtime-index-task-test-%d"));
    now = DateTimes.nowUtc();
  }

  @After
  public void tearDown()
  {
    taskExec.shutdownNow();
  }

  @Test
  public void testMakeTaskId()
  {
    Assert.assertEquals(
        "index_realtime_test_0_2015-01-02T00:00:00.000Z_abcdefgh",
        RealtimeIndexTask.makeTaskId("test", 0, DateTimes.of("2015-01-02"), 0x76543210)
    );
  }

  @Test(timeout = 60_000L)
  public void testDefaultResource()
  {
    final RealtimeIndexTask task = makeRealtimeTask(null);
    Assert.assertEquals(task.getId(), task.getTaskResource().getAvailabilityGroup());
  }


  @Test(timeout = 60_000L, expected = ExecutionException.class)
  public void testHandoffTimeout() throws Exception
  {
    final TestIndexerMetadataStorageCoordinator mdc = new TestIndexerMetadataStorageCoordinator();
    final RealtimeIndexTask task = makeRealtimeTask(null, TransformSpec.NONE, true, 100L);
    final TaskToolbox taskToolbox = makeToolbox(task, mdc, tempFolder.newFolder());
    final ListenableFuture<TaskStatus> statusFuture = runTask(task, taskToolbox);

    // Wait for firehose to show up, it starts off null.
    while (task.getFirehose() == null) {
      Thread.sleep(50);
    }

    final TestFirehose firehose = (TestFirehose) task.getFirehose();

    firehose.addRows(
        ImmutableList.of(
            ImmutableMap.of("t", now.getMillis(), "dim1", "foo", "met1", "1")
        )
    );

    // Stop the firehose, this will drain out existing events.
    firehose.close();

    // Wait for publish.
    while (mdc.getPublished().isEmpty()) {
      Thread.sleep(50);
    }

    Assert.assertEquals(1, task.getMetrics().processed());
    Assert.assertNotNull(Iterables.getOnlyElement(mdc.getPublished()));


    // handoff would timeout, resulting in exception
    statusFuture.get();
  }

  @Test(timeout = 60_000L)
  public void testBasics() throws Exception
  {
    final TestIndexerMetadataStorageCoordinator mdc = new TestIndexerMetadataStorageCoordinator();
    final RealtimeIndexTask task = makeRealtimeTask(null);
    final TaskToolbox taskToolbox = makeToolbox(task, mdc, tempFolder.newFolder());
    final ListenableFuture<TaskStatus> statusFuture = runTask(task, taskToolbox);
    final DataSegment publishedSegment;

    // Wait for firehose to show up, it starts off null.
    while (task.getFirehose() == null) {
      Thread.sleep(50);
    }

    final TestFirehose firehose = (TestFirehose) task.getFirehose();

    firehose.addRows(
        ImmutableList.of(
            ImmutableMap.of("t", now.getMillis(), "dim1", "foo", "met1", "1"),
            ImmutableMap.of("t", now.minus(new Period("P1D")).getMillis(), "dim1", "foo", "met1", 2.0),
            ImmutableMap.of("t", now.getMillis(), "dim2", "bar", "met1", 2.0)
        )
    );

    // Stop the firehose, this will drain out existing events.
    firehose.close();

    // Wait for publish.
    while (mdc.getPublished().isEmpty()) {
      Thread.sleep(50);
    }

    publishedSegment = Iterables.getOnlyElement(mdc.getPublished());

    // Check metrics.
    Assert.assertEquals(2, task.getMetrics().processed());
    Assert.assertEquals(1, task.getMetrics().thrownAway());
    Assert.assertEquals(0, task.getMetrics().unparseable());

    // Do some queries.
    Assert.assertEquals(2, sumMetric(task, null, "rows"));
    Assert.assertEquals(3, sumMetric(task, null, "met1"));

    // Simulate handoff.
    for (Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>> entry : handOffCallbacks.entrySet()) {
      final Pair<Executor, Runnable> executorRunnablePair = entry.getValue();
      Assert.assertEquals(
          new SegmentDescriptor(
              publishedSegment.getInterval(),
              publishedSegment.getVersion(),
              publishedSegment.getShardSpec().getPartitionNum()
          ),
          entry.getKey()
      );
      executorRunnablePair.lhs.execute(executorRunnablePair.rhs);
    }
    handOffCallbacks.clear();

    // Wait for the task to finish.
    final TaskStatus taskStatus = statusFuture.get();
    Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());
  }

  @Test(timeout = 60_000L)
  public void testTransformSpec() throws Exception
  {
    final TestIndexerMetadataStorageCoordinator mdc = new TestIndexerMetadataStorageCoordinator();
    final TransformSpec transformSpec = new TransformSpec(
        new SelectorDimFilter("dim1", "foo", null),
        ImmutableList.of(
            new ExpressionTransform("dim1t", "concat(dim1,dim1)", ExprMacroTable.nil())
        )
    );
    final RealtimeIndexTask task = makeRealtimeTask(null, transformSpec, true, 0);
    final TaskToolbox taskToolbox = makeToolbox(task, mdc, tempFolder.newFolder());
    final ListenableFuture<TaskStatus> statusFuture = runTask(task, taskToolbox);
    final DataSegment publishedSegment;

    // Wait for firehose to show up, it starts off null.
    while (task.getFirehose() == null) {
      Thread.sleep(50);
    }

    final TestFirehose firehose = (TestFirehose) task.getFirehose();

    firehose.addRows(
        ImmutableList.of(
            ImmutableMap.of("t", now.getMillis(), "dim1", "foo", "met1", "1"),
            ImmutableMap.of("t", now.minus(new Period("P1D")).getMillis(), "dim1", "foo", "met1", 2.0),
            ImmutableMap.of("t", now.getMillis(), "dim2", "bar", "met1", 2.0)
        )
    );

    // Stop the firehose, this will drain out existing events.
    firehose.close();

    // Wait for publish.
    while (mdc.getPublished().isEmpty()) {
      Thread.sleep(50);
    }

    publishedSegment = Iterables.getOnlyElement(mdc.getPublished());

    // Check metrics.
    Assert.assertEquals(1, task.getMetrics().processed());
    Assert.assertEquals(2, task.getMetrics().thrownAway());
    Assert.assertEquals(0, task.getMetrics().unparseable());

    // Do some queries.
    Assert.assertEquals(1, sumMetric(task, null, "rows"));
    Assert.assertEquals(1, sumMetric(task, new SelectorDimFilter("dim1t", "foofoo", null), "rows"));
    Assert.assertEquals(0, sumMetric(task, new SelectorDimFilter("dim1t", "barbar", null), "rows"));
    Assert.assertEquals(1, sumMetric(task, null, "met1"));

    // Simulate handoff.
    for (Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>> entry : handOffCallbacks.entrySet()) {
      final Pair<Executor, Runnable> executorRunnablePair = entry.getValue();
      Assert.assertEquals(
          new SegmentDescriptor(
              publishedSegment.getInterval(),
              publishedSegment.getVersion(),
              publishedSegment.getShardSpec().getPartitionNum()
          ),
          entry.getKey()
      );
      executorRunnablePair.lhs.execute(executorRunnablePair.rhs);
    }
    handOffCallbacks.clear();

    // Wait for the task to finish.
    final TaskStatus taskStatus = statusFuture.get();
    Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());
  }

  @Test(timeout = 60_000L)
  public void testReportParseExceptionsOnBadMetric() throws Exception
  {
    final TestIndexerMetadataStorageCoordinator mdc = new TestIndexerMetadataStorageCoordinator();
    final RealtimeIndexTask task = makeRealtimeTask(null, true);
    final TaskToolbox taskToolbox = makeToolbox(task, mdc, tempFolder.newFolder());
    final ListenableFuture<TaskStatus> statusFuture = runTask(task, taskToolbox);

    // Wait for firehose to show up, it starts off null.
    while (task.getFirehose() == null) {
      Thread.sleep(50);
    }

    final TestFirehose firehose = (TestFirehose) task.getFirehose();

    firehose.addRows(
        ImmutableList.of(
            ImmutableMap.of("t", now.getMillis(), "dim1", "foo", "met1", "1"),
            ImmutableMap.of("t", now.getMillis(), "dim1", "foo", "met1", "foo"),
            ImmutableMap.of("t", now.minus(new Period("P1D")).getMillis(), "dim1", "foo", "met1", "foo"),
            ImmutableMap.of("t", now.getMillis(), "dim2", "bar", "met1", 2.0)
        )
    );

    // Stop the firehose, this will drain out existing events.
    firehose.close();

    // Wait for the task to finish.
    expectedException.expect(ExecutionException.class);
    expectedException.expectCause(CoreMatchers.<Throwable>instanceOf(ParseException.class));
    expectedException.expectCause(
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.containsString("[Unable to parse value[foo] for field[met1]")
        )
    );

    statusFuture.get();
  }

  @Test(timeout = 60_000L)
  public void testNoReportParseExceptions() throws Exception
  {
    final TestIndexerMetadataStorageCoordinator mdc = new TestIndexerMetadataStorageCoordinator();
    final RealtimeIndexTask task = makeRealtimeTask(null, false);
    final TaskToolbox taskToolbox = makeToolbox(task, mdc, tempFolder.newFolder());
    final ListenableFuture<TaskStatus> statusFuture = runTask(task, taskToolbox);
    final DataSegment publishedSegment;

    // Wait for firehose to show up, it starts off null.
    while (task.getFirehose() == null) {
      Thread.sleep(50);
    }

    final TestFirehose firehose = (TestFirehose) task.getFirehose();

    firehose.addRows(
        Arrays.asList(
            // Good row- will be processed.
            ImmutableMap.of("t", now.getMillis(), "dim1", "foo", "met1", "1"),

            // Null row- will be thrown away.
            null,

            // Bad metric- will count as processed, but that particular metric won't update.
            ImmutableMap.of("t", now.getMillis(), "dim1", "foo", "met1", "foo"),

            // Bad row- will be unparseable.
            ImmutableMap.of("dim1", "foo", "met1", 2.0, FAIL_DIM, "x"),

            // Old row- will be thrownAway.
            ImmutableMap.of("t", now.minus(Period.days(1)).getMillis(), "dim1", "foo", "met1", 2.0),

            // Good row- will be processed.
            ImmutableMap.of("t", now.getMillis(), "dim2", "bar", "met1", 2.0)
        )
    );

    // Stop the firehose, this will drain out existing events.
    firehose.close();

    // Wait for publish.
    while (mdc.getPublished().isEmpty()) {
      Thread.sleep(50);
    }

    publishedSegment = Iterables.getOnlyElement(mdc.getPublished());

    // Check metrics.
    Assert.assertEquals(3, task.getMetrics().processed());
    Assert.assertEquals(1, task.getMetrics().thrownAway());
    Assert.assertEquals(2, task.getMetrics().unparseable());

    // Do some queries.
    Assert.assertEquals(3, sumMetric(task, null, "rows"));
    Assert.assertEquals(3, sumMetric(task, null, "met1"));

    // Simulate handoff.
    for (Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>> entry : handOffCallbacks.entrySet()) {
      final Pair<Executor, Runnable> executorRunnablePair = entry.getValue();
      Assert.assertEquals(
          new SegmentDescriptor(
              publishedSegment.getInterval(),
              publishedSegment.getVersion(),
              publishedSegment.getShardSpec().getPartitionNum()
          ),
          entry.getKey()
      );
      executorRunnablePair.lhs.execute(executorRunnablePair.rhs);
    }
    handOffCallbacks.clear();

    // Wait for the task to finish.
    final TaskStatus taskStatus = statusFuture.get();
    Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());
  }

  @Test(timeout = 60_000L)
  public void testRestore() throws Exception
  {
    final File directory = tempFolder.newFolder();
    final RealtimeIndexTask task1 = makeRealtimeTask(null);
    final DataSegment publishedSegment;

    // First run:
    {
      final TestIndexerMetadataStorageCoordinator mdc = new TestIndexerMetadataStorageCoordinator();
      final TaskToolbox taskToolbox = makeToolbox(task1, mdc, directory);
      final ListenableFuture<TaskStatus> statusFuture = runTask(task1, taskToolbox);

      // Wait for firehose to show up, it starts off null.
      while (task1.getFirehose() == null) {
        Thread.sleep(50);
      }

      final TestFirehose firehose = (TestFirehose) task1.getFirehose();

      firehose.addRows(
          ImmutableList.of(
              ImmutableMap.of("t", now.getMillis(), "dim1", "foo")
          )
      );

      // Trigger graceful shutdown.
      task1.stopGracefully();

      // Wait for the task to finish. The status doesn't really matter, but we'll check it anyway.
      final TaskStatus taskStatus = statusFuture.get();
      Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());

      // Nothing should be published.
      Assert.assertEquals(Sets.newHashSet(), mdc.getPublished());
    }

    // Second run:
    {
      final TestIndexerMetadataStorageCoordinator mdc = new TestIndexerMetadataStorageCoordinator();
      final RealtimeIndexTask task2 = makeRealtimeTask(task1.getId());
      final TaskToolbox taskToolbox = makeToolbox(task2, mdc, directory);
      final ListenableFuture<TaskStatus> statusFuture = runTask(task2, taskToolbox);

      // Wait for firehose to show up, it starts off null.
      while (task2.getFirehose() == null) {
        Thread.sleep(50);
      }

      // Do a query, at this point the previous data should be loaded.
      Assert.assertEquals(1, sumMetric(task2, null, "rows"));

      final TestFirehose firehose = (TestFirehose) task2.getFirehose();

      firehose.addRows(
          ImmutableList.of(
              ImmutableMap.of("t", now.getMillis(), "dim2", "bar")
          )
      );

      // Stop the firehose, this will drain out existing events.
      firehose.close();

      // Wait for publish.
      while (mdc.getPublished().isEmpty()) {
        Thread.sleep(50);
      }

      publishedSegment = Iterables.getOnlyElement(mdc.getPublished());

      // Do a query.
      Assert.assertEquals(2, sumMetric(task2, null, "rows"));

      // Simulate handoff.
      for (Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>> entry : handOffCallbacks.entrySet()) {
        final Pair<Executor, Runnable> executorRunnablePair = entry.getValue();
        Assert.assertEquals(
            new SegmentDescriptor(
                publishedSegment.getInterval(),
                publishedSegment.getVersion(),
                publishedSegment.getShardSpec().getPartitionNum()
            ),
            entry.getKey()
        );
        executorRunnablePair.lhs.execute(executorRunnablePair.rhs);
      }
      handOffCallbacks.clear();

      // Wait for the task to finish.
      final TaskStatus taskStatus = statusFuture.get();
      Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());
    }
  }

  @Test(timeout = 60_000L)
  public void testRestoreAfterHandoffAttemptDuringShutdown() throws Exception
  {
    final TaskStorage taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(null));
    final TestIndexerMetadataStorageCoordinator mdc = new TestIndexerMetadataStorageCoordinator();
    final File directory = tempFolder.newFolder();
    final RealtimeIndexTask task1 = makeRealtimeTask(null);
    final DataSegment publishedSegment;

    // First run:
    {
      final TaskToolbox taskToolbox = makeToolbox(task1, taskStorage, mdc, directory);
      final ListenableFuture<TaskStatus> statusFuture = runTask(task1, taskToolbox);

      // Wait for firehose to show up, it starts off null.
      while (task1.getFirehose() == null) {
        Thread.sleep(50);
      }

      final TestFirehose firehose = (TestFirehose) task1.getFirehose();

      firehose.addRows(
          ImmutableList.of(
              ImmutableMap.of("t", now.getMillis(), "dim1", "foo")
          )
      );

      // Stop the firehose, this will trigger a finishJob.
      firehose.close();

      // Wait for publish.
      while (mdc.getPublished().isEmpty()) {
        Thread.sleep(50);
      }

      publishedSegment = Iterables.getOnlyElement(mdc.getPublished());

      // Do a query.
      Assert.assertEquals(1, sumMetric(task1, null, "rows"));

      // Trigger graceful shutdown.
      task1.stopGracefully();

      // Wait for the task to finish. The status doesn't really matter.
      while (!statusFuture.isDone()) {
        Thread.sleep(50);
      }
    }

    // Second run:
    {
      final RealtimeIndexTask task2 = makeRealtimeTask(task1.getId());
      final TaskToolbox taskToolbox = makeToolbox(task2, taskStorage, mdc, directory);
      final ListenableFuture<TaskStatus> statusFuture = runTask(task2, taskToolbox);

      // Wait for firehose to show up, it starts off null.
      while (task2.getFirehose() == null) {
        Thread.sleep(50);
      }

      // Stop the firehose again, this will start another handoff.
      final TestFirehose firehose = (TestFirehose) task2.getFirehose();

      // Stop the firehose, this will trigger a finishJob.
      firehose.close();

      // publishedSegment is still published. No reason it shouldn't be.
      Assert.assertEquals(ImmutableSet.of(publishedSegment), mdc.getPublished());

      // Wait for a handoffCallback to show up.
      while (handOffCallbacks.isEmpty()) {
        Thread.sleep(50);
      }

      // Simulate handoff.
      for (Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>> entry : handOffCallbacks.entrySet()) {
        final Pair<Executor, Runnable> executorRunnablePair = entry.getValue();
        Assert.assertEquals(
            new SegmentDescriptor(
                publishedSegment.getInterval(),
                publishedSegment.getVersion(),
                publishedSegment.getShardSpec().getPartitionNum()
            ),
            entry.getKey()
        );
        executorRunnablePair.lhs.execute(executorRunnablePair.rhs);
      }
      handOffCallbacks.clear();

      // Wait for the task to finish.
      final TaskStatus taskStatus = statusFuture.get();
      Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());
    }
  }

  @Test(timeout = 60_000L)
  public void testRestoreCorruptData() throws Exception
  {
    final File directory = tempFolder.newFolder();
    final RealtimeIndexTask task1 = makeRealtimeTask(null);

    // First run:
    {
      final TestIndexerMetadataStorageCoordinator mdc = new TestIndexerMetadataStorageCoordinator();
      final TaskToolbox taskToolbox = makeToolbox(task1, mdc, directory);
      final ListenableFuture<TaskStatus> statusFuture = runTask(task1, taskToolbox);

      // Wait for firehose to show up, it starts off null.
      while (task1.getFirehose() == null) {
        Thread.sleep(50);
      }

      final TestFirehose firehose = (TestFirehose) task1.getFirehose();

      firehose.addRows(
          ImmutableList.of(
              ImmutableMap.of("t", now.getMillis(), "dim1", "foo")
          )
      );

      // Trigger graceful shutdown.
      task1.stopGracefully();

      // Wait for the task to finish. The status doesn't really matter, but we'll check it anyway.
      final TaskStatus taskStatus = statusFuture.get();
      Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());

      // Nothing should be published.
      Assert.assertEquals(Sets.newHashSet(), mdc.getPublished());
    }

    // Corrupt the data:
    final File smooshFile = new File(
        StringUtils.format(
            "%s/persistent/task/%s/work/persist/%s/%s_%s/0/00000.smoosh",
            directory,
            task1.getId(),
            task1.getDataSource(),
            Granularities.DAY.bucketStart(now),
            Granularities.DAY.bucketEnd(now)
        )
    );

    Files.write(smooshFile.toPath(), StringUtils.toUtf8("oops!"));

    // Second run:
    {
      final TestIndexerMetadataStorageCoordinator mdc = new TestIndexerMetadataStorageCoordinator();
      final RealtimeIndexTask task2 = makeRealtimeTask(task1.getId());
      final TaskToolbox taskToolbox = makeToolbox(task2, mdc, directory);
      final ListenableFuture<TaskStatus> statusFuture = runTask(task2, taskToolbox);

      // Wait for the task to finish.
      boolean caught = false;
      try {
        statusFuture.get();
      }
      catch (Exception e) {
        caught = true;
      }
      Assert.assertTrue("expected exception", caught);
    }
  }

  @Test(timeout = 60_000L)
  public void testStopBeforeStarting() throws Exception
  {
    final File directory = tempFolder.newFolder();
    final RealtimeIndexTask task1 = makeRealtimeTask(null);

    task1.stopGracefully();
    final TestIndexerMetadataStorageCoordinator mdc = new TestIndexerMetadataStorageCoordinator();
    final TaskToolbox taskToolbox = makeToolbox(task1, mdc, directory);
    final ListenableFuture<TaskStatus> statusFuture = runTask(task1, taskToolbox);

    // Wait for the task to finish.
    final TaskStatus taskStatus = statusFuture.get();
    Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());
  }

  private ListenableFuture<TaskStatus> runTask(final Task task, final TaskToolbox toolbox)
  {
    return taskExec.submit(
        new Callable<TaskStatus>()
        {
          @Override
          public TaskStatus call() throws Exception
          {
            try {
              if (task.isReady(toolbox.getTaskActionClient())) {
                return task.run(toolbox);
              } else {
                throw new ISE("Task is not ready");
              }
            }
            catch (Exception e) {
              log.warn(e, "Task failed");
              throw e;
            }
          }
        }
    );
  }

  private RealtimeIndexTask makeRealtimeTask(final String taskId)
  {
    return makeRealtimeTask(taskId, TransformSpec.NONE, true, 0);
  }

  private RealtimeIndexTask makeRealtimeTask(final String taskId, boolean reportParseExceptions)
  {
    return makeRealtimeTask(taskId, TransformSpec.NONE, reportParseExceptions, 0);
  }

  private RealtimeIndexTask makeRealtimeTask(
      final String taskId,
      final TransformSpec transformSpec,
      final boolean reportParseExceptions,
      final long handoffTimeout
  )
  {
    ObjectMapper objectMapper = new DefaultObjectMapper();
    DataSchema dataSchema = new DataSchema(
        "test_ds",
        TestHelper.makeJsonMapper().convertValue(
            new MapInputRowParser(
                new TimeAndDimsParseSpec(
                    new TimestampSpec("t", "auto", null),
                    new DimensionsSpec(
                        DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2", "dim1t")),
                        null,
                        null
                    )
                )
            ),
            JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
        ),
        new AggregatorFactory[]{new CountAggregatorFactory("rows"), new LongSumAggregatorFactory("met1", "met1")},
        new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
        transformSpec,
        objectMapper
    );
    RealtimeIOConfig realtimeIOConfig = new RealtimeIOConfig(
        new TestFirehoseFactory(),
        null,
        null
    );
    RealtimeTuningConfig realtimeTuningConfig = new RealtimeTuningConfig(
        1000,
        null,
        new Period("P1Y"),
        new Period("PT10M"),
        null,
        null,
        new ServerTimeRejectionPolicyFactory(),
        null,
        null,
        null,
        true,
        0,
        0,
        reportParseExceptions,
        handoffTimeout,
        null,
        null,
        null
    );
    return new RealtimeIndexTask(
        taskId,
        null,
        new FireDepartment(dataSchema, realtimeIOConfig, realtimeTuningConfig),
        null
    )
    {
      @Override
      protected boolean isFirehoseDrainableByClosing(FirehoseFactory firehoseFactory)
      {
        return true;
      }
    };
  }

  private TaskToolbox makeToolbox(
      final Task task,
      final IndexerMetadataStorageCoordinator mdc,
      final File directory
  )
  {
    return makeToolbox(
        task,
        new HeapMemoryTaskStorage(new TaskStorageConfig(null)),
        mdc,
        directory
    );
  }

  private TaskToolbox makeToolbox(
      final Task task,
      final TaskStorage taskStorage,
      final IndexerMetadataStorageCoordinator mdc,
      final File directory
  )
  {
    final TaskConfig taskConfig = new TaskConfig(directory.getPath(), null, null, 50000, null, false, null, null);
    final TaskLockbox taskLockbox = new TaskLockbox(taskStorage);
    try {
      taskStorage.insert(task, TaskStatus.running(task.getId()));
    }
    catch (EntryExistsException e) {
      // suppress
    }
    taskLockbox.syncFromStorage();
    final TaskActionToolbox taskActionToolbox = new TaskActionToolbox(
        taskLockbox,
        mdc,
        emitter,
        EasyMock.createMock(SupervisorManager.class)
    );
    final TaskActionClientFactory taskActionClientFactory = new LocalTaskActionClientFactory(
        taskStorage,
        taskActionToolbox
    );
    IntervalChunkingQueryRunnerDecorator queryRunnerDecorator = new IntervalChunkingQueryRunnerDecorator(
        null,
        null,
        null
    )
    {
      @Override
      public <T> QueryRunner<T> decorate(
          QueryRunner<T> delegate, QueryToolChest<T, ? extends Query<T>> toolChest
      )
      {
        return delegate;
      }
    };
    final QueryRunnerFactoryConglomerate conglomerate = new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>of(
            TimeseriesQuery.class,
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(queryRunnerDecorator),
                new TimeseriesQueryEngine(),
                new QueryWatcher()
                {
                  @Override
                  public void registerQuery(Query query, ListenableFuture future)
                  {
                    // do nothing
                  }
                }
            )
        )
    );
    handOffCallbacks = new ConcurrentHashMap<>();
    final SegmentHandoffNotifierFactory handoffNotifierFactory = new SegmentHandoffNotifierFactory()
    {
      @Override
      public SegmentHandoffNotifier createSegmentHandoffNotifier(String dataSource)
      {
        return new SegmentHandoffNotifier()
        {
          @Override
          public boolean registerSegmentHandoffCallback(
              SegmentDescriptor descriptor, Executor exec, Runnable handOffRunnable
          )
          {
            handOffCallbacks.put(descriptor, new Pair<>(exec, handOffRunnable));
            return true;
          }

          @Override
          public void start()
          {
            //Noop
          }

          @Override
          public void close()
          {
            //Noop
          }

        };
      }
    };
    final TestUtils testUtils = new TestUtils();
    SegmentLoaderConfig segmentLoaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return Lists.newArrayList();
      }
    };
    final TaskToolboxFactory toolboxFactory = new TaskToolboxFactory(
        taskConfig,
        taskActionClientFactory,
        emitter,
        new TestDataSegmentPusher(),
        new TestDataSegmentKiller(),
        null, // DataSegmentMover
        null, // DataSegmentArchiver
        new TestDataSegmentAnnouncer(),
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
        handoffNotifierFactory,
        () -> conglomerate,
        MoreExecutors.sameThreadExecutor(), // queryExecutorService
        EasyMock.createMock(MonitorScheduler.class),
        new SegmentLoaderFactory(
            new SegmentLoaderLocalCacheManager(null, segmentLoaderConfig, testUtils.getTestObjectMapper())
        ),
        testUtils.getTestObjectMapper(),
        testUtils.getTestIndexIO(),
        MapCache.create(1024),
        new CacheConfig(),
        testUtils.getTestIndexMergerV9(),
        EasyMock.createNiceMock(DruidNodeAnnouncer.class),
        EasyMock.createNiceMock(DruidNode.class),
        new LookupNodeService("tier"),
        new DataNodeService("tier", 1000, ServerType.INDEXER_EXECUTOR, 0),
        new NoopTestTaskFileWriter()
    );

    return toolboxFactory.build(task);
  }

  public long sumMetric(final Task task, final DimFilter filter, final String metric)
  {
    // Do a query.
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_ds")
                                  .filters(filter)
                                  .aggregators(
                                      ImmutableList.<AggregatorFactory>of(
                                          new LongSumAggregatorFactory(metric, metric)
                                      )
                                  ).granularity(Granularities.ALL)
                                  .intervals("2000/3000")
                                  .build();

    List<Result<TimeseriesResultValue>> results =
        task.getQueryRunner(query).run(QueryPlus.wrap(query), ImmutableMap.of()).toList();
    return results.isEmpty() ? 0 : results.get(0).getValue().getLongMetric(metric);
  }
}

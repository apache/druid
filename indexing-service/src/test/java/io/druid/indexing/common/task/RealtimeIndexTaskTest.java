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
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.LoggingEmitter;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.metrics.MonitorScheduler;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.MapCache;
import io.druid.concurrent.Execs;
import io.druid.data.input.*;
import io.druid.data.input.impl.InputRowParser;
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
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.metadata.EntryExistsException;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.Druids;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
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
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoaderLocalCacheManager;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.firehose.FirehoseV2CloseBeforeStartException;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.segment.realtime.plumber.ServerTimeRejectionPolicyFactory;
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
import org.junit.internal.matchers.ThrowableCauseMatcher;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

@RunWith(Parameterized.class)
public class RealtimeIndexTaskTest
{
  private static final Logger log = new Logger(RealtimeIndexTaskTest.class);
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private static final ServiceEmitter emitter = new ServiceEmitter(
      "service",
      "host",
      new LoggingEmitter(
          log,
          LoggingEmitter.Level.ERROR,
          jsonMapper
      )
  );

  private static final String FAIL_DIM = "__fail__";

  private static class TestFirehose implements Firehose
  {
    private final List<InputRow> queue = Lists.newLinkedList();
    private boolean closed = false;

    public void addRows(List<InputRow> rows)
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
        final InputRow row = queue.remove(0);
        if (row != null && row.getDimensions().contains(FAIL_DIM)) {
          throw new ParseException(FAIL_DIM);
        }
        return row;
      }
    }

    @Override
    public Runnable commit()
    {
      return new Runnable()
      {
        @Override
        public void run()
        {
          // do nothing
        }
      };
    }

    @Override
    public void close() throws IOException
    {
      synchronized (this) {
        closed = true;
        notifyAll();
      }
    }
  }

  private static class TestFirehoseV2 implements FirehoseV2
  {
    private final List<InputRow> queue = Lists.newLinkedList();
    private boolean closed = false;
    private InputRow currRow;

    public void addRows(List<InputRow> rows)
    {
      synchronized (this) {
        queue.addAll(rows);
        notifyAll();
      }
    }

    @Override
    public InputRow currRow()
    {
      if (currRow != null && currRow.getDimensions().contains(FAIL_DIM)) {
        throw new ParseException(FAIL_DIM);
      }
      return currRow;
    }

    @Override
    public boolean advance()
    {
      try {
        synchronized (this) {
          while (queue.isEmpty() && !closed) {
            wait();
          }
          if (!queue.isEmpty()) {
            currRow = queue.remove(0);
            return true;
          } else {
            currRow = null;
            return false;
          }
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw Throwables.propagate(e);
      }
    }

    @Override
    public Committer makeCommitter()
    {
      return new Committer()
      {
        @Override
        public Object getMetadata()
        {
          return null;
        }

        @Override
        public void run()
        {
        }
      };
    }

    @Override
    public void start() throws Exception
    {
      if (!advance())
      {
        throw new FirehoseV2CloseBeforeStartException("stopped before start");
      }
    }

    @Override
    public void close() throws IOException
    {
      synchronized (this) {
        closed = true;
        notifyAll();
      }
    }
  }

  private static class TestFirehoseFactory implements FirehoseFactory
  {
    public TestFirehoseFactory()
    {
    }

    @Override
    public Firehose connect(InputRowParser parser) throws IOException, ParseException
    {
      return new TestFirehose();
    }
  }

  private static class TestFirehoseFactoryV2 implements FirehoseFactoryV2
  {
    public TestFirehoseFactoryV2()
    {
    }

    @Override
    public FirehoseV2 connect(InputRowParser inputRowParser, Object o) throws IOException, ParseException
    {
      return new TestFirehoseV2();
    }
  }

  private static class TestTaskFirehoseHolder
  {
    private TestFirehose firehose;
    private TestFirehoseV2 firehoseV2;
    final boolean isV2;
    final RealtimeIndexTask task;

    public TestTaskFirehoseHolder(
        RealtimeIndexTask task
    )
    {
      FirehoseFactory firehoseFactory = task.getRealtimeIngestionSchema().getIOConfig().getFirehoseFactory();
      FirehoseFactoryV2 firehoseFactoryV2 = task.getRealtimeIngestionSchema().getIOConfig().getFirehoseFactoryV2();

      Preconditions.checkArgument((firehoseFactory == null) ^ (firehoseFactoryV2 == null),
          "either firehoseFactory or firehoseFactoryV2 specified, not both");
      isV2 = firehoseFactoryV2 != null;
      this.task = task;
    }

    public void addRows(List<InputRow> rows)
    {
      if(isV2) {
        Preconditions.checkArgument(firehoseV2 != null, "should use after firehose is connected");
        firehoseV2.addRows(rows);
      } else {
        Preconditions.checkArgument(firehose != null, "should use after firehose is connected");
        firehose.addRows(rows);
      }
    }

    public Object getFirehose()
    {
      if (isV2) {
        firehoseV2 = (TestFirehoseV2)task.getFirehoseV2();
        return firehoseV2;
      } else {
        firehose = (TestFirehose)task.getFirehose();
        return firehose;
      }
    }

    public void close() throws IOException
    {
      if(isV2) {
        firehoseV2.close();
      } else {
        firehose.close();
      }
    }
  }

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final boolean buildV9Directly;
  private final FirehoseFactory firehoseFactory;
  private final FirehoseFactoryV2 firehoseFactoryV2;

  private DateTime now;
  private ListeningExecutorService taskExec;
  private Map<SegmentDescriptor, Pair<Executor, Runnable>> handOffCallbacks;

  @Parameterized.Parameters(name = "buildV9Directly = {0}, firehoseFactory = {1}, firehoseFactoryV2 = {2}")
  public static Collection<?> constructorFeeder() throws IOException
  {
    return ImmutableList.of(
        new Object[]{true, new TestFirehoseFactory(), null},
        new Object[]{true, null, new TestFirehoseFactoryV2()},
        new Object[]{false, new TestFirehoseFactory(), null},
        new Object[]{false, null, new TestFirehoseFactoryV2()}
    );
  }

  public RealtimeIndexTaskTest(
      boolean buildV9Directly,
      FirehoseFactory firehoseFactory,
      FirehoseFactoryV2 firehoseFactoryV2)
  {
    this.buildV9Directly = buildV9Directly;
    this.firehoseFactory = firehoseFactory;
    this.firehoseFactoryV2 = firehoseFactoryV2;
  }

  @Before
  public void setUp()
  {
    EmittingLogger.registerEmitter(emitter);
    emitter.start();
    taskExec = MoreExecutors.listeningDecorator(Execs.singleThreaded("realtime-index-task-test-%d"));
    now = new DateTime();
  }

  @After
  public void tearDown()
  {
    taskExec.shutdownNow();
  }

  @Test
  public void testMakeTaskId() throws Exception
  {
    Assert.assertEquals(
        "index_realtime_test_0_2015-01-02T00:00:00.000Z_abcdefgh",
        RealtimeIndexTask.makeTaskId("test", 0, new DateTime("2015-01-02"), 0x76543210)
    );
  }

  @Test(timeout = 60_000L)
  public void testDefaultResource() throws Exception
  {
    final RealtimeIndexTask task = makeRealtimeTask(null);
    Assert.assertEquals(task.getId(), task.getTaskResource().getAvailabilityGroup());
  }


  @Test(timeout = 60_000L, expected = ExecutionException.class)
  public void testHandoffTimeout() throws Exception
  {
    final TestIndexerMetadataStorageCoordinator mdc = new TestIndexerMetadataStorageCoordinator();
    final RealtimeIndexTask task = makeRealtimeTask(null, true, 100L);
    final TaskToolbox taskToolbox = makeToolbox(task, mdc, tempFolder.newFolder());
    final ListenableFuture<TaskStatus> statusFuture = runTask(task, taskToolbox);

    // Wait for firehose to show up, it starts off null.
    TestTaskFirehoseHolder holder = new TestTaskFirehoseHolder(task);
    while (holder.getFirehose() == null) {
      Thread.sleep(50);
    }

    holder.addRows(
        ImmutableList.<InputRow>of(
            new MapBasedInputRow(
                now,
                ImmutableList.of("dim1"),
                ImmutableMap.<String, Object>of("dim1", "foo", "met1", "1")
            )
        )
    );

    // Stop the firehose, this will drain out existing events.
    holder.close();

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
    TestTaskFirehoseHolder holder = new TestTaskFirehoseHolder(task);
    while (holder.getFirehose() == null) {
      Thread.sleep(50);
    }

    holder.addRows(
        ImmutableList.<InputRow>of(
            new MapBasedInputRow(
                now,
                ImmutableList.of("dim1"),
                ImmutableMap.<String, Object>of("dim1", "foo", "met1", "1")
            ),
            new MapBasedInputRow(
                now.minus(new Period("P1D")),
                ImmutableList.of("dim1"),
                ImmutableMap.<String, Object>of("dim1", "foo", "met1", 2.0)
            ),
            new MapBasedInputRow(
                now,
                ImmutableList.of("dim2"),
                ImmutableMap.<String, Object>of("dim2", "bar", "met1", 2.0)
            )
        )
    );

    // Stop the firehose, this will drain out existing events.
    holder.close();

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
    Assert.assertEquals(2, sumMetric(task, "rows"));
    Assert.assertEquals(3, sumMetric(task, "met1"));

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
    Assert.assertEquals(TaskStatus.Status.SUCCESS, taskStatus.getStatusCode());
  }

  @Test(timeout = 60_000L)
  public void testReportParseExceptionsOnBadMetric() throws Exception
  {
    final TestIndexerMetadataStorageCoordinator mdc = new TestIndexerMetadataStorageCoordinator();
    final RealtimeIndexTask task = makeRealtimeTask(null, true);
    final TaskToolbox taskToolbox = makeToolbox(task, mdc, tempFolder.newFolder());
    final ListenableFuture<TaskStatus> statusFuture = runTask(task, taskToolbox);

    // Wait for firehose to show up, it starts off null.
    TestTaskFirehoseHolder holder = new TestTaskFirehoseHolder(task);
    while (holder.getFirehose() == null) {
      Thread.sleep(50);
    }

    holder.addRows(
        ImmutableList.<InputRow>of(
            new MapBasedInputRow(
                now,
                ImmutableList.of("dim1"),
                ImmutableMap.<String, Object>of("dim1", "foo", "met1", "1")
            ),
            new MapBasedInputRow(
                now,
                ImmutableList.of("dim1"),
                ImmutableMap.<String, Object>of("dim1", "foo", "met1", "foo")
            ),
            new MapBasedInputRow(
                now.minus(new Period("P1D")),
                ImmutableList.of("dim1"),
                ImmutableMap.<String, Object>of("dim1", "foo", "met1", "foo")
            ),
            new MapBasedInputRow(
                now,
                ImmutableList.of("dim2"),
                ImmutableMap.<String, Object>of("dim2", "bar", "met1", 2.0)
            )
        )
    );

    // Stop the firehose, this will drain out existing events.
    holder.close();

    // Wait for the task to finish.
    expectedException.expect(ExecutionException.class);
    expectedException.expectCause(CoreMatchers.<Throwable>instanceOf(ParseException.class));
    expectedException.expectCause(
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.containsString("Encountered parse error for aggregator[met1]")
        )
    );
    expectedException.expect(
        ThrowableCauseMatcher.hasCause(
            ThrowableCauseMatcher.hasCause(
                CoreMatchers.allOf(
                    CoreMatchers.<Throwable>instanceOf(ParseException.class),
                    ThrowableMessageMatcher.hasMessage(
                        CoreMatchers.containsString("Unable to parse metrics[met1], value[foo]")
                    )
                )
            )
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
    TestTaskFirehoseHolder holder = new TestTaskFirehoseHolder(task);
    while (holder.getFirehose() == null) {
      Thread.sleep(50);
    }

    holder.addRows(
        Arrays.<InputRow>asList(
            // Good row- will be processed.
            new MapBasedInputRow(
                now,
                ImmutableList.of("dim1"),
                ImmutableMap.<String, Object>of("dim1", "foo", "met1", "1")
            ),
            // Null row- will be unparseable.
            null,
            // Bad metric- will count as processed, but that particular metric won't update.
            new MapBasedInputRow(
                now,
                ImmutableList.of("dim1"),
                ImmutableMap.<String, Object>of("dim1", "foo", "met1", "foo")
            ),
            // Bad row- will be unparseable.
            new MapBasedInputRow(
                now,
                ImmutableList.of("dim1", FAIL_DIM),
                ImmutableMap.<String, Object>of("dim1", "foo", "met1", 2.0)
            ),
            // Old row- will be thrownAway.
            new MapBasedInputRow(
                now.minus(new Period("P1D")),
                ImmutableList.of("dim1"),
                ImmutableMap.<String, Object>of("dim1", "foo", "met1", 2.0)
            ),
            // Good row- will be processed.
            new MapBasedInputRow(
                now,
                ImmutableList.of("dim2"),
                ImmutableMap.<String, Object>of("dim2", "bar", "met1", 2.0)
            )
        )
    );

    // Stop the firehose, this will drain out existing events.
    holder.close();

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
    Assert.assertEquals(3, sumMetric(task, "rows"));
    Assert.assertEquals(3, sumMetric(task, "met1"));

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
    Assert.assertEquals(TaskStatus.Status.SUCCESS, taskStatus.getStatusCode());
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
      TestTaskFirehoseHolder holder = new TestTaskFirehoseHolder(task1);
      while (holder.getFirehose() == null) {
        Thread.sleep(50);
      }

      holder.addRows(
          ImmutableList.<InputRow>of(
              new MapBasedInputRow(
                  now,
                  ImmutableList.of("dim1"),
                  ImmutableMap.<String, Object>of("dim1", "foo")
              )
          )
      );

      // Trigger graceful shutdown.
      task1.stopGracefully();

      // Wait for the task to finish. The status doesn't really matter, but we'll check it anyway.
      final TaskStatus taskStatus = statusFuture.get();
      Assert.assertEquals(TaskStatus.Status.SUCCESS, taskStatus.getStatusCode());

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
      TestTaskFirehoseHolder holder = new TestTaskFirehoseHolder(task2);
      while (holder.getFirehose() == null) {
        Thread.sleep(50);
      }

      // Do a query, at this point the previous data should be loaded.
      Assert.assertEquals(1, sumMetric(task2, "rows"));

      holder.addRows(
          ImmutableList.<InputRow>of(
              new MapBasedInputRow(
                  now,
                  ImmutableList.of("dim2"),
                  ImmutableMap.<String, Object>of("dim2", "bar")
              )
          )
      );

      // Stop the firehose, this will drain out existing events.
      holder.close();

      // Wait for publish.
      while (mdc.getPublished().isEmpty()) {
        Thread.sleep(50);
      }

      publishedSegment = Iterables.getOnlyElement(mdc.getPublished());

      // Do a query.
      Assert.assertEquals(2, sumMetric(task2, "rows"));

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
      Assert.assertEquals(TaskStatus.Status.SUCCESS, taskStatus.getStatusCode());
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
      TestTaskFirehoseHolder holder = new TestTaskFirehoseHolder(task1);
      while (holder.getFirehose() == null) {
        Thread.sleep(50);
      }

      holder.addRows(
          ImmutableList.<InputRow>of(
              new MapBasedInputRow(
                  now,
                  ImmutableList.of("dim1"),
                  ImmutableMap.<String, Object>of("dim1", "foo")
              )
          )
      );

      // Stop the firehose, this will trigger a finishJob.
      holder.close();

      // Wait for publish.
      while (mdc.getPublished().isEmpty()) {
        Thread.sleep(50);
      }

      publishedSegment = Iterables.getOnlyElement(mdc.getPublished());

      // Do a query.
      Assert.assertEquals(1, sumMetric(task1, "rows"));

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
      TestTaskFirehoseHolder holder = new TestTaskFirehoseHolder(task2);
      while (holder.getFirehose() == null) {
        Thread.sleep(50);
      }

      // Stop the firehose, this will trigger a finishJob.
      holder.close();

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
      Assert.assertEquals(TaskStatus.Status.SUCCESS, taskStatus.getStatusCode());
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
      TestTaskFirehoseHolder holder = new TestTaskFirehoseHolder(task1);
      while (holder.getFirehose() == null) {
        Thread.sleep(50);
      }

      holder.addRows(
          ImmutableList.<InputRow>of(
              new MapBasedInputRow(
                  now,
                  ImmutableList.of("dim1"),
                  ImmutableMap.<String, Object>of("dim1", "foo")
              )
          )
      );

      // Trigger graceful shutdown.
      task1.stopGracefully();

      // Wait for the task to finish. The status doesn't really matter, but we'll check it anyway.
      final TaskStatus taskStatus = statusFuture.get();
      Assert.assertEquals(TaskStatus.Status.SUCCESS, taskStatus.getStatusCode());

      // Nothing should be published.
      Assert.assertEquals(Sets.newHashSet(), mdc.getPublished());
    }

    // Corrupt the data:
    final File smooshFile = new File(
        String.format(
            "%s/persistent/task/%s/work/persist/%s/%s_%s/0/00000.smoosh",
            directory,
            task1.getId(),
            task1.getDataSource(),
            Granularities.DAY.bucketStart(now),
            Granularities.DAY.bucketEnd(now)
        )
    );

    Files.write(smooshFile.toPath(), "oops!".getBytes(Charsets.UTF_8));

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
    Assert.assertEquals(TaskStatus.Status.SUCCESS, taskStatus.getStatusCode());
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
    return makeRealtimeTask(taskId, true, 0);
  }

  private RealtimeIndexTask makeRealtimeTask(final String taskId, boolean reportParseExceptions)
  {
    return makeRealtimeTask(taskId, reportParseExceptions, 0);
  }

  private RealtimeIndexTask makeRealtimeTask(final String taskId, boolean reportParseExceptions, long handoffTimeout)
  {
    ObjectMapper objectMapper = new DefaultObjectMapper();
    DataSchema dataSchema = new DataSchema(
        "test_ds",
        null,
        new AggregatorFactory[]{new CountAggregatorFactory("rows"), new LongSumAggregatorFactory("met1", "met1")},
        new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
        objectMapper
    );
    RealtimeIOConfig realtimeIOConfig = new RealtimeIOConfig(
        firehoseFactory,
        null,
        firehoseFactoryV2
    );
    RealtimeTuningConfig realtimeTuningConfig = new RealtimeTuningConfig(
        1000,
        new Period("P1Y"),
        new Period("PT10M"),
        null,
        null,
        new ServerTimeRejectionPolicyFactory(),
        null,
        null,
        null,
        buildV9Directly,
        0,
        0,
        reportParseExceptions,
        handoffTimeout,
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

      @Override
      protected boolean isFirehoseV2DrainableByClosing(FirehoseFactoryV2 firehoseFactoryV2)
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
    final QueryRunnerFactoryConglomerate conglomerate = new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>of(
            TimeseriesQuery.class,
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(
                    new IntervalChunkingQueryRunnerDecorator(null, null, null)
                    {
                      @Override
                      public <T> QueryRunner<T> decorate(
                          QueryRunner<T> delegate, QueryToolChest<T, ? extends Query<T>> toolChest
                      )
                      {
                        return delegate;
                      }
                    }
                ),
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
    handOffCallbacks = Maps.newConcurrentMap();
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

          Map<SegmentDescriptor, Pair<Executor, Runnable>> getHandOffCallbacks()
          {
            return handOffCallbacks;
          }
        };
      }
    };
    final TestUtils testUtils = new TestUtils();
    final TaskToolboxFactory toolboxFactory = new TaskToolboxFactory(
        taskConfig,
        taskActionClientFactory,
        emitter,
        new TestDataSegmentPusher(),
        new TestDataSegmentKiller(),
        null, // DataSegmentMover
        null, // DataSegmentArchiver
        new TestDataSegmentAnnouncer(),
        handoffNotifierFactory,
        conglomerate,
        MoreExecutors.sameThreadExecutor(), // queryExecutorService
        EasyMock.createMock(MonitorScheduler.class),
        new SegmentLoaderFactory(
            new SegmentLoaderLocalCacheManager(
                null,
                new SegmentLoaderConfig()
                {
                  @Override
                  public List<StorageLocationConfig> getLocations()
                  {
                    return Lists.newArrayList();
                  }
                }, testUtils.getTestObjectMapper()
            )
        ),
        testUtils.getTestObjectMapper(),
        testUtils.getTestIndexMerger(),
        testUtils.getTestIndexIO(),
        MapCache.create(1024),
        new CacheConfig(),
        testUtils.getTestIndexMergerV9()
    );

    return toolboxFactory.build(task);
  }

  public long sumMetric(final Task task, final String metric) throws Exception
  {
    // Do a query.
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_ds")
                                  .aggregators(
                                      ImmutableList.<AggregatorFactory>of(
                                          new LongSumAggregatorFactory(metric, metric)
                                      )
                                  ).granularity(Granularities.ALL)
                                  .intervals("2000/3000")
                                  .build();

    ArrayList<Result<TimeseriesResultValue>> results = Sequences.toList(
        task.getQueryRunner(query).run(query, ImmutableMap.<String, Object>of()),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    return results.isEmpty() ? 0 : results.get(0).getValue().getLongMetric(metric);
  }
}

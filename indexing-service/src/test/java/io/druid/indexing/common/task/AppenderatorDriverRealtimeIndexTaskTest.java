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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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
import io.druid.indexing.common.index.RealtimeAppenderatorIngestionSpec;
import io.druid.indexing.common.index.RealtimeAppenderatorTuningConfig;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.indexing.overlord.HeapMemoryTaskStorage;
import io.druid.indexing.overlord.SegmentPublishResult;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.supervisor.SupervisorManager;
import io.druid.indexing.test.TestDataSegmentAnnouncer;
import io.druid.indexing.test.TestDataSegmentKiller;
import io.druid.indexing.test.TestDataSegmentPusher;
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
import io.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import io.druid.metadata.TestDerbyConnector;
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
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoaderLocalCacheManager;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.segment.transform.ExpressionTransform;
import io.druid.segment.transform.TransformSpec;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DataSegmentServerAnnouncer;
import io.druid.server.coordination.ServerType;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import org.apache.commons.io.FileUtils;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class AppenderatorDriverRealtimeIndexTaskTest
{
  private static final Logger log = new Logger(AppenderatorDriverRealtimeIndexTaskTest.class);
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
    public void close() throws IOException
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

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private DateTime now;
  private ListeningExecutorService taskExec;
  private Map<SegmentDescriptor, Pair<Executor, Runnable>> handOffCallbacks;
  private Collection<DataSegment> publishedSegments;
  private CountDownLatch segmentLatch;
  private CountDownLatch handoffLatch;
  private TaskStorage taskStorage;
  private TaskLockbox taskLockbox;
  private TaskToolboxFactory taskToolboxFactory;
  private File baseDir;

  @Before
  public void setUp() throws IOException
  {
    EmittingLogger.registerEmitter(emitter);
    emitter.start();
    taskExec = MoreExecutors.listeningDecorator(Execs.singleThreaded("realtime-index-task-test-%d"));
    now = DateTimes.nowUtc();

    TestDerbyConnector derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createTaskTables();
    derbyConnector.createSegmentTable();
    derbyConnector.createPendingSegmentsTable();

    baseDir = tempFolder.newFolder();
    makeToolboxFactory(baseDir);
  }

  @After
  public void tearDown()
  {
    taskExec.shutdownNow();
  }

  @Test(timeout = 60_000L)
  public void testDefaultResource() throws Exception
  {
    final AppenderatorDriverRealtimeIndexTask task = makeRealtimeTask(null);
    Assert.assertEquals(task.getId(), task.getTaskResource().getAvailabilityGroup());
  }


  @Test(timeout = 60_000L, expected = ExecutionException.class)
  public void testHandoffTimeout() throws Exception
  {
    expectPublishedSegments(1);
    final AppenderatorDriverRealtimeIndexTask task = makeRealtimeTask(null, TransformSpec.NONE, true, 100L);
    final ListenableFuture<TaskStatus> statusFuture = runTask(task);

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

    // handoff would timeout, resulting in exception
    statusFuture.get();
  }

  @Test(timeout = 60_000L)
  public void testBasics() throws Exception
  {
    expectPublishedSegments(1);
    final AppenderatorDriverRealtimeIndexTask task = makeRealtimeTask(null);
    final ListenableFuture<TaskStatus> statusFuture = runTask(task);

    // Wait for firehose to show up, it starts off null.
    while (task.getFirehose() == null) {
      Thread.sleep(50);
    }

    final TestFirehose firehose = (TestFirehose) task.getFirehose();

    firehose.addRows(
        ImmutableList.of(
            ImmutableMap.of("t", now.getMillis(), "dim1", "foo", "met1", "1"),
            ImmutableMap.of("t", now.getMillis(), "dim2", "bar", "met1", 2.0)
        )
    );

    // Stop the firehose, this will drain out existing events.
    firehose.close();

    // Wait for publish.
    Collection<DataSegment> publishedSegments = awaitSegments();

    // Check metrics.
    Assert.assertEquals(2, task.getMetrics().processed());
    Assert.assertEquals(0, task.getMetrics().thrownAway());
    Assert.assertEquals(0, task.getMetrics().unparseable());

    // Do some queries.
    Assert.assertEquals(2, sumMetric(task, null, "rows"));
    Assert.assertEquals(3, sumMetric(task, null, "met1"));

    awaitHandoffs();

    for (DataSegment publishedSegment : publishedSegments) {
      Optional<Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>>> optional = handOffCallbacks.entrySet().stream()
                                                                                                  .filter(e -> e.getKey().equals(new SegmentDescriptor(
                                                                                                      publishedSegment.getInterval(),
                                                                                                      publishedSegment.getVersion(),
                                                                                                      publishedSegment.getShardSpec().getPartitionNum()
                                                                                                  )))
                                                                                                  .findFirst();

      Assert.assertTrue(
          publishedSegment + " missing from handoff callbacks: " + handOffCallbacks,
          optional.isPresent()
      );
      Pair<Executor, Runnable> executorRunnablePair = optional.get().getValue();

      // Simulate handoff.
      executorRunnablePair.lhs.execute(executorRunnablePair.rhs);
    }
    handOffCallbacks.clear();

    // Wait for the task to finish.
    final TaskStatus taskStatus = statusFuture.get();
    Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());
  }

  @Test(timeout = 60_000L)
  public void testLateData() throws Exception
  {
    expectPublishedSegments(1);
    final AppenderatorDriverRealtimeIndexTask task = makeRealtimeTask(null);
    final ListenableFuture<TaskStatus> statusFuture = runTask(task);

    // Wait for firehose to show up, it starts off null.
    while (task.getFirehose() == null) {
      Thread.sleep(50);
    }

    final TestFirehose firehose = (TestFirehose) task.getFirehose();

    firehose.addRows(
        ImmutableList.of(
            ImmutableMap.of("t", now.getMillis(), "dim1", "foo", "met1", "1"),
            // Data is from 2 days ago, should still be processed
            ImmutableMap.of("t", now.minus(new Period("P2D")).getMillis(), "dim2", "bar", "met1", 2.0)
        )
    );

    // Stop the firehose, this will drain out existing events.
    firehose.close();

    // Wait for publish.
    Collection<DataSegment> publishedSegments = awaitSegments();

    // Check metrics.
    Assert.assertEquals(2, task.getMetrics().processed());
    Assert.assertEquals(0, task.getMetrics().thrownAway());
    Assert.assertEquals(0, task.getMetrics().unparseable());

    // Do some queries.
    Assert.assertEquals(2, sumMetric(task, null, "rows"));
    Assert.assertEquals(3, sumMetric(task, null, "met1"));

    awaitHandoffs();

    for (DataSegment publishedSegment : publishedSegments) {
      Optional<Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>>> optional = handOffCallbacks.entrySet().stream()
                                                                                                  .filter(e -> e.getKey().equals(new SegmentDescriptor(
                                                                                                      publishedSegment.getInterval(),
                                                                                                      publishedSegment.getVersion(),
                                                                                                      publishedSegment.getShardSpec().getPartitionNum()
                                                                                                  )))
                                                                                                  .findFirst();

      Assert.assertTrue(
          publishedSegment + " missing from handoff callbacks: " + handOffCallbacks,
          optional.isPresent()
      );
      Pair<Executor, Runnable> executorRunnablePair = optional.get().getValue();

      // Simulate handoff.
      executorRunnablePair.lhs.execute(executorRunnablePair.rhs);
    }
    handOffCallbacks.clear();

    // Wait for the task to finish.
    final TaskStatus taskStatus = statusFuture.get();
    Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());
  }

  @Test(timeout = 60_000L)
  public void testMaxRowsPerSegment() throws Exception
  {
    // Expect 2 segments as we will hit maxRowsPerSegment
    expectPublishedSegments(2);

    final AppenderatorDriverRealtimeIndexTask task = makeRealtimeTask(null);
    final ListenableFuture<TaskStatus> statusFuture = runTask(task);

    // Wait for firehose to show up, it starts off null.
    while (task.getFirehose() == null) {
      Thread.sleep(50);
    }

    final TestFirehose firehose = (TestFirehose) task.getFirehose();

    // maxRowsPerSegment is 1000 as configured in #makeRealtimeTask
    for (int i = 0; i < 2000; i++) {
      firehose.addRows(
          ImmutableList.of(
              ImmutableMap.of("t", now.getMillis(), "dim1", "foo-" + i, "met1", "1")
          )
      );
    }

    // Stop the firehose, this will drain out existing events.
    firehose.close();

    // Wait for publish.
    Collection<DataSegment> publishedSegments = awaitSegments();

    // Check metrics.
    Assert.assertEquals(2000, task.getMetrics().processed());
    Assert.assertEquals(0, task.getMetrics().thrownAway());
    Assert.assertEquals(0, task.getMetrics().unparseable());

    // Do some queries.
    Assert.assertEquals(2000, sumMetric(task, null, "rows"));
    Assert.assertEquals(2000, sumMetric(task, null, "met1"));

    awaitHandoffs();

    for (DataSegment publishedSegment : publishedSegments) {
      Optional<Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>>> optional = handOffCallbacks.entrySet().stream()
                                                                                                  .filter(e -> e.getKey().equals(new SegmentDescriptor(
                                                                                                      publishedSegment.getInterval(),
                                                                                                      publishedSegment.getVersion(),
                                                                                                      publishedSegment.getShardSpec().getPartitionNum()
                                                                                                  )))
                                                                                                  .findFirst();

      Assert.assertTrue(
          publishedSegment + " missing from handoff callbacks: " + handOffCallbacks,
          optional.isPresent()
      );
      Pair<Executor, Runnable> executorRunnablePair = optional.get().getValue();

      // Simulate handoff.
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
    expectPublishedSegments(2);

    final TransformSpec transformSpec = new TransformSpec(
        new SelectorDimFilter("dim1", "foo", null),
        ImmutableList.of(
            new ExpressionTransform("dim1t", "concat(dim1,dim1)", ExprMacroTable.nil())
        )
    );
    final AppenderatorDriverRealtimeIndexTask task = makeRealtimeTask(null, transformSpec, true, 0);
    final ListenableFuture<TaskStatus> statusFuture = runTask(task);

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

    Collection<DataSegment> publishedSegments = awaitSegments();

    // Check metrics.
    Assert.assertEquals(2, task.getMetrics().processed());
    Assert.assertEquals(1, task.getMetrics().thrownAway());
    Assert.assertEquals(0, task.getMetrics().unparseable());

    // Do some queries.
    Assert.assertEquals(2, sumMetric(task, null, "rows"));
    Assert.assertEquals(2, sumMetric(task, new SelectorDimFilter("dim1t", "foofoo", null), "rows"));
    Assert.assertEquals(0, sumMetric(task, new SelectorDimFilter("dim1t", "barbar", null), "rows"));
    Assert.assertEquals(3, sumMetric(task, null, "met1"));

    awaitHandoffs();

    for (DataSegment publishedSegment : publishedSegments) {
      Optional<Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>>> optional = handOffCallbacks.entrySet().stream()
                                                                                                  .filter(e -> e.getKey().equals(new SegmentDescriptor(
                                                                                                      publishedSegment.getInterval(),
                                                                                                      publishedSegment.getVersion(),
                                                                                                      publishedSegment.getShardSpec().getPartitionNum()
                                                                                                  )))
                                                                                                  .findFirst();

      Assert.assertTrue(
          publishedSegment + " missing from handoff callbacks: " + handOffCallbacks,
          optional.isPresent()
      );
      Pair<Executor, Runnable> executorRunnablePair = optional.get().getValue();

      // Simulate handoff.
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
    expectPublishedSegments(0);
    final AppenderatorDriverRealtimeIndexTask task = makeRealtimeTask(null, true);
    final ListenableFuture<TaskStatus> statusFuture = runTask(task);

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
            CoreMatchers.containsString("Encountered parse error for aggregator[met1]")
        )
    );
    expectedException.expect(
        ThrowableCauseMatcher.hasCause(
            ThrowableCauseMatcher.hasCause(
                CoreMatchers.allOf(
                    CoreMatchers.<Throwable>instanceOf(ParseException.class),
                    ThrowableMessageMatcher.hasMessage(
                        CoreMatchers.containsString("Unable to parse value[foo] for field[met1]")
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
    expectPublishedSegments(1);

    final AppenderatorDriverRealtimeIndexTask task = makeRealtimeTask(null, false);
    final ListenableFuture<TaskStatus> statusFuture = runTask(task);

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

            // Good row- will be processed.
            ImmutableMap.of("t", now.getMillis(), "dim2", "bar", "met1", 2.0)
        )
    );

    // Stop the firehose, this will drain out existing events.
    firehose.close();

    // Wait for publish.
    Collection<DataSegment> publishedSegments = awaitSegments();

    DataSegment publishedSegment = Iterables.getOnlyElement(publishedSegments);

    // Check metrics.
    Assert.assertEquals(3, task.getMetrics().processed());
    Assert.assertEquals(0, task.getMetrics().thrownAway());
    Assert.assertEquals(2, task.getMetrics().unparseable());

    // Do some queries.
    Assert.assertEquals(3, sumMetric(task, null, "rows"));
    Assert.assertEquals(3, sumMetric(task, null, "met1"));

    awaitHandoffs();

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
    expectPublishedSegments(0);

    final AppenderatorDriverRealtimeIndexTask task1 = makeRealtimeTask(null);
    final DataSegment publishedSegment;

    // First run:
    {
      final ListenableFuture<TaskStatus> statusFuture = runTask(task1);

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
      Assert.assertTrue(publishedSegments.isEmpty());
    }

    // Second run:
    {
      expectPublishedSegments(1);
      final AppenderatorDriverRealtimeIndexTask task2 = makeRealtimeTask(task1.getId());
      final ListenableFuture<TaskStatus> statusFuture = runTask(task2);

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

      Collection<DataSegment> publishedSegments = awaitSegments();

      publishedSegment = Iterables.getOnlyElement(publishedSegments);

      // Do a query.
      Assert.assertEquals(2, sumMetric(task2, null, "rows"));

      awaitHandoffs();

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
    final AppenderatorDriverRealtimeIndexTask task1 = makeRealtimeTask(null);
    final DataSegment publishedSegment;

    // First run:
    {
      expectPublishedSegments(1);
      final ListenableFuture<TaskStatus> statusFuture = runTask(task1);

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

      Collection<DataSegment> publishedSegments = awaitSegments();

      publishedSegment = Iterables.getOnlyElement(publishedSegments);

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
      expectPublishedSegments(1);
      final AppenderatorDriverRealtimeIndexTask task2 = makeRealtimeTask(task1.getId());
      final ListenableFuture<TaskStatus> statusFuture = runTask(task2);

      // Wait for firehose to show up, it starts off null.
      while (task2.getFirehose() == null) {
        Thread.sleep(50);
      }

      // Stop the firehose again, this will start another handoff.
      final TestFirehose firehose = (TestFirehose) task2.getFirehose();

      // Stop the firehose, this will trigger a finishJob.
      firehose.close();

      awaitHandoffs();

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
    final AppenderatorDriverRealtimeIndexTask task1 = makeRealtimeTask(null);

    // First run:
    {
      expectPublishedSegments(0);

      final ListenableFuture<TaskStatus> statusFuture = runTask(task1);

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
      Assert.assertTrue(publishedSegments.isEmpty());
    }

    Optional<File> optional = FileUtils.listFiles(baseDir, null, true).stream()
                                       .filter(f -> f.getName().equals("00000.smoosh"))
                                       .findFirst();

    Assert.assertTrue("Could not find smoosh file", optional.isPresent());

    // Corrupt the data:
    final File smooshFile = optional.get();

    Files.write(smooshFile.toPath(), StringUtils.toUtf8("oops!"));

    // Second run:
    {
      expectPublishedSegments(0);

      final AppenderatorDriverRealtimeIndexTask task2 = makeRealtimeTask(task1.getId());
      final ListenableFuture<TaskStatus> statusFuture = runTask(task2);

      // Wait for the task to finish.
      boolean caught = false;
      try {
        statusFuture.get();
      }
      catch (Exception expected) {
        caught = true;
      }
      Assert.assertTrue("expected exception", caught);
    }
  }

  @Test(timeout = 60_000L)
  public void testStopBeforeStarting() throws Exception
  {
    expectPublishedSegments(0);

    final AppenderatorDriverRealtimeIndexTask task1 = makeRealtimeTask(null);

    task1.stopGracefully();
    final ListenableFuture<TaskStatus> statusFuture = runTask(task1);

    // Wait for the task to finish.
    final TaskStatus taskStatus = statusFuture.get();
    Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());
  }

  private ListenableFuture<TaskStatus> runTask(final Task task)
  {
    try {
      taskStorage.insert(task, TaskStatus.running(task.getId()));
    }
    catch (EntryExistsException e) {
      // suppress
    }
    taskLockbox.syncFromStorage();
    final TaskToolbox toolbox = taskToolboxFactory.build(task);
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

  private AppenderatorDriverRealtimeIndexTask makeRealtimeTask(final String taskId)
  {
    return makeRealtimeTask(taskId, TransformSpec.NONE, true, 0);
  }

  private AppenderatorDriverRealtimeIndexTask makeRealtimeTask(final String taskId, boolean reportParseExceptions)
  {
    return makeRealtimeTask(taskId, TransformSpec.NONE, reportParseExceptions, 0);
  }

  private AppenderatorDriverRealtimeIndexTask makeRealtimeTask(
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
    RealtimeAppenderatorTuningConfig tuningConfig = new RealtimeAppenderatorTuningConfig(
        1000,
        1000,
        null,
        null,
        null,
        null,
        null,
        reportParseExceptions,
        handoffTimeout,
        null,
        null
    );
    return new AppenderatorDriverRealtimeIndexTask(
        taskId,
        null,
        new RealtimeAppenderatorIngestionSpec(dataSchema, realtimeIOConfig, tuningConfig),
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

  private void expectPublishedSegments(int count)
  {
    segmentLatch = new CountDownLatch(count);
    handoffLatch = new CountDownLatch(count);
  }

  private Collection<DataSegment> awaitSegments() throws InterruptedException
  {
    Assert.assertTrue(
        "Timed out waiting for segments to be published",
        segmentLatch.await(1, TimeUnit.MINUTES)
    );

    return publishedSegments;
  }

  private void awaitHandoffs() throws InterruptedException
  {
    Assert.assertTrue(
        "Timed out waiting for segments to be handed off",
        handoffLatch.await(1, TimeUnit.MINUTES)
    );
  }

  private void makeToolboxFactory(final File directory)
  {
    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(null));
    taskLockbox = new TaskLockbox(taskStorage);

    publishedSegments = new CopyOnWriteArrayList<>();

    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(LinearShardSpec.class);
    mapper.registerSubtypes(NumberedShardSpec.class);
    IndexerSQLMetadataStorageCoordinator mdc = new IndexerSQLMetadataStorageCoordinator(
        mapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnectorRule.getConnector()
    )
    {
      @Override
      public Set<DataSegment> announceHistoricalSegments(Set<DataSegment> segments) throws IOException
      {
        Set<DataSegment> result = super.announceHistoricalSegments(segments);

        Assert.assertFalse(
            "Segment latch not initialized, did you forget to call expectPublishSegments?",
            segmentLatch == null
        );

        publishedSegments.addAll(result);
        segments.forEach(s -> segmentLatch.countDown());

        return result;
      }

      @Override
      public SegmentPublishResult announceHistoricalSegments(
          Set<DataSegment> segments, DataSourceMetadata startMetadata, DataSourceMetadata endMetadata
      ) throws IOException
      {
        SegmentPublishResult result = super.announceHistoricalSegments(segments, startMetadata, endMetadata);

        Assert.assertFalse(
            "Segment latch not initialized, did you forget to call expectPublishSegments?",
            segmentLatch == null
        );

        publishedSegments.addAll(result.getSegments());
        result.getSegments().forEach(s -> segmentLatch.countDown());

        return result;
      }
    };
    final TaskConfig taskConfig = new TaskConfig(directory.getPath(), null, null, 50000, null, false, null, null);

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
            handoffLatch.countDown();
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
    SegmentLoaderConfig segmentLoaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return Lists.newArrayList();
      }
    };

    taskToolboxFactory = new TaskToolboxFactory(
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
        new DataNodeService("tier", 1000, ServerType.INDEXER_EXECUTOR, 0)
    );
  }

  public long sumMetric(final Task task, final DimFilter filter, final String metric) throws Exception
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

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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.io.FileUtils;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.SingleFileTaskReportFileWriter;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.LocalTaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionToolbox;
import org.apache.druid.indexing.common.actions.TaskAuditLogConfig;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.index.RealtimeAppenderatorIngestionSpec;
import org.apache.druid.indexing.common.index.RealtimeAppenderatorTuningConfig;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.HeapMemoryTaskStorage;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.test.TestDataSegmentAnnouncer;
import org.apache.druid.indexing.test.TestDataSegmentKiller;
import org.apache.druid.indexing.test.TestDataSegmentPusher;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.handoff.SegmentHandoffNotifier;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class AppenderatorDriverRealtimeIndexTaskTest extends InitializedNullHandlingTest
{
  private static final Logger log = new Logger(AppenderatorDriverRealtimeIndexTaskTest.class);
  private static final ServiceEmitter EMITTER = new ServiceEmitter(
      "service",
      "host",
      new NoopEmitter()
  );
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();

  private static final String FAIL_DIM = "__fail__";

  private static class TestFirehose implements Firehose
  {
    private final InputRowParser<Map<String, Object>> parser;
    private final Deque<Optional<Map<String, Object>>> queue = new ArrayDeque<>();
    private boolean closed = false;

    public TestFirehose(final InputRowParser<Map<String, Object>> parser)
    {
      this.parser = parser;
    }

    public void addRows(List<Map<String, Object>> rows)
    {
      synchronized (this) {
        rows.stream().map(Optional::ofNullable).forEach(queue::add);
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
        throw new RuntimeException(e);
      }
    }

    @Override
    public InputRow nextRow()
    {
      synchronized (this) {
        final InputRow row = parser.parseBatch(queue.removeFirst().orElse(null)).get(0);
        if (row != null && row.getRaw(FAIL_DIM) != null) {
          throw new ParseException(null, FAIL_DIM);
        }
        return row;
      }
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
  private File reportsFile;

  @Before
  public void setUp() throws IOException
  {
    EmittingLogger.registerEmitter(EMITTER);
    EMITTER.start();
    taskExec = MoreExecutors.listeningDecorator(Execs.singleThreaded("realtime-index-task-test-%d"));
    now = DateTimes.nowUtc();

    TestDerbyConnector derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createTaskTables();
    derbyConnector.createSegmentTable();
    derbyConnector.createPendingSegmentsTable();

    baseDir = tempFolder.newFolder();
    reportsFile = File.createTempFile("KafkaIndexTaskTestReports-" + System.currentTimeMillis(), "json");
    makeToolboxFactory(baseDir);
  }

  @After
  public void tearDown()
  {
    taskExec.shutdownNow();
    reportsFile.delete();
  }

  @Test(timeout = 60_000L)
  public void testDefaultResource()
  {
    final AppenderatorDriverRealtimeIndexTask task = makeRealtimeTask(null);
    Assert.assertEquals(task.getId(), task.getTaskResource().getAvailabilityGroup());
  }


  @Test(timeout = 60_000L)
  public void testHandoffTimeout() throws Exception
  {
    expectPublishedSegments(1);
    final AppenderatorDriverRealtimeIndexTask task = makeRealtimeTask(null, TransformSpec.NONE, true, 100L, true, 0, 1);
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
    TaskStatus status = statusFuture.get();
    Assert.assertTrue(status.getErrorMsg()
                            .contains("java.util.concurrent.TimeoutException: Waited 100 milliseconds"));
  }

  @Test(timeout = 60_000L)
  public void testBasics() throws Exception
  {
    expectPublishedSegments(1);
    final AppenderatorDriverRealtimeIndexTask task = makeRealtimeTask(null);
    Assert.assertTrue(task.supportsQueries());
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
    Assert.assertEquals(2, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(0, task.getRowIngestionMeters().getUnparseable());

    // Do some queries.
    Assert.assertEquals(2, sumMetric(task, null, "rows").longValue());
    Assert.assertEquals(3, sumMetric(task, null, "met1").longValue());

    awaitHandoffs();

    for (DataSegment publishedSegment : publishedSegments) {
      Pair<Executor, Runnable> executorRunnablePair = handOffCallbacks.get(
          new SegmentDescriptor(
              publishedSegment.getInterval(),
              publishedSegment.getVersion(),
              publishedSegment.getShardSpec().getPartitionNum()
          )
      );
      Assert.assertNotNull(
          publishedSegment + " missing from handoff callbacks: " + handOffCallbacks,
          executorRunnablePair
      );

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
    Assert.assertEquals(2, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(0, task.getRowIngestionMeters().getUnparseable());

    // Do some queries.
    Assert.assertEquals(2, sumMetric(task, null, "rows").longValue());
    Assert.assertEquals(3, sumMetric(task, null, "met1").longValue());

    awaitHandoffs();

    for (DataSegment publishedSegment : publishedSegments) {
      Pair<Executor, Runnable> executorRunnablePair = handOffCallbacks.get(
          new SegmentDescriptor(
              publishedSegment.getInterval(),
              publishedSegment.getVersion(),
              publishedSegment.getShardSpec().getPartitionNum()
          )
      );
      Assert.assertNotNull(
          publishedSegment + " missing from handoff callbacks: " + handOffCallbacks,
          executorRunnablePair
      );

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
    Assert.assertEquals(2000, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(0, task.getRowIngestionMeters().getUnparseable());

    // Do some queries.
    Assert.assertEquals(2000, sumMetric(task, null, "rows").longValue());
    Assert.assertEquals(2000, sumMetric(task, null, "met1").longValue());

    awaitHandoffs();

    for (DataSegment publishedSegment : publishedSegments) {
      Pair<Executor, Runnable> executorRunnablePair = handOffCallbacks.get(
          new SegmentDescriptor(
              publishedSegment.getInterval(),
              publishedSegment.getVersion(),
              publishedSegment.getShardSpec().getPartitionNum()
          )
      );
      Assert.assertNotNull(
          publishedSegment + " missing from handoff callbacks: " + handOffCallbacks,
          executorRunnablePair
      );

      // Simulate handoff.
      executorRunnablePair.lhs.execute(executorRunnablePair.rhs);
    }
    handOffCallbacks.clear();

    // Wait for the task to finish.
    final TaskStatus taskStatus = statusFuture.get();
    Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());
  }

  @Test(timeout = 60_000L)
  public void testMaxTotalRows() throws Exception
  {
    // Expect 2 segments as we will hit maxTotalRows
    expectPublishedSegments(2);

    final AppenderatorDriverRealtimeIndexTask task =
        makeRealtimeTask(null, Integer.MAX_VALUE, 1500L);
    final ListenableFuture<TaskStatus> statusFuture = runTask(task);

    // Wait for firehose to show up, it starts off null.
    while (task.getFirehose() == null) {
      Thread.sleep(50);
    }

    final TestFirehose firehose = (TestFirehose) task.getFirehose();

    // maxTotalRows is 1500
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
    Assert.assertEquals(2000, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(0, task.getRowIngestionMeters().getUnparseable());

    // Do some queries.
    Assert.assertEquals(2000, sumMetric(task, null, "rows").longValue());
    Assert.assertEquals(2000, sumMetric(task, null, "met1").longValue());

    awaitHandoffs();

    Assert.assertEquals(2, publishedSegments.size());
    for (DataSegment publishedSegment : publishedSegments) {
      Pair<Executor, Runnable> executorRunnablePair = handOffCallbacks.get(
          new SegmentDescriptor(
              publishedSegment.getInterval(),
              publishedSegment.getVersion(),
              publishedSegment.getShardSpec().getPartitionNum()
          )
      );
      Assert.assertNotNull(
          publishedSegment + " missing from handoff callbacks: " + handOffCallbacks,
          executorRunnablePair
      );

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
    final AppenderatorDriverRealtimeIndexTask task = makeRealtimeTask(null, transformSpec, true, 0, true, 0, 1);
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
    Assert.assertEquals(2, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(1, task.getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(0, task.getRowIngestionMeters().getUnparseable());

    // Do some queries.
    Assert.assertEquals(2, sumMetric(task, null, "rows").longValue());
    Assert.assertEquals(2, sumMetric(task, new SelectorDimFilter("dim1t", "foofoo", null), "rows").longValue());
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(0, sumMetric(task, new SelectorDimFilter("dim1t", "barbar", null), "metric1").longValue());
    } else {
      Assert.assertNull(sumMetric(task, new SelectorDimFilter("dim1t", "barbar", null), "metric1"));
    }
    Assert.assertEquals(3, sumMetric(task, null, "met1").longValue());

    awaitHandoffs();

    for (DataSegment publishedSegment : publishedSegments) {
      Pair<Executor, Runnable> executorRunnablePair = handOffCallbacks.get(
          new SegmentDescriptor(
              publishedSegment.getInterval(),
              publishedSegment.getVersion(),
              publishedSegment.getShardSpec().getPartitionNum()
          )
      );
      Assert.assertNotNull(
          publishedSegment + " missing from handoff callbacks: " + handOffCallbacks,
          executorRunnablePair
      );

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
            ImmutableMap.of("t", 2000000L, "dim1", "foo", "met1", "1"),
            ImmutableMap.of("t", 3000000L, "dim1", "foo", "met1", "foo"),
            ImmutableMap.of("t", now.minus(new Period("P1D")).getMillis(), "dim1", "foo", "met1", "foo"),
            ImmutableMap.of("t", 4000000L, "dim2", "bar", "met1", 2.0)
        )
    );

    // Stop the firehose, this will drain out existing events.
    firehose.close();

    // Wait for the task to finish.
    TaskStatus status = statusFuture.get();
    Assert.assertTrue(status.getErrorMsg().contains("org.apache.druid.java.util.common.RE: Max parse exceptions[0] exceeded"));

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    ParseExceptionReport parseExceptionReport =
        ParseExceptionReport.forPhase(reportData, RowIngestionMeters.BUILD_SEGMENTS);
    List<String> expectedMessages = ImmutableList.of(
        "Unable to parse value[foo] for field[met1]"
    );
    Assert.assertEquals(expectedMessages, parseExceptionReport.getErrorMessages());

    List<String> expectedInputs = ImmutableList.of(
        "{t=3000000, dim1=foo, met1=foo}"
    );
    Assert.assertEquals(expectedInputs, parseExceptionReport.getInputs());
  }

  @Test(timeout = 60_000L)
  public void testNoReportParseExceptions() throws Exception
  {
    expectPublishedSegments(1);

    final AppenderatorDriverRealtimeIndexTask task = makeRealtimeTask(
        null,
        TransformSpec.NONE,
        false,
        0,
        true,
        null,
        1
    );
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
    Assert.assertEquals(2, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(1, task.getRowIngestionMeters().getProcessedWithError());
    Assert.assertEquals(0, task.getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(2, task.getRowIngestionMeters().getUnparseable());

    // Do some queries.
    Assert.assertEquals(3, sumMetric(task, null, "rows").longValue());
    Assert.assertEquals(3, sumMetric(task, null, "met1").longValue());

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

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED, 2,
            RowIngestionMeters.PROCESSED_BYTES, 0,
            RowIngestionMeters.PROCESSED_WITH_ERROR, 1,
            RowIngestionMeters.UNPARSEABLE, 2,
            RowIngestionMeters.THROWN_AWAY, 0
        )
    );


    // Wait for the task to finish.
    final TaskStatus taskStatus = statusFuture.get();
    Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Assert.assertEquals(expectedMetrics, reportData.getRowStats());
  }

  @Test(timeout = 60_000L)
  public void testMultipleParseExceptionsSuccess() throws Exception
  {
    expectPublishedSegments(1);

    final AppenderatorDriverRealtimeIndexTask task = makeRealtimeTask(null, TransformSpec.NONE, false, 0, true, 10, 10);
    final ListenableFuture<TaskStatus> statusFuture = runTask(task);

    // Wait for firehose to show up, it starts off null.
    while (task.getFirehose() == null) {
      Thread.sleep(50);
    }

    final TestFirehose firehose = (TestFirehose) task.getFirehose();

    firehose.addRows(
        Arrays.asList(
            // Good row- will be processed.
            ImmutableMap.of("t", 1521251960729L, "dim1", "foo", "met1", "1"),

            // Null row- will be thrown away.
            null,

            // Bad metric- will count as processed, but that particular metric won't update.
            ImmutableMap.of("t", 1521251960729L, "dim1", "foo", "met1", "foo"),

            // Bad long dim- will count as processed, but bad dims will get default values
            ImmutableMap.of(
                "t",
                1521251960729L,
                "dim1",
                "foo",
                "dimLong",
                "notnumber",
                "dimFloat",
                "notnumber",
                "met1",
                "foo"
            ),

            // Bad row- will be unparseable.
            ImmutableMap.of("dim1", "foo", "met1", 2.0, FAIL_DIM, "x"),

            // Good row- will be processed.
            ImmutableMap.of("t", 1521251960729L, "dim2", "bar", "met1", 2.0)
        )
    );

    // Stop the firehose, this will drain out existing events.
    firehose.close();

    // Wait for publish.
    Collection<DataSegment> publishedSegments = awaitSegments();

    DataSegment publishedSegment = Iterables.getOnlyElement(publishedSegments);

    // Check metrics.
    Assert.assertEquals(2, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(2, task.getRowIngestionMeters().getProcessedWithError());
    Assert.assertEquals(0, task.getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(2, task.getRowIngestionMeters().getUnparseable());

    // Do some queries.
    Assert.assertEquals(4, sumMetric(task, null, "rows").longValue());
    Assert.assertEquals(3, sumMetric(task, null, "met1").longValue());

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

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED, 2,
            RowIngestionMeters.PROCESSED_BYTES, 0,
            RowIngestionMeters.PROCESSED_WITH_ERROR, 2,
            RowIngestionMeters.UNPARSEABLE, 2,
            RowIngestionMeters.THROWN_AWAY, 0
        )
    );

    // Wait for the task to finish.
    final TaskStatus taskStatus = statusFuture.get();
    Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();
    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    ParseExceptionReport parseExceptionReport =
        ParseExceptionReport.forPhase(reportData, RowIngestionMeters.BUILD_SEGMENTS);

    List<String> expectedMessages = Arrays.asList(
        "Timestamp[null] is unparseable! Event: {dim1=foo, met1=2.0, __fail__=x}",
        "could not convert value [notnumber] to long",
        "Unable to parse value[foo] for field[met1]",
        "Timestamp[null] is unparseable! Event: null"
    );
    Assert.assertEquals(expectedMessages, parseExceptionReport.getErrorMessages());

    List<String> expectedInputs = Arrays.asList(
        "{dim1=foo, met1=2.0, __fail__=x}",
        "{t=1521251960729, dim1=foo, dimLong=notnumber, dimFloat=notnumber, met1=foo}",
        "{t=1521251960729, dim1=foo, met1=foo}",
        null
    );
    Assert.assertEquals(expectedInputs, parseExceptionReport.getInputs());
    Assert.assertEquals(IngestionState.COMPLETED, reportData.getIngestionState());
  }

  @Test(timeout = 60_000L)
  public void testMultipleParseExceptionsFailure() throws Exception
  {
    expectPublishedSegments(1);

    final AppenderatorDriverRealtimeIndexTask task = makeRealtimeTask(null, TransformSpec.NONE, false, 0, true, 3, 10);
    final ListenableFuture<TaskStatus> statusFuture = runTask(task);

    // Wait for firehose to show up, it starts off null.
    while (task.getFirehose() == null) {
      Thread.sleep(50);
    }

    final TestFirehose firehose = (TestFirehose) task.getFirehose();

    firehose.addRows(
        Arrays.asList(
            // Good row- will be processed.
            ImmutableMap.of("t", 1521251960729L, "dim1", "foo", "met1", "1"),

            // Null row- will be thrown away.
            null,

            // Bad metric- will count as processed, but that particular metric won't update.
            ImmutableMap.of("t", 1521251960729L, "dim1", "foo", "met1", "foo"),

            // Bad long dim- will count as processed, but bad dims will get default values
            ImmutableMap.of(
                "t",
                1521251960729L,
                "dim1",
                "foo",
                "dimLong",
                "notnumber",
                "dimFloat",
                "notnumber",
                "met1",
                "foo"
            ),

            // Bad row- will be unparseable.
            ImmutableMap.of("dim1", "foo", "met1", 2.0, FAIL_DIM, "x"),

            // Good row- will be processed.
            ImmutableMap.of("t", 1521251960729L, "dim2", "bar", "met1", 2.0)
        )
    );

    // Stop the firehose, this will drain out existing events.
    firehose.close();

    // Wait for the task to finish.
    final TaskStatus taskStatus = statusFuture.get();
    Assert.assertEquals(TaskState.FAILED, taskStatus.getStatusCode());
    Assert.assertTrue(taskStatus.getErrorMsg().contains("Max parse exceptions[3] exceeded"));

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED, 1,
            RowIngestionMeters.PROCESSED_BYTES, 0,
            RowIngestionMeters.PROCESSED_WITH_ERROR, 2,
            RowIngestionMeters.UNPARSEABLE, 2,
            RowIngestionMeters.THROWN_AWAY, 0
        )
    );
    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    ParseExceptionReport parseExceptionReport =
        ParseExceptionReport.forPhase(reportData, RowIngestionMeters.BUILD_SEGMENTS);

    List<String> expectedMessages = ImmutableList.of(
        "Timestamp[null] is unparseable! Event: {dim1=foo, met1=2.0, __fail__=x}",
        "could not convert value [notnumber] to long",
        "Unable to parse value[foo] for field[met1]",
        "Timestamp[null] is unparseable! Event: null"
    );
    Assert.assertEquals(expectedMessages, parseExceptionReport.getErrorMessages());

    List<String> expectedInputs = Arrays.asList(
        "{dim1=foo, met1=2.0, __fail__=x}",
        "{t=1521251960729, dim1=foo, dimLong=notnumber, dimFloat=notnumber, met1=foo}",
        "{t=1521251960729, dim1=foo, met1=foo}",
        null
    );
    Assert.assertEquals(expectedInputs, parseExceptionReport.getInputs());
    Assert.assertEquals(IngestionState.BUILD_SEGMENTS, reportData.getIngestionState());
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
      task1.stopGracefully(taskToolboxFactory.build(task1).getConfig());

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
      Assert.assertEquals(1, sumMetric(task2, null, "rows").longValue());

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
      Assert.assertEquals(2, sumMetric(task2, null, "rows").longValue());

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
      Assert.assertEquals(1, sumMetric(task1, null, "rows").longValue());

      // Trigger graceful shutdown.
      task1.stopGracefully(taskToolboxFactory.build(task1).getConfig());

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
      task1.stopGracefully(taskToolboxFactory.build(task1).getConfig());

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
      TaskStatus status = statusFuture.get();

      Map<String, Object> expectedMetrics = ImmutableMap.of(
          RowIngestionMeters.BUILD_SEGMENTS,
          ImmutableMap.of(
              RowIngestionMeters.PROCESSED_WITH_ERROR, 0,
              RowIngestionMeters.PROCESSED, 0,
              RowIngestionMeters.PROCESSED_BYTES, 0,
              RowIngestionMeters.UNPARSEABLE, 0,
              RowIngestionMeters.THROWN_AWAY, 0
          )
      );

      IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();
      Assert.assertEquals(expectedMetrics, reportData.getRowStats());

      Pattern errorPattern = Pattern.compile(
          "(?s)java\\.lang\\.IllegalArgumentException.*\n"
          + "\tat (java\\.base/)?java\\.nio\\.Buffer\\..*"
      );
      Assert.assertTrue(errorPattern.matcher(status.getErrorMsg()).matches());
    }
  }

  @Test(timeout = 60_000L)
  public void testStopBeforeStarting() throws Exception
  {
    expectPublishedSegments(0);

    final AppenderatorDriverRealtimeIndexTask task1 = makeRealtimeTask(null);

    task1.stopGracefully(taskToolboxFactory.build(task1).getConfig());
    final ListenableFuture<TaskStatus> statusFuture = runTask(task1);

    // Wait for the task to finish.
    final TaskStatus taskStatus = statusFuture.get();
    Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());
  }

  @Test(timeout = 60_000L)
  public void testInputSourceResourcesThrowException()
  {
    // Expect 2 segments as we will hit maxTotalRows
    expectPublishedSegments(2);

    final AppenderatorDriverRealtimeIndexTask task =
        makeRealtimeTask(null, Integer.MAX_VALUE, 1500L);
    Assert.assertThrows(
        UOE.class,
        task::getInputSourceResources
    );
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
        () -> {
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
    );
  }

  private AppenderatorDriverRealtimeIndexTask makeRealtimeTask(final String taskId)
  {
    return makeRealtimeTask(
        taskId,
        TransformSpec.NONE,
        true,
        0,
        true,
        0,
        1
    );
  }

  private AppenderatorDriverRealtimeIndexTask makeRealtimeTask(
      final String taskId,
      final Integer maxRowsPerSegment,
      final Long maxTotalRows
  )
  {
    return makeRealtimeTask(
        taskId,
        TransformSpec.NONE,
        true,
        0,
        true,
        0,
        1,
        maxRowsPerSegment,
        maxTotalRows
    );
  }

  private AppenderatorDriverRealtimeIndexTask makeRealtimeTask(final String taskId, boolean reportParseExceptions)
  {
    return makeRealtimeTask(
        taskId,
        TransformSpec.NONE,
        reportParseExceptions,
        0,
        true,
        null,
        1
    );
  }

  private AppenderatorDriverRealtimeIndexTask makeRealtimeTask(
      final String taskId,
      final TransformSpec transformSpec,
      final boolean reportParseExceptions,
      final long handoffTimeout,
      final Boolean logParseExceptions,
      final Integer maxParseExceptions,
      final Integer maxSavedParseExceptions
  )
  {

    return makeRealtimeTask(
        taskId,
        transformSpec,
        reportParseExceptions,
        handoffTimeout,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        1000,
        null
    );
  }

  private AppenderatorDriverRealtimeIndexTask makeRealtimeTask(
      final String taskId,
      final TransformSpec transformSpec,
      final boolean reportParseExceptions,
      final long handoffTimeout,
      final Boolean logParseExceptions,
      final Integer maxParseExceptions,
      final Integer maxSavedParseExceptions,
      final Integer maxRowsPerSegment,
      final Long maxTotalRows
  )
  {
    DataSchema dataSchema = new DataSchema(
        "test_ds",
        TestHelper.makeJsonMapper().convertValue(
            new MapInputRowParser(
                new TimeAndDimsParseSpec(
                    new TimestampSpec("t", "auto", null),
                    new DimensionsSpec(
                        ImmutableList.of(
                            new StringDimensionSchema("dim1"),
                            new StringDimensionSchema("dim2"),
                            new StringDimensionSchema("dim1t"),
                            new LongDimensionSchema("dimLong"),
                            new FloatDimensionSchema("dimFloat")
                        )
                    )
                )
            ),
            JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
        ),
        new AggregatorFactory[]{new CountAggregatorFactory("rows"), new LongSumAggregatorFactory("met1", "met1")},
        new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
        transformSpec,
        OBJECT_MAPPER
    );
    RealtimeIOConfig realtimeIOConfig = new RealtimeIOConfig(
        new TestFirehoseFactory(),
        null
    );
    RealtimeAppenderatorTuningConfig tuningConfig = new RealtimeAppenderatorTuningConfig(
        null,
        1000,
        null,
        null,
        maxRowsPerSegment,
        maxTotalRows,
        null,
        null,
        null,
        null,
        null,
        null,
        reportParseExceptions,
        handoffTimeout,
        null,
        null,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
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
      public Set<DataSegment> commitSegments(Set<DataSegment> segments) throws IOException
      {
        Set<DataSegment> result = super.commitSegments(segments);

        Assert.assertFalse(
            "Segment latch not initialized, did you forget to call expectPublishSegments?",
            segmentLatch == null
        );

        publishedSegments.addAll(result);
        segments.forEach(s -> segmentLatch.countDown());

        return result;
      }

      @Override
      public SegmentPublishResult commitSegmentsAndMetadata(
          Set<DataSegment> segments,
          DataSourceMetadata startMetadata,
          DataSourceMetadata endMetadata
      ) throws IOException
      {
        SegmentPublishResult result = super.commitSegmentsAndMetadata(segments, startMetadata, endMetadata);

        Assert.assertNotNull(
            "Segment latch not initialized, did you forget to call expectPublishSegments?",
            segmentLatch
        );

        publishedSegments.addAll(result.getSegments());
        result.getSegments().forEach(s -> segmentLatch.countDown());

        return result;
      }
    };

    taskLockbox = new TaskLockbox(taskStorage, mdc);
    final TaskConfig taskConfig = new TaskConfigBuilder()
        .setBaseDir(directory.getPath())
        .setDefaultRowFlushBoundary(50000)
        .setRestoreTasksOnRestart(true)
        .setBatchProcessingMode(TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name())
        .build();

    final TaskActionToolbox taskActionToolbox = new TaskActionToolbox(
        taskLockbox,
        taskStorage,
        mdc,
        EMITTER,
        EasyMock.createMock(SupervisorManager.class),
        OBJECT_MAPPER
    );
    final TaskActionClientFactory taskActionClientFactory = new LocalTaskActionClientFactory(
        taskStorage,
        taskActionToolbox,
        new TaskAuditLogConfig(false)
    );
    final QueryRunnerFactoryConglomerate conglomerate = new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.of(
            TimeseriesQuery.class,
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(),
                new TimeseriesQueryEngine(),
                (query, future) -> {
                  // do nothing
                }
            )
        )
    );
    handOffCallbacks = new ConcurrentHashMap<>();
    final SegmentHandoffNotifierFactory handoffNotifierFactory = dataSource -> new SegmentHandoffNotifier()
    {
      @Override
      public boolean registerSegmentHandoffCallback(
          SegmentDescriptor descriptor,
          Executor exec,
          Runnable handOffRunnable
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

    };
    final TestUtils testUtils = new TestUtils();
    taskToolboxFactory = new TaskToolboxFactory(
        null,
        taskConfig,
        new DruidNode("druid/middlemanager", "localhost", false, 8091, null, true, false),
        taskActionClientFactory,
        EMITTER,
        new TestDataSegmentPusher(),
        new TestDataSegmentKiller(),
        null, // DataSegmentMover
        null, // DataSegmentArchiver
        new TestDataSegmentAnnouncer(),
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
        handoffNotifierFactory,
        () -> conglomerate,
        DirectQueryProcessingPool.INSTANCE, // queryExecutorService
        NoopJoinableFactory.INSTANCE,
        () -> EasyMock.createMock(MonitorScheduler.class),
        new SegmentCacheManagerFactory(testUtils.getTestObjectMapper()),
        testUtils.getTestObjectMapper(),
        testUtils.getTestIndexIO(),
        MapCache.create(1024),
        new CacheConfig(),
        new CachePopulatorStats(),
        testUtils.getIndexMergerV9Factory(),
        EasyMock.createNiceMock(DruidNodeAnnouncer.class),
        EasyMock.createNiceMock(DruidNode.class),
        new LookupNodeService("tier"),
        new DataNodeService("tier", 1000, ServerType.INDEXER_EXECUTOR, 0),
        new SingleFileTaskReportFileWriter(reportsFile),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new NoopChatHandlerProvider(),
        testUtils.getRowIngestionMetersFactory(),
        new TestAppenderatorsManager(),
        new NoopOverlordClient(),
        new NoopCoordinatorClient(),
        null,
        null,
        null,
        "1",
        CentralizedDatasourceSchemaConfig.create()
    );
  }

  @Nullable
  public Long sumMetric(final Task task, final DimFilter filter, final String metric)
  {
    // Do a query.
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_ds")
                                  .filters(filter)
                                  .aggregators(
                                      ImmutableList.of(
                                          new LongSumAggregatorFactory(metric, metric)
                                      )
                                  ).granularity(Granularities.ALL)
                                  .intervals("2000/3000")
                                  .build();

    List<Result<TimeseriesResultValue>> results =
        task.getQueryRunner(query).run(QueryPlus.wrap(query)).toList();

    if (results.isEmpty()) {
      return 0L;
    } else {
      return results.get(0).getValue().getLongMetric(metric);
    }
  }

  private IngestionStatsAndErrorsTaskReportData getTaskReportData() throws IOException
  {
    Map<String, TaskReport> taskReports = OBJECT_MAPPER.readValue(
        reportsFile,
        new TypeReference<Map<String, TaskReport>>()
        {
        }
    );
    return IngestionStatsAndErrorsTaskReportData.getPayloadFromTaskReports(
        taskReports
    );
  }
}

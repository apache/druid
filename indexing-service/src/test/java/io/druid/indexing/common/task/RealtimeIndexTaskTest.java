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
import com.google.api.client.util.Charsets;
import com.google.api.client.util.Sets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Granularity;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.LoggingEmitter;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.metrics.MonitorScheduler;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.MapCache;
import io.druid.concurrent.Execs;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularity;
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
import io.druid.indexing.test.TestDataSegmentAnnouncer;
import io.druid.indexing.test.TestDataSegmentKiller;
import io.druid.indexing.test.TestDataSegmentPusher;
import io.druid.indexing.test.TestIndexerMetadataStorageCoordinator;
import io.druid.jackson.DefaultObjectMapper;
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
import io.druid.segment.realtime.firehose.EventReceiverFirehoseFactory;
import io.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.metrics.EventReceiverFirehoseRegister;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

@RunWith(Parameterized.class)
public class RealtimeIndexTaskTest
{
  private static final Logger log = new Logger(RealtimeIndexTaskTest.class);
  private static final DruidServerMetadata dummyServer = new DruidServerMetadata(
      "dummy",
      "dummy_host",
      0,
      "historical",
      "dummy_tier",
      0
  );
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

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final boolean buildV9Directly;

  private DateTime now;
  private ListeningExecutorService taskExec;
  private Map<SegmentDescriptor, Pair<Executor, Runnable>> handOffCallbacks;

  @Parameterized.Parameters(name = "buildV9Directly = {0}")
  public static Collection<?> constructorFeeder() throws IOException
  {
    return ImmutableList.of(
        new Object[]{true},
        new Object[]{false}
    );
  }

  public RealtimeIndexTaskTest(boolean buildV9Directly)
  {
    this.buildV9Directly = buildV9Directly;
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

    final EventReceiverFirehoseFactory.EventReceiverFirehose firehose =
        (EventReceiverFirehoseFactory.EventReceiverFirehose) task.getFirehose();

    firehose.addRows(
        ImmutableList.<InputRow>of(
            new MapBasedInputRow(
                now,
                ImmutableList.of("dim1"),
                ImmutableMap.<String, Object>of("dim1", "foo")
            ),
            new MapBasedInputRow(
                now,
                ImmutableList.of("dim2"),
                ImmutableMap.<String, Object>of("dim2", "bar")
            )
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
    Assert.assertEquals(2, countEvents(task));

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
      while (task1.getFirehose() == null) {
        Thread.sleep(50);
      }

      final EventReceiverFirehoseFactory.EventReceiverFirehose firehose =
          (EventReceiverFirehoseFactory.EventReceiverFirehose) task1.getFirehose();

      firehose.addRows(
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
      while (task2.getFirehose() == null) {
        Thread.sleep(50);
      }

      // Do a query, at this point the previous data should be loaded.
      Assert.assertEquals(1, countEvents(task2));

      final EventReceiverFirehoseFactory.EventReceiverFirehose firehose =
          (EventReceiverFirehoseFactory.EventReceiverFirehose) task2.getFirehose();

      firehose.addRows(
          ImmutableList.<InputRow>of(
              new MapBasedInputRow(
                  now,
                  ImmutableList.of("dim2"),
                  ImmutableMap.<String, Object>of("dim2", "bar")
              )
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
      Assert.assertEquals(2, countEvents(task2));

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

  @Test(timeout = 10000L)
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

      final EventReceiverFirehoseFactory.EventReceiverFirehose firehose =
          (EventReceiverFirehoseFactory.EventReceiverFirehose) task1.getFirehose();

      firehose.addRows(
          ImmutableList.<InputRow>of(
              new MapBasedInputRow(
                  now,
                  ImmutableList.of("dim1"),
                  ImmutableMap.<String, Object>of("dim1", "foo")
              )
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
      Assert.assertEquals(1, countEvents(task1));

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
      final EventReceiverFirehoseFactory.EventReceiverFirehose firehose =
          (EventReceiverFirehoseFactory.EventReceiverFirehose) task2.getFirehose();

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
      Assert.assertEquals(TaskStatus.Status.SUCCESS, taskStatus.getStatusCode());
    }
  }

  @Test(timeout = 10000L)
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

      final EventReceiverFirehoseFactory.EventReceiverFirehose firehose =
          (EventReceiverFirehoseFactory.EventReceiverFirehose) task1.getFirehose();

      firehose.addRows(
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
            Granularity.DAY.truncate(now),
            Granularity.DAY.increment(Granularity.DAY.truncate(now))
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

  @Test(timeout = 10000L)
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
    ObjectMapper objectMapper = new DefaultObjectMapper();
    DataSchema dataSchema = new DataSchema(
        "test_ds",
        null,
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularity.DAY, QueryGranularity.NONE, null),
        objectMapper
    );
    RealtimeIOConfig realtimeIOConfig = new RealtimeIOConfig(
        new EventReceiverFirehoseFactory(
            "foo",
            100,
            new NoopChatHandlerProvider(),
            objectMapper,
            null,
            new EventReceiverFirehoseRegister()
        ),
        null,
        null
    );
    RealtimeTuningConfig realtimeTuningConfig = new RealtimeTuningConfig(
        1000,
        new Period("P1Y"),
        new Period("PT10M"),
        null,
        null,
        null,
        null,
        null,
        null,
        buildV9Directly,
        0, 0
    );
    return new RealtimeIndexTask(
        taskId,
        null,
        new FireDepartment(dataSchema, realtimeIOConfig, realtimeTuningConfig),
        null
    );
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
        emitter
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
          public void stop()
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

  public long countEvents(final Task task) throws Exception
  {
    // Do a query.
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_ds")
                                  .aggregators(
                                      ImmutableList.<AggregatorFactory>of(
                                          new LongSumAggregatorFactory("rows", "rows")
                                      )
                                  ).granularity(QueryGranularity.ALL)
                                  .intervals("2000/3000")
                                  .build();

    ArrayList<Result<TimeseriesResultValue>> results = Sequences.toList(
        task.getQueryRunner(query).run(query, ImmutableMap.<String, Object>of()),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    return results.get(0).getValue().getLongMetric("rows");
  }
}

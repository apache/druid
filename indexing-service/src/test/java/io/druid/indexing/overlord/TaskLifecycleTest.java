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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.cache.MapCache;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.discovery.DataNodeService;
import io.druid.discovery.DruidNodeAnnouncer;
import io.druid.discovery.LookupNodeService;
import io.druid.indexer.TaskState;
import io.druid.indexing.common.SegmentLoaderFactory;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.LocalTaskActionClientFactory;
import io.druid.indexing.common.actions.LockListAction;
import io.druid.indexing.common.actions.SegmentInsertAction;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.task.AbstractFixedIntervalTask;
import io.druid.indexing.common.task.IndexTask;
import io.druid.indexing.common.task.IndexTask.IndexIOConfig;
import io.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import io.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import io.druid.indexing.common.task.KillTask;
import io.druid.indexing.common.task.NoopTestTaskFileWriter;
import io.druid.indexing.common.task.RealtimeIndexTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.common.task.TaskResource;
import io.druid.indexing.overlord.config.TaskQueueConfig;
import io.druid.indexing.overlord.supervisor.SupervisorManager;
import io.druid.indexing.test.TestIndexerMetadataStorageCoordinator;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.metrics.Monitor;
import io.druid.java.util.metrics.MonitorScheduler;
import io.druid.metadata.DerbyMetadataStorageActionHandlerFactory;
import io.druid.metadata.TestDerbyConnector;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentArchiver;
import io.druid.segment.loading.DataSegmentMover;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentKiller;
import io.druid.segment.loading.LocalDataSegmentPusherConfig;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoaderLocalCacheManager;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentTest;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.server.coordination.DataSegmentServerAnnouncer;
import io.druid.server.coordination.ServerType;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.server.security.AuthTestUtils;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

@RunWith(Parameterized.class)
public class TaskLifecycleTest
{
  private static final ObjectMapper MAPPER;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  private static final TestUtils TEST_UTILS;

  static {
    TEST_UTILS = new TestUtils();
    MAPPER = TEST_UTILS.getTestObjectMapper();
    INDEX_MERGER_V9 = TEST_UTILS.getTestIndexMergerV9();
    INDEX_IO = TEST_UTILS.getTestIndexIO();
  }

  private static final String HEAP_TASK_STORAGE = "HeapMemoryTaskStorage";
  private static final String METADATA_TASK_STORAGE = "MetadataTaskStorage";

  @Parameterized.Parameters(name = "taskStorageType={0}")
  public static Collection<String[]> constructFeed()
  {
    return Arrays.asList(new String[][]{{HEAP_TASK_STORAGE}, {METADATA_TASK_STORAGE}});
  }

  public TaskLifecycleTest(String taskStorageType)
  {
    this.taskStorageType = taskStorageType;
  }

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final Ordering<DataSegment> byIntervalOrdering = new Ordering<DataSegment>()
  {
    @Override
    public int compare(DataSegment dataSegment, DataSegment dataSegment2)
    {
      return Comparators.intervalsByStartThenEnd().compare(dataSegment.getInterval(), dataSegment2.getInterval());
    }
  };
  private static DateTime now = DateTimes.nowUtc();

  private static final Iterable<InputRow> realtimeIdxTaskInputRows = ImmutableList.of(
      IR(now.toString("YYYY-MM-dd'T'HH:mm:ss"), "test_dim1", "test_dim2", 1.0f),
      IR(now.plus(new Period(Hours.ONE)).toString("YYYY-MM-dd'T'HH:mm:ss"), "test_dim1", "test_dim2", 2.0f),
      IR(now.plus(new Period(Hours.TWO)).toString("YYYY-MM-dd'T'HH:mm:ss"), "test_dim1", "test_dim2", 3.0f)
  );

  private static final Iterable<InputRow> IdxTaskInputRows = ImmutableList.of(
      IR("2010-01-01T01", "x", "y", 1),
      IR("2010-01-01T01", "x", "z", 1),
      IR("2010-01-02T01", "a", "b", 2),
      IR("2010-01-02T01", "a", "c", 1)
  );

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private final String taskStorageType;

  private ObjectMapper mapper;
  private TaskStorageQueryAdapter tsqa = null;
  private TaskStorage taskStorage = null;
  private TaskLockbox taskLockbox = null;
  private TaskQueue taskQueue = null;
  private TaskRunner taskRunner = null;
  private TestIndexerMetadataStorageCoordinator mdc = null;
  private TaskActionClientFactory tac = null;
  private TaskToolboxFactory tb = null;
  private IndexSpec indexSpec;
  private QueryRunnerFactoryConglomerate queryRunnerFactoryConglomerate;
  private MonitorScheduler monitorScheduler;
  private ServiceEmitter emitter;
  private TaskQueueConfig tqc;
  private TaskConfig taskConfig;
  private DataSegmentPusher dataSegmentPusher;

  private int pushedSegments;
  private int announcedSinks;
  private SegmentHandoffNotifierFactory handoffNotifierFactory;
  private Map<SegmentDescriptor, Pair<Executor, Runnable>> handOffCallbacks;

  private static CountDownLatch publishCountDown;

  private static ServiceEmitter newMockEmitter()
  {
    return new NoopServiceEmitter();
  }

  private static InputRow IR(String dt, String dim1, String dim2, float met)
  {
    return new MapBasedInputRow(
        DateTimes.of(dt).getMillis(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableMap.<String, Object>of(
            "dim1", dim1,
            "dim2", dim2,
            "met", met
        )
    );
  }

  private static class MockExceptionalFirehoseFactory implements FirehoseFactory
  {
    @Override
    public Firehose connect(InputRowParser parser, File temporaryDirectory)
    {
      return new Firehose()
      {
        @Override
        public boolean hasMore()
        {
          return true;
        }

        @Nullable
        @Override
        public InputRow nextRow()
        {
          throw new RuntimeException("HA HA HA");
        }

        @Override
        public Runnable commit()
        {
          return new Runnable()
          {
            @Override
            public void run()
            {

            }
          };
        }

        @Override
        public void close()
        {

        }
      };
    }
  }

  private static class MockFirehoseFactory implements FirehoseFactory
  {
    @JsonProperty
    private boolean usedByRealtimeIdxTask;

    @JsonCreator
    public MockFirehoseFactory(@JsonProperty("usedByRealtimeIdxTask") boolean usedByRealtimeIdxTask)
    {
      this.usedByRealtimeIdxTask = usedByRealtimeIdxTask;
    }

    @Override
    public Firehose connect(InputRowParser parser, File temporaryDirectory)
    {
      final Iterator<InputRow> inputRowIterator = usedByRealtimeIdxTask
                                                  ? realtimeIdxTaskInputRows.iterator()
                                                  : IdxTaskInputRows.iterator();

      return new Firehose()
      {
        @Override
        public boolean hasMore()
        {
          return inputRowIterator.hasNext();
        }

        @Nullable
        @Override
        public InputRow nextRow()
        {
          return inputRowIterator.next();
        }

        @Override
        public Runnable commit()
        {
          return new Runnable()
          {
            @Override
            public void run()
            {

            }
          };
        }

        @Override
        public void close()
        {

        }
      };
    }
  }

  @Before
  public void setUp() throws Exception
  {
    // mock things
    queryRunnerFactoryConglomerate = EasyMock.createStrictMock(QueryRunnerFactoryConglomerate.class);
    monitorScheduler = EasyMock.createStrictMock(MonitorScheduler.class);

    // initialize variables
    announcedSinks = 0;
    pushedSegments = 0;
    indexSpec = new IndexSpec();
    emitter = newMockEmitter();
    EmittingLogger.registerEmitter(emitter);
    mapper = TEST_UTILS.getTestObjectMapper();
    handOffCallbacks = new ConcurrentHashMap<>();

    // Set up things, the order does matter as if it is messed up then the setUp
    // should fail because of the Precondition checks in the respective setUp methods
    // For creating a customized TaskQueue see testRealtimeIndexTaskFailure test

    taskStorage = setUpTaskStorage();

    handoffNotifierFactory = setUpSegmentHandOffNotifierFactory();

    dataSegmentPusher = setUpDataSegmentPusher();

    mdc = setUpMetadataStorageCoordinator();

    tb = setUpTaskToolboxFactory(dataSegmentPusher, handoffNotifierFactory, mdc);

    taskRunner = setUpThreadPoolTaskRunner(tb);

    taskQueue = setUpTaskQueue(taskStorage, taskRunner);
  }

  private TaskStorage setUpTaskStorage()
  {
    Preconditions.checkNotNull(mapper);
    Preconditions.checkNotNull(derbyConnectorRule);

    TaskStorage taskStorage;

    switch (taskStorageType) {
      case HEAP_TASK_STORAGE: {
        taskStorage = new HeapMemoryTaskStorage(
            new TaskStorageConfig(null)
            {
            }
        );
        break;
      }

      case METADATA_TASK_STORAGE: {
        TestDerbyConnector testDerbyConnector = derbyConnectorRule.getConnector();
        mapper.registerSubtypes(
            new NamedType(MockExceptionalFirehoseFactory.class, "mockExcepFirehoseFactory"),
            new NamedType(MockFirehoseFactory.class, "mockFirehoseFactory")
        );
        testDerbyConnector.createTaskTables();
        testDerbyConnector.createSegmentTable();
        taskStorage = new MetadataTaskStorage(
            testDerbyConnector,
            new TaskStorageConfig(null),
            new DerbyMetadataStorageActionHandlerFactory(
                testDerbyConnector,
                derbyConnectorRule.metadataTablesConfigSupplier().get(),
                mapper
            )
        );
        break;
      }

      default: {
        throw new RE("Unknown task storage type [%s]", taskStorageType);
      }
    }
    tsqa = new TaskStorageQueryAdapter(taskStorage);
    return taskStorage;
  }

  private SegmentHandoffNotifierFactory setUpSegmentHandOffNotifierFactory()
  {
    Preconditions.checkNotNull(handOffCallbacks);

    return new SegmentHandoffNotifierFactory()
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
  }

  private DataSegmentPusher setUpDataSegmentPusher()
  {
    return new DataSegmentPusher()
    {
      @Override
      public String getPathForHadoop()
      {
        throw new UnsupportedOperationException();
      }

      @Deprecated
      @Override
      public String getPathForHadoop(String dataSource)
      {
        return getPathForHadoop();
      }

      @Override
      public DataSegment push(File file, DataSegment segment, boolean useUniquePath)
      {
        pushedSegments++;
        return segment;
      }

      @Override
      public Map<String, Object> makeLoadSpec(URI uri)
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  private TestIndexerMetadataStorageCoordinator setUpMetadataStorageCoordinator()
  {
    return new TestIndexerMetadataStorageCoordinator()
    {
      @Override
      public Set<DataSegment> announceHistoricalSegments(Set<DataSegment> segments)
      {
        Set<DataSegment> retVal = super.announceHistoricalSegments(segments);
        if (publishCountDown != null) {
          publishCountDown.countDown();
        }
        return retVal;
      }
    };
  }

  private TaskToolboxFactory setUpTaskToolboxFactory(
      DataSegmentPusher dataSegmentPusher,
      SegmentHandoffNotifierFactory handoffNotifierFactory,
      TestIndexerMetadataStorageCoordinator mdc
  ) throws IOException
  {
    Preconditions.checkNotNull(queryRunnerFactoryConglomerate);
    Preconditions.checkNotNull(monitorScheduler);
    Preconditions.checkNotNull(taskStorage);
    Preconditions.checkNotNull(emitter);

    taskLockbox = new TaskLockbox(taskStorage);
    tac = new LocalTaskActionClientFactory(taskStorage, new TaskActionToolbox(taskLockbox, mdc, emitter, EasyMock.createMock(
        SupervisorManager.class)));
    File tmpDir = temporaryFolder.newFolder();
    taskConfig = new TaskConfig(tmpDir.toString(), null, null, 50000, null, false, null, null);

    SegmentLoaderConfig segmentLoaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return Lists.newArrayList();
      }
    };
    return new TaskToolboxFactory(
        taskConfig,
        tac,
        emitter,
        dataSegmentPusher,
        new LocalDataSegmentKiller(new LocalDataSegmentPusherConfig()),
        new DataSegmentMover()
        {
          @Override
          public DataSegment move(DataSegment dataSegment, Map<String, Object> targetLoadSpec)
          {
            return dataSegment;
          }
        },
        new DataSegmentArchiver()
        {
          @Override
          public DataSegment archive(DataSegment segment)
          {
            return segment;
          }

          @Override
          public DataSegment restore(DataSegment segment)
          {
            return segment;
          }
        },
        new DataSegmentAnnouncer()
        {
          @Override
          public void announceSegment(DataSegment segment)
          {
            announcedSinks++;
          }

          @Override
          public void unannounceSegment(DataSegment segment)
          {

          }

          @Override
          public void announceSegments(Iterable<DataSegment> segments)
          {

          }

          @Override
          public void unannounceSegments(Iterable<DataSegment> segments)
          {

          }
        }, // segment announcer
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
        handoffNotifierFactory,
        () -> queryRunnerFactoryConglomerate, // query runner factory conglomerate corporation unionized collective
        MoreExecutors.sameThreadExecutor(), // query executor service
        monitorScheduler, // monitor scheduler
        new SegmentLoaderFactory(
            new SegmentLoaderLocalCacheManager(null, segmentLoaderConfig, new DefaultObjectMapper())
        ),
        MAPPER,
        INDEX_IO,
        MapCache.create(0),
        FireDepartmentTest.NO_CACHE_CONFIG,
        INDEX_MERGER_V9,
        EasyMock.createNiceMock(DruidNodeAnnouncer.class),
        EasyMock.createNiceMock(DruidNode.class),
        new LookupNodeService("tier"),
        new DataNodeService("tier", 1000, ServerType.INDEXER_EXECUTOR, 0),
        new NoopTestTaskFileWriter()
    );
  }

  private TaskRunner setUpThreadPoolTaskRunner(TaskToolboxFactory tb)
  {
    Preconditions.checkNotNull(taskConfig);
    Preconditions.checkNotNull(emitter);

    return new ThreadPoolTaskRunner(
        tb,
        taskConfig,
        emitter,
        new DruidNode("dummy", "dummy", 10000, null, true, false),
        new ServerConfig()
    );
  }

  private TaskQueue setUpTaskQueue(TaskStorage ts, TaskRunner tr) throws Exception
  {
    Preconditions.checkNotNull(taskLockbox);
    Preconditions.checkNotNull(tac);
    Preconditions.checkNotNull(emitter);

    tqc = mapper.readValue(
        "{\"startDelay\":\"PT0S\", \"restartDelay\":\"PT1S\", \"storageSyncRate\":\"PT0.5S\"}",
        TaskQueueConfig.class
    );

    return new TaskQueue(tqc, ts, tr, tac, taskLockbox, emitter);
  }

  @After
  public void tearDown()
  {
    if (taskQueue.isActive()) {
      taskQueue.stop();
    }
  }

  @Test
  public void testIndexTask() throws Exception
  {
    final Task indexTask = new IndexTask(
        null,
        null,
        new IndexIngestionSpec(
            new DataSchema(
                "foo",
                null,
                new AggregatorFactory[]{new DoubleSumAggregatorFactory("met", "met")},
                new UniformGranularitySpec(
                    Granularities.DAY,
                    null,
                    ImmutableList.of(Intervals.of("2010-01-01/P2D"))
                ),
                null,
                mapper
            ),
            new IndexIOConfig(new MockFirehoseFactory(false), false),
            new IndexTuningConfig(
                10000,
                10,
                null,
                null,
                null,
                null,
                indexSpec,
                3,
                true,
                true,
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    final Optional<TaskStatus> preRunTaskStatus = tsqa.getStatus(indexTask.getId());
    Assert.assertTrue("pre run task status not present", !preRunTaskStatus.isPresent());

    final TaskStatus mergedStatus = runTask(indexTask);
    final TaskStatus status = taskStorage.getStatus(indexTask.getId()).get();
    final List<DataSegment> publishedSegments = byIntervalOrdering.sortedCopy(mdc.getPublished());
    final List<DataSegment> loggedSegments = byIntervalOrdering.sortedCopy(tsqa.getInsertedSegments(indexTask.getId()));

    Assert.assertEquals("statusCode", TaskState.SUCCESS, status.getStatusCode());
    Assert.assertEquals("merged statusCode", TaskState.SUCCESS, mergedStatus.getStatusCode());
    Assert.assertEquals("segments logged vs published", loggedSegments, publishedSegments);
    Assert.assertEquals("num segments published", 2, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());

    Assert.assertEquals("segment1 datasource", "foo", publishedSegments.get(0).getDataSource());
    Assert.assertEquals("segment1 interval", Intervals.of("2010-01-01/P1D"), publishedSegments.get(0).getInterval());
    Assert.assertEquals(
        "segment1 dimensions",
        ImmutableList.of("dim1", "dim2"),
        publishedSegments.get(0).getDimensions()
    );
    Assert.assertEquals("segment1 metrics", ImmutableList.of("met"), publishedSegments.get(0).getMetrics());

    Assert.assertEquals("segment2 datasource", "foo", publishedSegments.get(1).getDataSource());
    Assert.assertEquals("segment2 interval", Intervals.of("2010-01-02/P1D"), publishedSegments.get(1).getInterval());
    Assert.assertEquals(
        "segment2 dimensions",
        ImmutableList.of("dim1", "dim2"),
        publishedSegments.get(1).getDimensions()
    );
    Assert.assertEquals("segment2 metrics", ImmutableList.of("met"), publishedSegments.get(1).getMetrics());
  }

  @Test
  public void testIndexTaskFailure() throws Exception
  {
    final Task indexTask = new IndexTask(
        null,
        null,
        new IndexIngestionSpec(
            new DataSchema(
                "foo",
                null,
                new AggregatorFactory[]{new DoubleSumAggregatorFactory("met", "met")},
                new UniformGranularitySpec(
                    Granularities.DAY,
                    null,
                    ImmutableList.of(Intervals.of("2010-01-01/P1D"))
                ),
                null,
                mapper
            ),
            new IndexIOConfig(new MockExceptionalFirehoseFactory(), false),
            new IndexTuningConfig(
                10000,
                10,
                null,
                null,
                null,
                null,
                indexSpec,
                3,
                true,
                true,
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    final TaskStatus status = runTask(indexTask);

    Assert.assertEquals("statusCode", TaskState.FAILED, status.getStatusCode());
    Assert.assertEquals("num segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testKillTask() throws Exception
  {
    final File tmpSegmentDir = temporaryFolder.newFolder();

    List<DataSegment> expectedUnusedSegments = Lists.transform(
        ImmutableList.<String>of(
            "2011-04-01/2011-04-02",
            "2011-04-02/2011-04-03",
            "2011-04-04/2011-04-05"
        ), new Function<String, DataSegment>()
        {
          @Override
          public DataSegment apply(String input)
          {
            final Interval interval = Intervals.of(input);
            try {
              return DataSegment.builder()
                                .dataSource("test_kill_task")
                                .interval(interval)
                                .loadSpec(
                                    ImmutableMap.<String, Object>of(
                                        "type",
                                        "local",
                                        "path",
                                        tmpSegmentDir.getCanonicalPath()
                                        + "/druid/localStorage/wikipedia/"
                                        + interval.getStart()
                                        + "-"
                                        + interval.getEnd()
                                        + "/"
                                        + "2011-04-6T16:52:46.119-05:00"
                                        + "/0/index.zip"
                                    )
                                )
                                .version("2011-04-6T16:52:46.119-05:00")
                                .dimensions(ImmutableList.<String>of())
                                .metrics(ImmutableList.<String>of())
                                .shardSpec(NoneShardSpec.instance())
                                .binaryVersion(9)
                                .size(0)
                                .build();
            }
            catch (IOException e) {
              throw new ISE(e, "Error creating segments");
            }
          }
        }
    );

    mdc.setUnusedSegments(expectedUnusedSegments);

    // manually create local segments files
    List<File> segmentFiles = Lists.newArrayList();
    for (DataSegment segment : mdc.getUnusedSegmentsForInterval("test_kill_task", Intervals.of("2011-04-01/P4D"))) {
      File file = new File((String) segment.getLoadSpec().get("path"));
      file.mkdirs();
      segmentFiles.add(file);
    }

    final Task killTask = new KillTask(null, "test_kill_task", Intervals.of("2011-04-01/P4D"), null);

    final TaskStatus status = runTask(killTask);
    Assert.assertEquals("merged statusCode", TaskState.SUCCESS, status.getStatusCode());
    Assert.assertEquals("num segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 3, mdc.getNuked().size());
    Assert.assertTrue(
        "expected unused segments get killed",
        expectedUnusedSegments.containsAll(mdc.getNuked()) && mdc.getNuked().containsAll(
            expectedUnusedSegments
        )
    );

    for (File file : segmentFiles) {
      Assert.assertFalse("unused segments files get deleted", file.exists());
    }
  }

  @Test
  public void testRealtimeishTask() throws Exception
  {
    final Task rtishTask = new RealtimeishTask();
    final TaskStatus status = runTask(rtishTask);

    Assert.assertEquals("statusCode", TaskState.SUCCESS, status.getStatusCode());
    Assert.assertEquals("num segments published", 2, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testNoopTask() throws Exception
  {
    final Task noopTask = new DefaultObjectMapper().readValue(
        "{\"type\":\"noop\", \"runTime\":\"100\"}\"",
        Task.class
    );
    final TaskStatus status = runTask(noopTask);

    Assert.assertEquals("statusCode", TaskState.SUCCESS, status.getStatusCode());
    Assert.assertEquals("num segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testNeverReadyTask() throws Exception
  {
    final Task neverReadyTask = new DefaultObjectMapper().readValue(
        "{\"type\":\"noop\", \"isReadyResult\":\"exception\"}\"",
        Task.class
    );
    final TaskStatus status = runTask(neverReadyTask);

    Assert.assertEquals("statusCode", TaskState.FAILED, status.getStatusCode());
    Assert.assertEquals("num segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testSimple() throws Exception
  {
    final Task task = new AbstractFixedIntervalTask(
        "id1",
        "id1",
        new TaskResource("id1", 1),
        "ds",
        Intervals.of("2012-01-01/P1D"),
        null
    )
    {
      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public TaskStatus run(TaskToolbox toolbox) throws Exception
      {
        final TaskLock myLock = Iterables.getOnlyElement(
            toolbox.getTaskActionClient()
                   .submit(new LockListAction())
        );

        final DataSegment segment = DataSegment.builder()
                                               .dataSource("ds")
                                               .interval(Intervals.of("2012-01-01/P1D"))
                                               .version(myLock.getVersion())
                                               .build();

        toolbox.getTaskActionClient().submit(new SegmentInsertAction(ImmutableSet.of(segment)));
        return TaskStatus.success(getId());
      }
    };

    final TaskStatus status = runTask(task);

    Assert.assertEquals("statusCode", TaskState.SUCCESS, status.getStatusCode());
    Assert.assertEquals("segments published", 1, mdc.getPublished().size());
    Assert.assertEquals("segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testBadInterval() throws Exception
  {
    final Task task = new AbstractFixedIntervalTask("id1", "id1", "ds", Intervals.of("2012-01-01/P1D"), null)
    {
      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public TaskStatus run(TaskToolbox toolbox) throws Exception
      {
        final TaskLock myLock = Iterables.getOnlyElement(toolbox.getTaskActionClient().submit(new LockListAction()));

        final DataSegment segment = DataSegment.builder()
                                               .dataSource("ds")
                                               .interval(Intervals.of("2012-01-01/P2D"))
                                               .version(myLock.getVersion())
                                               .build();

        toolbox.getTaskActionClient().submit(new SegmentInsertAction(ImmutableSet.of(segment)));
        return TaskStatus.success(getId());
      }
    };

    final TaskStatus status = runTask(task);

    Assert.assertEquals("statusCode", TaskState.FAILED, status.getStatusCode());
    Assert.assertEquals("segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testBadVersion() throws Exception
  {
    final Task task = new AbstractFixedIntervalTask("id1", "id1", "ds", Intervals.of("2012-01-01/P1D"), null)
    {
      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public TaskStatus run(TaskToolbox toolbox) throws Exception
      {
        final TaskLock myLock = Iterables.getOnlyElement(toolbox.getTaskActionClient().submit(new LockListAction()));

        final DataSegment segment = DataSegment.builder()
                                               .dataSource("ds")
                                               .interval(Intervals.of("2012-01-01/P1D"))
                                               .version(myLock.getVersion() + "1!!!1!!")
                                               .build();

        toolbox.getTaskActionClient().submit(new SegmentInsertAction(ImmutableSet.of(segment)));
        return TaskStatus.success(getId());
      }
    };

    final TaskStatus status = runTask(task);

    Assert.assertEquals("statusCode", TaskState.FAILED, status.getStatusCode());
    Assert.assertEquals("segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("segments nuked", 0, mdc.getNuked().size());
  }

  @Test(timeout = 60_000L)
  public void testRealtimeIndexTask() throws Exception
  {
    publishCountDown = new CountDownLatch(1);
    monitorScheduler.addMonitor(EasyMock.anyObject(Monitor.class));
    EasyMock.expectLastCall().atLeastOnce();
    monitorScheduler.removeMonitor(EasyMock.anyObject(Monitor.class));
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(monitorScheduler, queryRunnerFactoryConglomerate);

    RealtimeIndexTask realtimeIndexTask = newRealtimeIndexTask();
    final String taskId = realtimeIndexTask.getId();

    taskQueue.start();
    taskQueue.add(realtimeIndexTask);
    //wait for task to process events and publish segment
    publishCountDown.await();

    // Realtime Task has published the segment, simulate loading of segment to a historical node so that task finishes with SUCCESS status
    Assert.assertEquals(1, handOffCallbacks.size());
    Pair<Executor, Runnable> executorRunnablePair = Iterables.getOnlyElement(handOffCallbacks.values());
    executorRunnablePair.lhs.execute(executorRunnablePair.rhs);
    handOffCallbacks.clear();

    // Wait for realtime index task to handle callback in plumber and succeed
    while (tsqa.getStatus(taskId).get().isRunnable()) {
      Thread.sleep(10);
    }

    Assert.assertTrue("Task should be in Success state", tsqa.getStatus(taskId).get().isSuccess());

    Assert.assertEquals(1, announcedSinks);
    Assert.assertEquals(1, pushedSegments);
    Assert.assertEquals(1, mdc.getPublished().size());
    DataSegment segment = mdc.getPublished().iterator().next();
    Assert.assertEquals("test_ds", segment.getDataSource());
    Assert.assertEquals(ImmutableList.of("dim1", "dim2"), segment.getDimensions());
    Assert.assertEquals(
        Intervals.of(now.toString("YYYY-MM-dd") + "/" + now.plusDays(1).toString("YYYY-MM-dd")),
        segment.getInterval()
    );
    Assert.assertEquals(ImmutableList.of("count"), segment.getMetrics());
    EasyMock.verify(monitorScheduler, queryRunnerFactoryConglomerate);
  }

  @Test(timeout = 60_000L)
  public void testRealtimeIndexTaskFailure() throws Exception
  {
    dataSegmentPusher = new DataSegmentPusher()
    {
      @Deprecated
      @Override
      public String getPathForHadoop(String s)
      {
        return getPathForHadoop();
      }

      @Override
      public String getPathForHadoop()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public DataSegment push(File file, DataSegment dataSegment, boolean useUniquePath)
      {
        throw new RuntimeException("FAILURE");
      }

      @Override
      public Map<String, Object> makeLoadSpec(URI uri)
      {
        throw new UnsupportedOperationException();
      }
    };

    tb = setUpTaskToolboxFactory(dataSegmentPusher, handoffNotifierFactory, mdc);

    taskRunner = setUpThreadPoolTaskRunner(tb);

    taskQueue = setUpTaskQueue(taskStorage, taskRunner);

    monitorScheduler.addMonitor(EasyMock.anyObject(Monitor.class));
    EasyMock.expectLastCall().atLeastOnce();
    monitorScheduler.removeMonitor(EasyMock.anyObject(Monitor.class));
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(monitorScheduler, queryRunnerFactoryConglomerate);

    RealtimeIndexTask realtimeIndexTask = newRealtimeIndexTask();
    final String taskId = realtimeIndexTask.getId();

    taskQueue.start();
    taskQueue.add(realtimeIndexTask);

    // Wait for realtime index task to fail
    while (tsqa.getStatus(taskId).get().isRunnable()) {
      Thread.sleep(10);
    }

    Assert.assertTrue("Task should be in Failure state", tsqa.getStatus(taskId).get().isFailure());

    EasyMock.verify(monitorScheduler, queryRunnerFactoryConglomerate);
  }

  @Test
  public void testResumeTasks() throws Exception
  {
    final Task indexTask = new IndexTask(
        null,
        null,
        new IndexIngestionSpec(
            new DataSchema(
                "foo",
                null,
                new AggregatorFactory[]{new DoubleSumAggregatorFactory("met", "met")},
                new UniformGranularitySpec(
                    Granularities.DAY,
                    null,
                    ImmutableList.of(Intervals.of("2010-01-01/P2D"))
                ),
                null,
                mapper
            ),
            new IndexIOConfig(new MockFirehoseFactory(false), false),
            new IndexTuningConfig(
                10000,
                10,
                null,
                null,
                null,
                null,
                indexSpec,
                null,
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    final long startTime = System.currentTimeMillis();

    // manually insert the task into TaskStorage, waiting for TaskQueue to sync from storage
    taskQueue.start();
    taskStorage.insert(indexTask, TaskStatus.running(indexTask.getId()));

    while (tsqa.getStatus(indexTask.getId()).get().isRunnable()) {
      if (System.currentTimeMillis() > startTime + 10 * 1000) {
        throw new ISE("Where did the task go?!: %s", indexTask.getId());
      }

      Thread.sleep(100);
    }

    final TaskStatus status = taskStorage.getStatus(indexTask.getId()).get();
    final List<DataSegment> publishedSegments = byIntervalOrdering.sortedCopy(mdc.getPublished());
    final List<DataSegment> loggedSegments = byIntervalOrdering.sortedCopy(tsqa.getInsertedSegments(indexTask.getId()));

    Assert.assertEquals("statusCode", TaskState.SUCCESS, status.getStatusCode());
    Assert.assertEquals("segments logged vs published", loggedSegments, publishedSegments);
    Assert.assertEquals("num segments published", 2, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());

    Assert.assertEquals("segment1 datasource", "foo", publishedSegments.get(0).getDataSource());
    Assert.assertEquals("segment1 interval", Intervals.of("2010-01-01/P1D"), publishedSegments.get(0).getInterval());
    Assert.assertEquals(
        "segment1 dimensions",
        ImmutableList.of("dim1", "dim2"),
        publishedSegments.get(0).getDimensions()
    );
    Assert.assertEquals("segment1 metrics", ImmutableList.of("met"), publishedSegments.get(0).getMetrics());

    Assert.assertEquals("segment2 datasource", "foo", publishedSegments.get(1).getDataSource());
    Assert.assertEquals("segment2 interval", Intervals.of("2010-01-02/P1D"), publishedSegments.get(1).getInterval());
    Assert.assertEquals(
        "segment2 dimensions",
        ImmutableList.of("dim1", "dim2"),
        publishedSegments.get(1).getDimensions()
    );
    Assert.assertEquals("segment2 metrics", ImmutableList.of("met"), publishedSegments.get(1).getMetrics());
  }

  private TaskStatus runTask(final Task task) throws Exception
  {
    final Task dummyTask = new DefaultObjectMapper().readValue(
        "{\"type\":\"noop\", \"isReadyResult\":\"exception\"}\"",
        Task.class
    );
    final long startTime = System.currentTimeMillis();

    Preconditions.checkArgument(!task.getId().equals(dummyTask.getId()));

    // Since multiple tasks can be run in a single unit test using runTask(), hence this check and synchronization
    synchronized (this) {
      if (!taskQueue.isActive()) {
        taskQueue.start();
      }
    }
    taskQueue.add(dummyTask);
    taskQueue.add(task);

    TaskStatus retVal = null;

    for (final String taskId : ImmutableList.of(dummyTask.getId(), task.getId())) {
      try {
        TaskStatus status;
        while ((status = tsqa.getStatus(taskId).get()).isRunnable()) {
          if (System.currentTimeMillis() > startTime + 10 * 1000) {
            throw new ISE("Where did the task go?!: %s", task.getId());
          }

          Thread.sleep(100);
        }
        if (taskId.equals(task.getId())) {
          retVal = status;
        }
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    return retVal;
  }

  private RealtimeIndexTask newRealtimeIndexTask()
  {
    String taskId = StringUtils.format("rt_task_%s", System.currentTimeMillis());
    DataSchema dataSchema = new DataSchema(
        "test_ds",
        null,
        new AggregatorFactory[]{new LongSumAggregatorFactory("count", "rows")},
        new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
        null,
        mapper
    );
    RealtimeIOConfig realtimeIOConfig = new RealtimeIOConfig(
        new MockFirehoseFactory(true),
        null,
        // PlumberSchool - Realtime Index Task always uses RealtimePlumber which is hardcoded in RealtimeIndexTask class
        null
    );
    RealtimeTuningConfig realtimeTuningConfig = new RealtimeTuningConfig(
        1000,
        null,
        new Period("P1Y"),
        null, //default window period of 10 minutes
        null, // base persist dir ignored by Realtime Index task
        null,
        null,
        null,
        null,
        null,
        null,
        0,
        0,
        null,
        null,
        null,
        null,
        null
    );
    FireDepartment fireDepartment = new FireDepartment(dataSchema, realtimeIOConfig, realtimeTuningConfig);
    return new RealtimeIndexTask(
        taskId,
        new TaskResource(taskId, 1),
        fireDepartment,
        null
    );
  }
}

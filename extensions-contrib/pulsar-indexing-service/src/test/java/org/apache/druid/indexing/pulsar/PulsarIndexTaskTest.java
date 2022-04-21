package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.SingleFileTaskReportFileWriter;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.LocalTaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionToolbox;
import org.apache.druid.indexing.common.actions.TaskAuditLogConfig;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.MetadataTaskStorage;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.pulsar.supervisor.PulsarSupervisorIOConfig;
import org.apache.druid.indexing.pulsar.supervisor.PulsarSupervisorTuningConfig;
import org.apache.druid.indexing.pulsar.test.Fixtures;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTestBase;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.test.TestDataSegmentAnnouncer;
import org.apache.druid.indexing.test.TestDataSegmentKiller;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.apache.druid.metadata.DerbyMetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.handoff.SegmentHandoffNotifier;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.AuthTestUtils;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@RunWith(Parameterized.class)
public class PulsarIndexTaskTest extends SeekableStreamIndexTaskTestBase
{
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private static final String STREAM = "stream";
  private static final Integer SHARD_ID1 = 1;
  private static final String SHARD_ID0 = "0";
  private static PulsarRecordSupplier recordSupplier;

  private static ServiceEmitter emitter;

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
//        new Object[] {LockGranularity.TIME_CHUNK},
        new Object[] {LockGranularity.TIME_CHUNK}
    );
  }

  static {
    new PulsarIndexingServiceModule().getJacksonModules().stream().forEach(OBJECT_MAPPER::registerModule);
  }
  private AppenderatorsManager appenderatorsManager;
  private long handoffConditionTimeout = 0;
  private boolean reportParseExceptions = false;
  private boolean logParseExceptions = true;
  private Integer maxParseExceptions = null;
  private Integer maxSavedParseExceptions = null;
  private boolean doHandoff = true;
  private Integer maxRowsPerSegment = null;
  private Long maxTotalRows = null;
  private final Period intermediateHandoffPeriod = null;
  private int maxRecordsPerPoll;

  private final Set<Integer> checkpointRequestsHash = new HashSet<>();

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derby = new TestDerbyConnector.DerbyConnectorRule();

  @BeforeClass
  public static void setupClass()
  {
    emitter = new ServiceEmitter(
        "service",
        "host",
        new NoopEmitter()
    );
    emitter.start();
    EmittingLogger.registerEmitter(emitter);
    taskExec = MoreExecutors.listeningDecorator(
        Executors.newCachedThreadPool(
            Execs.makeThreadFactory("pulsar-task-test-%d")
        )
    );
  }

  public PulsarIndexTaskTest(LockGranularity lockGranularity)
  {
    super(lockGranularity);
  }

  @Before
  public void setupTest() throws IOException, InterruptedException
  {
    handoffConditionTimeout = 0;
    reportParseExceptions = false;
    logParseExceptions = true;
    maxParseExceptions = null;
    maxSavedParseExceptions = null;
    doHandoff = true;
    reportsFile = File.createTempFile("PulsarIndexTaskTestReports-" + System.currentTimeMillis(), "json");
    maxRecordsPerPoll = 1;

    recordSupplier = mock(PulsarRecordSupplier.class);
    appenderatorsManager = new TestAppenderatorsManager();
    makeToolboxFactory();
  }

  @After
  public void tearDownTest()
  {
    synchronized (runningTasks) {
      for (Task task : runningTasks) {
        task.stopGracefully(toolboxFactory.build(task).getConfig());
      }

      runningTasks.clear();
    }
    reportsFile.delete();
    destroyToolboxFactory();
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    taskExec.shutdown();
    taskExec.awaitTermination(9999, TimeUnit.DAYS);
    emitter.close();
  }


  private void makeToolboxFactory() throws IOException
  {
    directory = tempFolder.newFolder();
    final TestUtils testUtils = new TestUtils();
    final ObjectMapper objectMapper = testUtils.getTestObjectMapper();

    for (Module module : new PulsarIndexingServiceModule().getJacksonModules()) {
      objectMapper.registerModule(module);
    }


    /**
     * [ERROR]required: java.lang.String,java.lang.String,java.lang.String,java.lang.Integer,java.util.List<java.lang.String>,boolean,org.joda.time.Period,org.joda.time.Period,java.util.List<org.apache.druid.segment.loading.StorageLocationConfig>,boolean,boolean,java.lang.String
     * [ERROR]   found: java.lang.String,java.lang.String,<nulltype>      ,int              ,<nulltype>                      ,boolean,<nulltype>          ,<nulltype>          ,                                                            <nulltype>,boolean,boolean,java.lang.String,boolean
     *
     */

    final TaskConfig taskConfig = new TaskConfig(
        new File(directory, "baseDir").getPath(),
        new File(directory, "baseTaskDir").getPath(),
        null,
        50000,
        null,
        true,
        null,
        null,
        null,
        false,
        false,
        TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name(),
        false
    );
    final TestDerbyConnector derbyConnector = derby.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createPendingSegmentsTable();
    derbyConnector.createSegmentTable();
    derbyConnector.createRulesTable();
    derbyConnector.createConfigTable();
    derbyConnector.createTaskTables();
    derbyConnector.createAuditTable();
    taskStorage = new MetadataTaskStorage(
        derbyConnector,
        new TaskStorageConfig(null),
        new DerbyMetadataStorageActionHandlerFactory(
            derbyConnector,
            derby.metadataTablesConfigSupplier().get(),
            objectMapper
        )
    );
    metadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        testUtils.getTestObjectMapper(),
        derby.metadataTablesConfigSupplier().get(),
        derbyConnector
    );
    taskLockbox = new TaskLockbox(taskStorage, metadataStorageCoordinator);
    final TaskActionToolbox taskActionToolbox = new TaskActionToolbox(
        taskLockbox,
        taskStorage,
        metadataStorageCoordinator,
        emitter,
        new SupervisorManager(null)
        {
          @Override
          public boolean checkPointDataSourceMetadata(
              String supervisorId,
              int taskGroupId,
              @Nullable DataSourceMetadata checkpointMetadata
          )
          {
            LOG.info("Adding checkpoint hash to the set");
            checkpointRequestsHash.add(
                Objects.hash(
                    supervisorId,
                    taskGroupId,
                    checkpointMetadata
                )
            );
            return true;
          }
        }
    );
    final TaskActionClientFactory taskActionClientFactory = new LocalTaskActionClientFactory(
        taskStorage,
        taskActionToolbox,
        new TaskAuditLogConfig(false)
    );

    final SegmentHandoffNotifierFactory handoffNotifierFactory = dataSource -> new SegmentHandoffNotifier()
    {
      @Override
      public boolean registerSegmentHandoffCallback(
          SegmentDescriptor descriptor,
          Executor exec,
          Runnable handOffRunnable
      )
      {
        if (doHandoff) {
          // Simulate immediate handoff
          exec.execute(handOffRunnable);
        }
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
    final LocalDataSegmentPusherConfig dataSegmentPusherConfig = new LocalDataSegmentPusherConfig();
    dataSegmentPusherConfig.storageDirectory = getSegmentDirectory();
    final DataSegmentPusher dataSegmentPusher = new LocalDataSegmentPusher(dataSegmentPusherConfig);

    toolboxFactory = new TaskToolboxFactory(
        taskConfig,
        null, // taskExecutorNode
        taskActionClientFactory,
        emitter,
        dataSegmentPusher,
        new TestDataSegmentKiller(),
        null, // DataSegmentMover
        null, // DataSegmentArchiver
        new TestDataSegmentAnnouncer(),
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
        handoffNotifierFactory,
        this::makeTimeseriesOnlyConglomerate,
        DirectQueryProcessingPool.INSTANCE,
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
        new DataNodeService("tier", 1, ServerType.INDEXER_EXECUTOR, 0),
        new SingleFileTaskReportFileWriter(reportsFile),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new NoopChatHandlerProvider(),
        testUtils.getRowIngestionMetersFactory(),
        new TestAppenderatorsManager(),
        new NoopIndexingServiceClient(),
        null,
        null,
        null
    );
  }

  private QueryRunnerFactoryConglomerate makeTimeseriesOnlyConglomerate()
  {
    return new DefaultQueryRunnerFactoryConglomerate(
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
  }

  @Test(timeout = 60_000L)
  public void testRunAfterDataInserted() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn(1l).anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyLong());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(Fixtures.generateRecords(STREAM)).anyTimes();

    recordSupplier.close();
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getPreviousSeekFailure()).andReturn(null).anyTimes();

    replayAll();

    final PulsarIndexTask task = createTask(
        null,
        new PulsarIndexTaskIOConfig(
            0,
            "sequence0",
            null,
            null,
            new SeekableStreamStartSequenceNumbers<Integer, Long>(STREAM, ImmutableMap.of(SHARD_ID1, 0l),
                ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, 2l)),
            ImmutableMap.of(PulsarSupervisorIOConfig.SERVICE_URL_KEY, "pulsar://localhost"),
            PulsarSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            null,
            null,
            null,
            INPUT_FORMAT
        )
    );
    Assert.assertTrue(task.supportsQueries());

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(2, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0, ImmutableList.of("a")),
            sdd("2009/P1D", 0, ImmutableList.of("b"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new PulsarDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, 1l))
        ),
        newDataSchemaMetadata()
    );
  }

  DataSourceMetadata newDataSchemaMetadata()
  {
    return metadataStorageCoordinator.retrieveDataSourceMetadata(NEW_DATA_SCHEMA.getDataSource());
  }

  private PulsarIndexTask createTask(
      final String taskId,
      final PulsarIndexTaskIOConfig ioConfig
  ) throws JsonProcessingException
  {
    boolean resetOffsetAutomatically = false;
    int maxRowsInMemory = 1000;
    final DataSchema dataSchema = new DataSchema(
        "test_ds",
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(
            Arrays.asList(
                new StringDimensionSchema("dim1"),
                new StringDimensionSchema("dim1t"),
                new StringDimensionSchema("dim2"),
                new LongDimensionSchema("dimLong"),
                new FloatDimensionSchema("dimFloat")
            )
        ),
        new AggregatorFactory[] {
            new DoubleSumAggregatorFactory("met1sum", "met1"),
            new CountAggregatorFactory("rows")
        },
        new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
        null
    );
    final PulsarIndexTaskTuningConfig tuningConfig = new PulsarSupervisorTuningConfig(
        null,
        maxRowsInMemory,
        null,
        null,
        maxRowsPerSegment,
        maxTotalRows,
        new Period("P1Y"),
        null,
        4,
        new IndexSpec(),
        new IndexSpec(),
        true,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        true,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        intermediateHandoffPeriod,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions
    );

    return new TestablePulsarIndexTask(
        taskId,
        null,
        cloneDataSchema(dataSchema),
        tuningConfig,
        ioConfig,
        null
    );
  }

  private static DataSchema cloneDataSchema(final DataSchema dataSchema)
  {
    return new DataSchema(
        dataSchema.getDataSource(),
        dataSchema.getTimestampSpec(),
        dataSchema.getDimensionsSpec(),
        dataSchema.getAggregators(),
        dataSchema.getGranularitySpec(),
        dataSchema.getTransformSpec(),
        dataSchema.getParserMap(),
        OBJECT_MAPPER
    );
  }


  @JsonTypeName("index_pulsar")
  private static class TestablePulsarIndexTask extends PulsarIndexTask
  {
    private PulsarRecordSupplier localSupplier;

    @JsonCreator
    private TestablePulsarIndexTask(
        @JsonProperty("id") String id,
        @JsonProperty("resource") TaskResource taskResource,
        @JsonProperty("dataSchema") DataSchema dataSchema,
        @JsonProperty("tuningConfig") PulsarIndexTaskTuningConfig tuningConfig,
        @JsonProperty("ioConfig") PulsarIndexTaskIOConfig ioConfig,
        @JsonProperty("context") Map<String, Object> context
    )
    {
      super(
          id,
          taskResource,
          dataSchema,
          tuningConfig,
          ioConfig,
          context,
          null
      );
    }

    private void setLocalSupplier(PulsarRecordSupplier recordSupplier)
    {
      this.localSupplier = recordSupplier;
    }

    @Override
    protected PulsarRecordSupplier newTaskRecordSupplier()
    {
      return localSupplier == null ? recordSupplier : localSupplier;
    }
  }


}

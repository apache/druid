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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.LockGranularity;
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
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.MetadataTaskStorage;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.test.TestDataSegmentAnnouncer;
import org.apache.druid.indexing.test.TestDataSegmentKiller;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.apache.druid.metadata.DerbyMetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.handoff.SegmentHandoffNotifier;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.incremental.RowIngestionMetersTotals;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderator;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;
import org.assertj.core.api.Assertions;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public abstract class SeekableStreamIndexTaskTestBase extends EasyMockSupport
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derby = new TestDerbyConnector.DerbyConnectorRule();

  protected static final ObjectMapper OBJECT_MAPPER;
  protected static final DataSchema OLD_DATA_SCHEMA;
  protected static final DataSchema NEW_DATA_SCHEMA = new DataSchema(
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
      new AggregatorFactory[]{
          new DoubleSumAggregatorFactory("met1sum", "met1"),
          new CountAggregatorFactory("rows")
      },
      new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
      null
  );
  protected static final InputFormat INPUT_FORMAT = new JsonInputFormat(
      new JSONPathSpec(true, ImmutableList.of()),
      ImmutableMap.of(),
      null,
      null,
      null
  );
  protected static final Logger LOG = new Logger(SeekableStreamIndexTaskTestBase.class);
  protected static ListeningExecutorService taskExec;

  static {
    NullHandling.initializeForTests();
  }

  protected final List<Task> runningTasks = new ArrayList<>();
  protected final LockGranularity lockGranularity;
  protected File directory;
  protected File reportsFile;
  protected TaskToolboxFactory toolboxFactory;
  protected TaskStorage taskStorage;
  protected TaskLockbox taskLockbox;
  protected IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  protected final Set<Integer> checkpointRequestsHash = new HashSet<>();

  static {
    OBJECT_MAPPER = new TestUtils().getTestObjectMapper();
    OBJECT_MAPPER.registerSubtypes(new NamedType(JSONParseSpec.class, "json"));
    OLD_DATA_SCHEMA = new DataSchema(
        "test_ds",
        OBJECT_MAPPER.convertValue(
            new StringInputRowParser(
                new JSONParseSpec(
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
                    new JSONPathSpec(true, ImmutableList.of()),
                    ImmutableMap.of(),
                    false
                ),
                StandardCharsets.UTF_8.name()
            ),
            Map.class
        ),
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("met1sum", "met1"),
            new CountAggregatorFactory("rows")
        },
        new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
        null,
        OBJECT_MAPPER
    );
  }

  public SeekableStreamIndexTaskTestBase(
      LockGranularity lockGranularity
  )
  {
    this.lockGranularity = lockGranularity;
  }

  protected static ByteEntity jb(
      String timestamp,
      String dim1,
      String dim2,
      String dimLong,
      String dimFloat,
      String met1
  )
  {
    return jb(false, timestamp, dim1, dim2, dimLong, dimFloat, met1);
  }

  protected static byte[] jbb(
      String timestamp,
      String dim1,
      String dim2,
      String dimLong,
      String dimFloat,
      String met1
  )
  {
    return jbb(false, timestamp, dim1, dim2, dimLong, dimFloat, met1);
  }

  protected static ByteEntity jb(boolean prettyPrint,
      String timestamp,
      String dim1,
      String dim2,
      String dimLong,
      String dimFloat,
      String met1
  )
  {
    return new ByteEntity(jbb(prettyPrint, timestamp, dim1, dim2, dimLong, dimFloat, met1));
  }

  protected static byte[] jbb(
      boolean prettyPrint,
      String timestamp,
      String dim1,
      String dim2,
      String dimLong,
      String dimFloat,
      String met1
  )
  {
    return StringUtils.toUtf8(toJsonString(
        prettyPrint,
        timestamp,
        dim1,
        dim2,
        dimLong,
        dimFloat,
        met1
    ));
  }

  protected static List<ByteEntity> jbl(
      String timestamp,
      String dim1,
      String dim2,
      String dimLong,
      String dimFloat,
      String met1
  )
  {
    return Collections.singletonList(jb(timestamp, dim1, dim2, dimLong, dimFloat, met1));
  }

  protected static String toJsonString(boolean prettyPrint,
                             String timestamp,
                             String dim1,
                             String dim2,
                             String dimLong,
                             String dimFloat,
                             String met1
  )
  {
    try {
      ObjectMapper mapper = new ObjectMapper();
      if (prettyPrint) {
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
      }
      return mapper.writeValueAsString(
          ImmutableMap.builder()
                      .put("timestamp", timestamp)
                      .put("dim1", dim1)
                      .put("dim2", dim2)
                      .put("dimLong", dimLong)
                      .put("dimFloat", dimFloat)
                      .put("met1", met1)
                      .build()
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected File getSegmentDirectory()
  {
    return new File(directory, "segments");
  }

  protected List<String> readSegmentColumn(final String column, final SegmentDescriptor descriptor) throws IOException
  {
    File indexBasePath = new File(
        StringUtils.format(
            "%s/%s/%s_%s/%s/%d",
            getSegmentDirectory(),
            OLD_DATA_SCHEMA.getDataSource(),
            descriptor.getInterval().getStart(),
            descriptor.getInterval().getEnd(),
            descriptor.getVersion(),
            descriptor.getPartitionNumber()
        )
    );

    File outputLocation = new File(
        directory,
        StringUtils.format(
            "%s_%s_%s_%s",
            descriptor.getInterval().getStart(),
            descriptor.getInterval().getEnd(),
            descriptor.getVersion(),
            descriptor.getPartitionNumber()
        )
    );
    outputLocation.mkdir();
    CompressionUtils.unzip(
        Files.asByteSource(new File(indexBasePath.listFiles()[0], "index.zip")),
        outputLocation,
        Predicates.alwaysFalse(),
        false
    );
    IndexIO indexIO = new TestUtils().getTestIndexIO();
    List<String> values = new ArrayList<>();

    QueryableIndex index = indexIO.loadIndex(outputLocation);
    try (DictionaryEncodedColumn<String> theColumn =
        (DictionaryEncodedColumn<String>) index.getColumnHolder(column).getColumn()) {
      for (int i = 0; i < theColumn.length(); i++) {
        int id = theColumn.getSingleValueRow(i);
        String value = theColumn.lookupName(id);
        values.add(value);
      }
    }

    return values;
  }

  protected SegmentDescriptor sd(final String intervalString, final int partitionNum)
  {
    final Interval interval = Intervals.of(intervalString);
    return new SegmentDescriptor(interval, "fakeVersion", partitionNum);
  }

  protected void assertEqualsExceptVersion(
      List<SegmentDescriptorAndExpectedDim1Values> expectedDescriptors,
      List<SegmentDescriptor> actualDescriptors
  ) throws IOException
  {
    Assert.assertEquals("number of segments", expectedDescriptors.size(), actualDescriptors.size());
    final Comparator<SegmentDescriptor> comparator = (s1, s2) -> {
      final int intervalCompare = Comparators.intervalsByStartThenEnd().compare(s1.getInterval(), s2.getInterval());
      if (intervalCompare == 0) {
        return Integer.compare(s1.getPartitionNumber(), s2.getPartitionNumber());
      } else {
        return intervalCompare;
      }
    };

    final List<SegmentDescriptorAndExpectedDim1Values> expectedDescsCopy = new ArrayList<>(expectedDescriptors);
    final List<SegmentDescriptor> actualDescsCopy = new ArrayList<>(actualDescriptors);
    expectedDescsCopy.sort(
        Comparator.comparing(SegmentDescriptorAndExpectedDim1Values::getSegmentDescriptor, comparator)
    );
    actualDescsCopy.sort(comparator);

    for (int i = 0; i < expectedDescsCopy.size(); i++) {
      SegmentDescriptorAndExpectedDim1Values expectedDesc = expectedDescsCopy.get(i);
      SegmentDescriptor actualDesc = actualDescsCopy.get(i);
      Assert.assertEquals(
          expectedDesc.segmentDescriptor.getInterval(),
          actualDesc.getInterval()
      );
      Assert.assertEquals(
          expectedDesc.segmentDescriptor.getPartitionNumber(),
          actualDesc.getPartitionNumber()
      );
      if (expectedDesc.expectedDim1Values.isEmpty()) {
        continue; // Treating empty expectedDim1Values as a signal that checking the dim1 column value is not needed.
      }
      Assertions.assertThat(readSegmentColumn("dim1", actualDesc))
                .describedAs("dim1 values")
                .isIn(expectedDesc.expectedDim1Values);
    }
  }

  /** "sdd" stands for "Segment Descriptor and expected Dim1 values" */
  protected SegmentDescriptorAndExpectedDim1Values sdd(
      String interval,
      int partitionNum,
      List<String>... expectedDim1Values
  )
  {
    return new SegmentDescriptorAndExpectedDim1Values(interval, partitionNum, expectedDim1Values);
  }

  protected IngestionStatsAndErrorsTaskReportData getTaskReportData() throws IOException
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

  protected ListenableFuture<TaskStatus> runTask(final Task task)
  {
    try {
      taskStorage.insert(task, TaskStatus.running(task.getId()));
    }
    catch (EntryExistsException e) {
      // suppress
    }
    taskLockbox.syncFromStorage();
    final TaskToolbox toolbox = toolboxFactory.build(task);
    synchronized (runningTasks) {
      runningTasks.add(task);
    }
    return taskExec.submit(
        () -> {
          try {
            task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
            if (task.isReady(toolbox.getTaskActionClient())) {
              return task.run(toolbox);
            } else {
              throw new ISE("Task is not ready");
            }
          }
          catch (Throwable e) {
            LOG.warn(e, "Task failed");
            return TaskStatus.failure(task.getId(), Throwables.getStackTraceAsString(e));
          }
        }
    );
  }

  protected long countEvents(final Task task)
  {
    // Do a query.
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(OLD_DATA_SCHEMA.getDataSource())
                                  .aggregators(
                                      ImmutableList.of(
                                          new LongSumAggregatorFactory("rows", "rows")
                                      )
                                  ).granularity(Granularities.ALL)
                                  .intervals(Intervals.ONLY_ETERNITY)
                                  .build();

    List<Result<TimeseriesResultValue>> results = task.getQueryRunner(query).run(QueryPlus.wrap(query)).toList();

    return results.isEmpty() ? 0L : DimensionHandlerUtils.nullToZero(results.get(0).getValue().getLongMetric("rows"));
  }

  protected void unlockAppenderatorBasePersistDirForTask(SeekableStreamIndexTask task)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
  {
    Method unlockBasePersistDir = ((StreamAppenderator) task.getAppenderator())
        .getClass()
        .getDeclaredMethod("unlockBasePersistDirectory");
    unlockBasePersistDir.setAccessible(true);
    unlockBasePersistDir.invoke(task.getAppenderator());
  }

  protected Collection<DataSegment> publishedSegments()
  {
    return metadataStorageCoordinator
        .retrieveAllUsedSegments(OLD_DATA_SCHEMA.getDataSource(), Segments.ONLY_VISIBLE);
  }

  protected List<SegmentDescriptor> publishedDescriptors()
  {
    return publishedSegments()
        .stream()
        .map(DataSegment::toDescriptor)
        .collect(Collectors.toList());
  }

  protected void destroyToolboxFactory()
  {
    toolboxFactory = null;
    taskStorage = null;
    taskLockbox = null;
    metadataStorageCoordinator = null;
  }

  protected void verifyTaskMetrics(
      SeekableStreamIndexTask<?, ?, ?> task,
      RowIngestionMetersTotals expectedTotals
  )
  {
    Assert.assertEquals(expectedTotals, task.getRunner().getRowIngestionMeters().getTotals());
  }

  protected abstract QueryRunnerFactoryConglomerate makeQueryRunnerConglomerate();

  protected void makeToolboxFactory(TestUtils testUtils, ServiceEmitter emitter, boolean doHandoff)
      throws IOException
  {
    final ObjectMapper objectMapper = testUtils.getTestObjectMapper();
    directory = tempFolder.newFolder();
    final TaskConfig taskConfig =
        new TaskConfigBuilder()
            .setBaseDir(new File(directory, "baseDir").getPath())
            .setBaseTaskDir(new File(directory, "baseTaskDir").getPath())
            .setDefaultRowFlushBoundary(50000)
            .setRestoreTasksOnRestart(true)
            .setBatchProcessingMode(TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name())
            .build();
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
        objectMapper,
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
              @Nullable DataSourceMetadata previousDataSourceMetadata
          )
          {
            // log.info("Adding checkpoint hash to the set");
            checkpointRequestsHash.add(
                Objects.hash(
                    supervisorId,
                    taskGroupId,
                    previousDataSourceMetadata
                )
            );
            return true;
          }
        },
        objectMapper
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
    dataSegmentPusherConfig.zip = true;
    final DataSegmentPusher dataSegmentPusher = new LocalDataSegmentPusher(dataSegmentPusherConfig);

    toolboxFactory = new TaskToolboxFactory(
        null,
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
        this::makeQueryRunnerConglomerate,
        DirectQueryProcessingPool.INSTANCE,
        NoopJoinableFactory.INSTANCE,
        () -> EasyMock.createMock(MonitorScheduler.class),
        new SegmentCacheManagerFactory(objectMapper),
        objectMapper,
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
        new NoopOverlordClient(),
        new NoopCoordinatorClient(),
        null,
        null,
        null,
        "1",
        CentralizedDatasourceSchemaConfig.create()
    );
  }

  protected class SegmentDescriptorAndExpectedDim1Values
  {
    final SegmentDescriptor segmentDescriptor;
    final Set<List<String>> expectedDim1Values;

    protected SegmentDescriptorAndExpectedDim1Values(
        String interval,
        int partitionNum,
        List<String>... expectedDim1Values
    )
    {
      segmentDescriptor = sd(interval, partitionNum);
      this.expectedDim1Values = ImmutableSet.copyOf(Arrays.asList(expectedDim1Values));
    }

    public SegmentDescriptor getSegmentDescriptor()
    {
      return segmentDescriptor;
    }
  }
}

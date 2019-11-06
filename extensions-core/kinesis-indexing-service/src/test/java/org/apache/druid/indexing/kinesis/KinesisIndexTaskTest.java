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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.common.SingleFileTaskReportFileWriter;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.LocalTaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionToolbox;
import org.apache.druid.indexing.common.actions.TaskAuditLogConfig;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTaskTest;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisor;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.MetadataTaskStorage;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SequenceMetadata;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.test.TestDataSegmentAnnouncer;
import org.apache.druid.indexing.test.TestDataSegmentKiller;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ListenableFutures;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.metadata.DerbyMetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.Druids;
import org.apache.druid.query.IntervalChunkingQueryRunnerDecorator;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorImpl;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.joda.time.Interval;
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;


@RunWith(Parameterized.class)
public class KinesisIndexTaskTest extends EasyMockSupport
{
  private static final Logger LOG = new Logger(KinesisIndexTaskTest.class);
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private static final String STREAM = "stream";
  private static final String SHARD_ID1 = "1";
  private static final String SHARD_ID0 = "0";
  private static KinesisRecordSupplier recordSupplier;
  private static List<OrderedPartitionableRecord<String, String>> records;

  private static ServiceEmitter emitter;
  private static ListeningExecutorService taskExec;

  private final List<Task> runningTasks = new ArrayList<>();

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK},
        new Object[]{LockGranularity.SEGMENT}
    );
  }

  private final LockGranularity lockGranularity;

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

  private AppenderatorsManager appenderatorsManager;
  private TaskToolboxFactory toolboxFactory;
  private IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private TaskStorage taskStorage;
  private TaskLockbox taskLockbox;
  private File directory;
  private final Set<Integer> checkpointRequestsHash = new HashSet<>();
  private File reportsFile;
  private RowIngestionMetersFactory rowIngestionMetersFactory;

  private static final DataSchema DATA_SCHEMA = new DataSchema(
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
                      ),
                      null,
                      null
                  ),
                  new JSONPathSpec(true, ImmutableList.of()),
                  ImmutableMap.of()
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
            Execs.makeThreadFactory("kinesis-task-test-%d")
        )
    );
  }

  public KinesisIndexTaskTest(LockGranularity lockGranularity)
  {
    this.lockGranularity = lockGranularity;
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
    records = generateRecords(STREAM);
    reportsFile = File.createTempFile("KinesisIndexTaskTestReports-" + System.currentTimeMillis(), "json");
    maxRecordsPerPoll = 1;

    recordSupplier = mock(KinesisRecordSupplier.class);

    appenderatorsManager = new TestAppenderatorsManager();

    // sleep required because of kinesalite
    Thread.sleep(500);
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

  private static List<OrderedPartitionableRecord<String, String>> generateRecords(String stream)
  {
    return ImmutableList.of(
        new OrderedPartitionableRecord<>(stream, "1", "0", jb("2008", "a", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "1", jb("2009", "b", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "2", jb("2010", "c", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "3", jb("2011", "d", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "4", jb("2011", "e", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(
            stream,
            "1",
            "5",
            jb("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0")
        ),
        new OrderedPartitionableRecord<>(
            stream,
            "1",
            "6",
            Collections.singletonList(StringUtils.toUtf8("unparseable"))
        ),
        new OrderedPartitionableRecord<>(
            stream,
            "1",
            "7",
            Collections.singletonList(StringUtils.toUtf8("unparseable2"))
        ),
        new OrderedPartitionableRecord<>(stream, "1", "8", Collections.singletonList(StringUtils.toUtf8("{}"))),
        new OrderedPartitionableRecord<>(stream, "1", "9", jb("2013", "f", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "10", jb("2049", "f", "y", "notanumber", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "11", jb("2049", "f", "y", "10", "notanumber", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "12", jb("2049", "f", "y", "10", "20.0", "notanumber")),
        new OrderedPartitionableRecord<>(stream, "0", "0", jb("2012", "g", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "0", "1", jb("2011", "h", "y", "10", "20.0", "1.0"))
    );
  }

  private static List<OrderedPartitionableRecord<String, String>> generateSinglePartitionRecords(String stream)
  {
    return ImmutableList.of(
        new OrderedPartitionableRecord<>(stream, "1", "0", jb("2008", "a", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "1", jb("2009", "b", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "2", jb("2010", "c", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "3", jb("2011", "d", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "4", jb("2011", "e", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "5", jb("2012", "a", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "6", jb("2013", "b", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "7", jb("2010", "c", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "8", jb("2011", "d", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "9", jb("2011", "e", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "10", jb("2008", "a", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "11", jb("2009", "b", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "12", jb("2010", "c", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "13", jb("2012", "d", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "14", jb("2013", "e", "y", "10", "20.0", "1.0"))
    );
  }

  @Test(timeout = 120_000L)
  public void testRunAfterDataInserted() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(2, 5)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )

    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = sd("2010/P1D", 0);
    SegmentDescriptor desc2 = sd("2011/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))
        ),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", publishedDescriptors.get(1)));
  }

  @Test(timeout = 120_000L)
  public void testRunBeforeDataInserted() throws Exception
  {

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(Collections.emptyList())
            .times(5)
            .andReturn(records.subList(13, 15))
            .once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID0, "0"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID0, "1")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )

    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();
    // Check metrics
    Assert.assertEquals(2, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = sd("2011/P1D", 0);
    SegmentDescriptor desc2 = sd("2012/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID0, "1"))
        ),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("h"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("g"), readSegmentColumn("dim1", publishedDescriptors.get(1)));
  }

  @Test(timeout = 120_000L)
  public void testIncrementalHandOff() throws Exception
  {
    final String baseSequenceName = "sequence0";
    // as soon as any segment has more than one record, incremental publishing should happen
    maxRowsPerSegment = 2;
    maxRecordsPerPoll = 1;

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(0, 5))
            .once()
            .andReturn(records.subList(4, records.size()))
            .once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final SeekableStreamStartSequenceNumbers<String, String> startPartitions = new SeekableStreamStartSequenceNumbers<>(
        STREAM,
        ImmutableMap.of(SHARD_ID1, "0", SHARD_ID0, "0"),
        ImmutableSet.of()
    );

    final SeekableStreamEndSequenceNumbers<String, String> checkpoint1 = new SeekableStreamEndSequenceNumbers<>(
        STREAM,
        ImmutableMap.of(SHARD_ID1, "4", SHARD_ID0, "0")
    );

    final SeekableStreamEndSequenceNumbers<String, String> endPartitions = new SeekableStreamEndSequenceNumbers<>(
        STREAM,
        ImmutableMap.of(SHARD_ID1, "9", SHARD_ID0, "1")
    );
    final KinesisIndexTask task = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            baseSequenceName,
            startPartitions,
            endPartitions,
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);
    while (task.getRunner().getStatus() != SeekableStreamIndexTaskRunner.Status.PAUSED) {
      Thread.sleep(10);
    }
    final Map<String, String> currentOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpoint1.getPartitionSequenceNumberMap(), currentOffsets);
    task.getRunner().setEndOffsets(currentOffsets, false);

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    Assert.assertEquals(1, checkpointRequestsHash.size());
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                DATA_SCHEMA.getDataSource(),
                0,
                new KinesisDataSourceMetadata(startPartitions)
            )
        )
    );

    // Check metrics
    Assert.assertEquals(8, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(4, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = sd("2008/P1D", 0);
    SegmentDescriptor desc2 = sd("2009/P1D", 0);
    SegmentDescriptor desc3 = sd("2010/P1D", 0);
    SegmentDescriptor desc4 = sd("2011/P1D", 0);
    SegmentDescriptor desc5 = sd("2011/P1D", 1);
    SegmentDescriptor desc6 = sd("2012/P1D", 0);
    SegmentDescriptor desc7 = sd("2013/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2, desc3, desc4, desc5, desc6, desc7), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(
                STREAM,
                ImmutableMap.of(SHARD_ID1, "9", SHARD_ID0, "1")
            )
        ),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("a"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("b"), readSegmentColumn("dim1", publishedDescriptors.get(1)));
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(2)));
    Assert.assertTrue((ImmutableList.of("d", "e").equals(readSegmentColumn("dim1", publishedDescriptors.get(3)))
                       && ImmutableList.of("h").equals(readSegmentColumn("dim1", publishedDescriptors.get(4)))) ||
                      (ImmutableList.of("d", "h").equals(readSegmentColumn("dim1", publishedDescriptors.get(3)))
                       && ImmutableList.of("e").equals(readSegmentColumn("dim1", publishedDescriptors.get(4)))));
    Assert.assertEquals(ImmutableList.of("g"), readSegmentColumn("dim1", publishedDescriptors.get(5)));
    Assert.assertEquals(ImmutableList.of("f"), readSegmentColumn("dim1", publishedDescriptors.get(6)));
  }

  @Test(timeout = 120_000L)
  public void testIncrementalHandOffMaxTotalRows() throws Exception
  {
    final String baseSequenceName = "sequence0";
    // incremental publish should happen every 3 records
    maxRowsPerSegment = Integer.MAX_VALUE;
    maxTotalRows = 3L;

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(0, 3))
            .once()
            .andReturn(records.subList(2, 10))
            .once()
            .andReturn(records.subList(9, 11));

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    // Insert data
    final SeekableStreamStartSequenceNumbers<String, String> startPartitions = new SeekableStreamStartSequenceNumbers<>(
        STREAM,
        ImmutableMap.of(SHARD_ID1, "0"),
        ImmutableSet.of()
    );
    // Checkpointing will happen at either checkpoint1 or checkpoint2 depending on ordering
    // of events fetched across two partitions from Kafka
    final SeekableStreamEndSequenceNumbers<String, String> checkpoint1 = new SeekableStreamEndSequenceNumbers<>(
        STREAM,
        ImmutableMap.of(SHARD_ID1, "2")
    );
    final SeekableStreamEndSequenceNumbers<String, String> checkpoint2 = new SeekableStreamEndSequenceNumbers<>(
        STREAM,
        ImmutableMap.of(SHARD_ID1, "9")
    );
    final SeekableStreamEndSequenceNumbers<String, String> endPartitions = new SeekableStreamEndSequenceNumbers<>(
        STREAM,
        ImmutableMap.of(SHARD_ID1, "10")
    );

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            baseSequenceName,
            startPartitions,
            endPartitions,
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);
    while (task.getRunner().getStatus() != SeekableStreamIndexTaskRunner.Status.PAUSED) {
      Thread.sleep(10);
    }
    final Map<String, String> currentOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());

    Assert.assertEquals(checkpoint1.getPartitionSequenceNumberMap(), currentOffsets);
    task.getRunner().setEndOffsets(currentOffsets, false);

    while (task.getRunner().getStatus() != SeekableStreamIndexTaskRunner.Status.PAUSED) {
      Thread.sleep(10);
    }

    final Map<String, String> nextOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());

    Assert.assertEquals(checkpoint2.getPartitionSequenceNumberMap(), nextOffsets);

    task.getRunner().setEndOffsets(nextOffsets, false);

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    Assert.assertEquals(2, checkpointRequestsHash.size());
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                DATA_SCHEMA.getDataSource(),
                0,
                new KinesisDataSourceMetadata(startPartitions)
            )
        )
    );
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                DATA_SCHEMA.getDataSource(),
                0,
                new KinesisDataSourceMetadata(
                    new SeekableStreamStartSequenceNumbers<>(STREAM, currentOffsets, currentOffsets.keySet()))
            )
        )
    );

    // Check metrics
    Assert.assertEquals(6, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(4, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = sd("2008/P1D", 0);
    SegmentDescriptor desc2 = sd("2009/P1D", 0);
    SegmentDescriptor desc3 = sd("2010/P1D", 0);
    SegmentDescriptor desc4 = sd("2011/P1D", 0);
    SegmentDescriptor desc5 = sd("2049/P1D", 0);
    SegmentDescriptor desc7 = sd("2013/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2, desc3, desc4, desc5, desc7), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "10"))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("a"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("b"), readSegmentColumn("dim1", publishedDescriptors.get(1)));
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(2)));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", publishedDescriptors.get(3)));
    Assert.assertEquals(ImmutableList.of("f"), readSegmentColumn("dim1", publishedDescriptors.get(4)));
    Assert.assertEquals(ImmutableList.of("f"), readSegmentColumn("dim1", publishedDescriptors.get(5)));
  }


  @Test(timeout = 120_000L)
  public void testRunWithMinimumMessageTime() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(0, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "0"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4")),
            true,
            DateTimes.of("2010"),
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getRunner().getStatus() != SeekableStreamIndexTaskRunner.Status.READING) {
      Thread.sleep(10);
    }

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(2, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = sd("2010/P1D", 0);
    SegmentDescriptor desc2 = sd("2011/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", publishedDescriptors.get(1)));
  }


  @Test(timeout = 120_000L)
  public void testRunWithMaximumMessageTime() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(0, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "0"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4")),
            true,
            null,
            DateTimes.of("2010"),
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getRunner().getStatus() != SeekableStreamIndexTaskRunner.Status.READING) {
      Thread.sleep(10);
    }

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(2, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = sd("2008/P1D", 0);
    SegmentDescriptor desc2 = sd("2009/P1D", 0);
    SegmentDescriptor desc3 = sd("2010/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2, desc3), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("a"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("b"), readSegmentColumn("dim1", publishedDescriptors.get(1)));
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(2)));
  }


  @Test(timeout = 120_000L)
  public void testRunWithTransformSpec() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(0, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        null,
        DATA_SCHEMA.withTransformSpec(
            new TransformSpec(
                new SelectorDimFilter("dim1", "b", null),
                ImmutableList.of(
                    new ExpressionTransform("dim1t", "concat(dim1,dim1)", ExprMacroTable.nil())
                )
            )
        ),
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "0"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getRunner().getStatus() != SeekableStreamIndexTaskRunner.Status.READING) {
      Thread.sleep(10);
    }

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(1, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(4, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = sd("2009/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("b"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("bb"), readSegmentColumn("dim1t", publishedDescriptors.get(0)));
  }


  @Test(timeout = 120_000L)
  public void testRunOnSingletonRange() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(2, 3)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    // When start and end offsets are the same, it means we need to read one message (since in Kinesis, end offsets
    // are inclusive).
    final KinesisIndexTask task = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(1, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    assertEqualsExceptVersion(ImmutableList.of(sd("2010/P1D", 0)), publishedDescriptors());
  }


  @Test(timeout = 60_000L)
  public void testHandoffConditionTimeoutWhenHandoffOccurs() throws Exception
  {
    handoffConditionTimeout = 5_000;

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(2, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = sd("2010/P1D", 0);
    SegmentDescriptor desc2 = sd("2011/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))
        ),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", publishedDescriptors.get(1)));
  }


  @Test(timeout = 60_000L)
  public void testHandoffConditionTimeoutWhenHandoffDoesNotOccur() throws Exception
  {
    doHandoff = false;
    handoffConditionTimeout = 100;

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(2, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = sd("2010/P1D", 0);
    SegmentDescriptor desc2 = sd("2011/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))
        ),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", publishedDescriptors.get(1)));
  }


  @Test(timeout = 120_000L)
  public void testReportParseExceptions() throws Exception
  {
    reportParseExceptions = true;

    // these will be ignored because reportParseExceptions is true
    maxParseExceptions = 1000;
    maxSavedParseExceptions = 2;

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(2, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "5")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.FAILED, future.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(1, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    Assert.assertEquals(ImmutableList.of(), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));
  }


  @Test(timeout = 120_000L)
  public void testMultipleParseExceptionsSuccess() throws Exception
  {
    reportParseExceptions = false;
    maxParseExceptions = 7;
    maxSavedParseExceptions = 7;

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(2, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "12")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    TaskStatus status = future.get();

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, status.getStatusCode());

    verifyAll();

    Assert.assertNull(status.getErrorMsg());

    // Check metrics
    Assert.assertEquals(4, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessedWithError());
    Assert.assertEquals(4, task.getRunner().getRowIngestionMeters().getUnparseable());

    // Check published metadata
    SegmentDescriptor desc1 = sd("2010/P1D", 0);
    SegmentDescriptor desc2 = sd("2011/P1D", 0);
    SegmentDescriptor desc3 = sd("2013/P1D", 0);
    SegmentDescriptor desc4 = sd("2049/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2, desc3, desc4), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "12"))
        ),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED, 4,
            RowIngestionMeters.PROCESSED_WITH_ERROR, 3,
            RowIngestionMeters.UNPARSEABLE, 4,
            RowIngestionMeters.THROWN_AWAY, 0
        )
    );
    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    Map<String, Object> unparseableEvents = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        Arrays.asList(
            "Found unparseable columns in row: [MapBasedInputRow{timestamp=2049-01-01T00:00:00.000Z, event={timestamp=2049, dim1=f, dim2=y, dimLong=10, dimFloat=20.0, met1=notanumber}, dimensions=[dim1, dim1t, dim2, dimLong, dimFloat]}], exceptions: [Unable to parse value[notanumber] for field[met1],]",
            "Found unparseable columns in row: [MapBasedInputRow{timestamp=2049-01-01T00:00:00.000Z, event={timestamp=2049, dim1=f, dim2=y, dimLong=10, dimFloat=notanumber, met1=1.0}, dimensions=[dim1, dim1t, dim2, dimLong, dimFloat]}], exceptions: [could not convert value [notanumber] to float,]",
            "Found unparseable columns in row: [MapBasedInputRow{timestamp=2049-01-01T00:00:00.000Z, event={timestamp=2049, dim1=f, dim2=y, dimLong=notanumber, dimFloat=20.0, met1=1.0}, dimensions=[dim1, dim1t, dim2, dimLong, dimFloat]}], exceptions: [could not convert value [notanumber] to long,]",
            "Unparseable timestamp found! Event: {}",
            "Unable to parse row [unparseable2]",
            "Unable to parse row [unparseable]",
            "Encountered row with timestamp that cannot be represented as a long: [MapBasedInputRow{timestamp=246140482-04-24T15:36:27.903Z, event={timestamp=246140482-04-24T15:36:27.903Z, dim1=x, dim2=z, dimLong=10, dimFloat=20.0, met1=1.0}, dimensions=[dim1, dim1t, dim2, dimLong, dimFloat]}]"
        )
    );

    Assert.assertEquals(unparseableEvents, reportData.getUnparseableEvents());
  }


  @Test(timeout = 120_000L)
  public void testMultipleParseExceptionsFailure() throws Exception
  {
    reportParseExceptions = false;
    maxParseExceptions = 2;
    maxSavedParseExceptions = 2;

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(2, 13)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "9")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    TaskStatus status = future.get();

    // Wait for task to exit
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());
    verifyAll();
    IndexTaskTest.checkTaskStatusErrorMsgForParseExceptionsExceeded(status);

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getProcessedWithError());
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    Assert.assertEquals(ImmutableList.of(), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED, 3,
            RowIngestionMeters.PROCESSED_WITH_ERROR, 0,
            RowIngestionMeters.UNPARSEABLE, 3,
            RowIngestionMeters.THROWN_AWAY, 0
        )
    );
    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    Map<String, Object> unparseableEvents = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        Arrays.asList(
            "Unable to parse row [unparseable2]",
            "Unable to parse row [unparseable]"
        )
    );

    Assert.assertEquals(unparseableEvents, reportData.getUnparseableEvents());
  }


  @Test(timeout = 120_000L)
  public void testRunReplicas() throws Exception
  {
    // Insert data
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(2, 13)).times(2);

    recordSupplier.close();
    EasyMock.expectLastCall().times(2);

    replayAll();

    final KinesisIndexTask task1 = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );
    final KinesisIndexTask task2 = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(3, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(3, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = sd("2010/P1D", 0);
    SegmentDescriptor desc2 = sd("2011/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))
        ),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", publishedDescriptors.get(1)));
  }


  @Test(timeout = 120_000L)
  public void testRunConflicting() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(2, 13))
            .once()
            .andReturn(records.subList(3, 13))
            .once();

    recordSupplier.close();
    EasyMock.expectLastCall().atLeastOnce();

    replayAll();

    final KinesisIndexTask task1 = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );
    final KinesisIndexTask task2 = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            1,
            "sequence1",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "3"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "9")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    // Run first task
    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskState.FAILED, future2.get().getStatusCode());

    verifyAll();
    // Check metrics
    Assert.assertEquals(3, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(3, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(4, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata, should all be from the first task
    SegmentDescriptor desc1 = sd("2010/P1D", 0);
    SegmentDescriptor desc2 = sd("2011/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", publishedDescriptors.get(1)));
  }


  @Test(timeout = 120_000L)
  public void testRunConflictingWithoutTransactions() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(2, 13))
            .once()
            .andReturn(records.subList(3, 13))
            .once();

    recordSupplier.close();
    EasyMock.expectLastCall().times(2);

    replayAll();

    final KinesisIndexTask task1 = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4")),
            false,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );
    final KinesisIndexTask task2 = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            1,
            "sequence1",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "3"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "9")),
            false,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    // Run first task
    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    // Check published segments & metadata
    SegmentDescriptor desc1 = sd("2010/P1D", 0);
    SegmentDescriptor desc2 = sd("2011/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(3, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(3, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(4, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc3 = sd("2011/P1D", 1);
    SegmentDescriptor desc4 = sd("2013/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2, desc3, desc4), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", publishedDescriptors.get(1)));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", publishedDescriptors.get(2)));
    Assert.assertEquals(ImmutableList.of("f"), readSegmentColumn("dim1", publishedDescriptors.get(3)));
  }


  @Test(timeout = 120_000L)
  public void testRunOneTaskTwoPartitions() throws Exception
  {
    // Insert data
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(2, records.size())).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence1",
            new SeekableStreamStartSequenceNumbers<>(
                STREAM,
                ImmutableMap.of(SHARD_ID1, "2", SHARD_ID0, "0"),
                ImmutableSet.of()
            ),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4", SHARD_ID0, "1")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    while (countEvents(task) < 5) {
      Thread.sleep(10);
    }

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(5, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = sd("2010/P1D", 0);
    SegmentDescriptor desc2 = sd("2011/P1D", 0);
    SegmentDescriptor desc4 = sd("2012/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2, desc4), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4", SHARD_ID0, "1"))
        ),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("g"), readSegmentColumn("dim1", publishedDescriptors.get(2)));

    // Check desc2/desc3 without strong ordering because two partitions are interleaved nondeterministically
    Assert.assertEquals(
        ImmutableSet.of(ImmutableList.of("d", "e", "h")),
        ImmutableSet.of(readSegmentColumn("dim1", publishedDescriptors.get(1)))
    );
  }


  @Test(timeout = 120_000L)
  public void testRunTwoTasksTwoPartitions() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(2, 13))
            .once()
            .andReturn(records.subList(13, 15))
            .once();

    recordSupplier.close();
    EasyMock.expectLastCall().times(2);

    replayAll();

    final KinesisIndexTask task1 = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );
    final KinesisIndexTask task2 = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            1,
            "sequence1",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID0, "0"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID0, "1")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(3, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(2, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = sd("2010/P1D", 0);
    SegmentDescriptor desc2 = sd("2011/P1D", 0);
    SegmentDescriptor desc3 = sd("2011/P1D", 1);
    SegmentDescriptor desc4 = sd("2012/P1D", 0);

    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2, desc3, desc4), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4", SHARD_ID0, "1"))
        ),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    // Check desc2/desc3 without strong ordering because two partitions are interleaved nondeterministically
    Assert.assertEquals(
        ImmutableSet.of(ImmutableList.of("d", "e"), ImmutableList.of("h")),
        ImmutableSet.of(readSegmentColumn("dim1", publishedDescriptors.get(1)), readSegmentColumn("dim1", publishedDescriptors.get(2)))
    );
    Assert.assertEquals(ImmutableList.of("g"), readSegmentColumn("dim1", publishedDescriptors.get(3)));
  }


  @Test(timeout = 120_000L)
  public void testRestore() throws Exception
  {
    final StreamPartition<String> streamPartition = StreamPartition.of(STREAM, SHARD_ID1);
    recordSupplier.assign(ImmutableSet.of(streamPartition));
    EasyMock.expectLastCall();
    recordSupplier.seek(streamPartition, "2");
    EasyMock.expectLastCall();
    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(2, 4))
            .once()
            .andReturn(Collections.emptyList())
            .anyTimes();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task1 = createTask(
        "task1",
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "5")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);

    while (countEvents(task1) != 2) {
      Thread.sleep(25);
    }

    Assert.assertEquals(2, countEvents(task1));

    // Stop without publishing segment
    task1.stopGracefully(toolboxFactory.build(task1).getConfig());
    unlockAppenderatorBasePersistDirForTask(task1);

    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    verifyAll();
    EasyMock.reset(recordSupplier);

    recordSupplier.assign(ImmutableSet.of(streamPartition));
    EasyMock.expectLastCall();
    recordSupplier.seek(streamPartition, "3");
    EasyMock.expectLastCall();
    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(3, 6)).once();
    recordSupplier.assign(ImmutableSet.of());
    EasyMock.expectLastCall();
    recordSupplier.close();
    EasyMock.expectLastCall();

    replayAll();

    // Start a new task
    final KinesisIndexTask task2 = createTask(
        task1.getId(),
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "5")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    while (countEvents(task2) < 3) {
      Thread.sleep(25);
    }

    Assert.assertEquals(3, countEvents(task2));

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(2, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(1, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(1, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = sd("2010/P1D", 0);
    SegmentDescriptor desc2 = sd("2011/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "5"))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", publishedDescriptors.get(1)));
  }

  @Test(timeout = 120_000L)
  public void testRestoreAfterPersistingSequences() throws Exception
  {
    maxRowsPerSegment = 2;
    maxRecordsPerPoll = 1;
    records = generateSinglePartitionRecords(STREAM);

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    // simulate 1 record at a time
    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(Collections.singletonList(records.get(0)))
            .once()
            .andReturn(Collections.singletonList(records.get(1)))
            .once()
            .andReturn(Collections.singletonList(records.get(2)))
            .once()
            .andReturn(Collections.singletonList(records.get(3)))
            .once()
            .andReturn(Collections.singletonList(records.get(4)))
            .once()
            .andReturn(Collections.emptyList())
            .anyTimes();

    replayAll();

    final KinesisIndexTask task1 = createTask(
        "task1",
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "0"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "6")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final SeekableStreamEndSequenceNumbers<String, String> checkpoint1 = new SeekableStreamEndSequenceNumbers<>(
        STREAM,
        ImmutableMap.of(SHARD_ID1, "4")
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);

    while (task1.getRunner().getStatus() != SeekableStreamIndexTaskRunner.Status.PAUSED) {
      Thread.sleep(10);
    }
    final Map<String, String> currentOffsets = ImmutableMap.copyOf(task1.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpoint1.getPartitionSequenceNumberMap(), currentOffsets);
    task1.getRunner().setEndOffsets(currentOffsets, false);

    // Stop without publishing segment
    task1.stopGracefully(toolboxFactory.build(task1).getConfig());
    unlockAppenderatorBasePersistDirForTask(task1);

    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    verifyAll();
    EasyMock.reset(recordSupplier);

    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(Collections.singletonList(records.get(5)))
            .once()
            .andReturn(Collections.singletonList(records.get(6)))
            .once()
            .andReturn(Collections.emptyList())
            .anyTimes();

    recordSupplier.close();
    EasyMock.expectLastCall();

    replayAll();

    // Start a new task
    final KinesisIndexTask task2 = createTask(
        task1.getId(),
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "0"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "6")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(5, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(2, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = sd("2008/P1D", 0);
    SegmentDescriptor desc2 = sd("2009/P1D", 0);
    SegmentDescriptor desc3 = sd("2010/P1D", 0);
    SegmentDescriptor desc4 = sd("2011/P1D", 0);
    SegmentDescriptor desc5 = sd("2012/P1D", 0);
    SegmentDescriptor desc6 = sd("2013/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2, desc3, desc4, desc5, desc6), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "6"))
        ),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );
  }

  @Test(timeout = 120_000L)
  public void testRunWithPauseAndResume() throws Exception
  {
    final StreamPartition<String> streamPartition = StreamPartition.of(STREAM, SHARD_ID1);
    recordSupplier.assign(ImmutableSet.of(streamPartition));
    EasyMock.expectLastCall();
    recordSupplier.seek(streamPartition, "2");
    EasyMock.expectLastCall();
    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(2, 5))
            .once()
            .andReturn(Collections.emptyList())
            .anyTimes();

    replayAll();

    final KinesisIndexTask task = createTask(
        "task1",
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "13")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);


    while (countEvents(task) != 3) {
      Thread.sleep(25);
    }

    Assert.assertEquals(3, countEvents(task));
    Assert.assertEquals(SeekableStreamIndexTaskRunner.Status.READING, task.getRunner().getStatus());

    task.getRunner().pause();

    while (task.getRunner().getStatus() != SeekableStreamIndexTaskRunner.Status.PAUSED) {
      Thread.sleep(10);
    }
    Assert.assertEquals(SeekableStreamIndexTaskRunner.Status.PAUSED, task.getRunner().getStatus());

    verifyAll();

    ConcurrentMap<String, String> currentOffsets = task.getRunner().getCurrentOffsets();

    try {
      future.get(10, TimeUnit.SECONDS);
      Assert.fail("Task completed when it should have been paused");
    }
    catch (TimeoutException e) {
      // carry on..
    }

    Assert.assertEquals(currentOffsets, task.getRunner().getCurrentOffsets());

    EasyMock.reset(recordSupplier);

    recordSupplier.assign(ImmutableSet.of());
    EasyMock.expectLastCall();
    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    task.getRunner().setEndOffsets(currentOffsets, true);

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();
    Assert.assertEquals(task.getRunner().getEndOffsets(), task.getRunner().getCurrentOffsets());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = sd("2010/P1D", 0);
    SegmentDescriptor desc2 = sd("2011/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(
            STREAM,
            ImmutableMap.of(SHARD_ID1, currentOffsets.get(SHARD_ID1))
        )),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", publishedDescriptors.get(1)));
  }

  @Test(timeout = 60_000L)
  public void testRunContextSequenceAheadOfStartingOffsets() throws Exception
  {
    // This tests the case when a replacement task is created in place of a failed test
    // which has done some incremental handoffs, thus the context will contain starting
    // sequence sequences from which the task should start reading and ignore the start sequences
    // Insert data
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(records.subList(2, 13))
            .once();

    recordSupplier.close();
    EasyMock.expectLastCall();

    replayAll();

    final TreeMap<Integer, Map<String, String>> sequences = new TreeMap<>();
    // Here the sequence number is 1 meaning that one incremental handoff was done by the failed task
    // and this task should start reading from offset 2 for partition 0 (not offset 1, because end is inclusive)
    sequences.put(1, ImmutableMap.of(SHARD_ID1, "1"));
    final Map<String, Object> context = new HashMap<>();
    context.put(
        SeekableStreamSupervisor.CHECKPOINTS_CTX_KEY,
        OBJECT_MAPPER.writerFor(KinesisSupervisor.CHECKPOINTS_TYPE_REF).writeValueAsString(sequences)
    );


    final KinesisIndexTask task = createTask(
        "task1",
        DATA_SCHEMA,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "0"), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        ),
        context
    );
    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = sd("2010/P1D", 0);
    SegmentDescriptor desc2 = sd("2011/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "4"))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", publishedDescriptors.get(1)));
  }

  @Test(timeout = 5000L)
  public void testIncrementalHandOffReadsThroughEndOffsets() throws Exception
  {
    records = generateSinglePartitionRecords(STREAM);

    final String baseSequenceName = "sequence0";
    // as soon as any segment has more than one record, incremental publishing should happen
    maxRowsPerSegment = 2;

    final KinesisRecordSupplier recordSupplier1 = mock(KinesisRecordSupplier.class);
    recordSupplier1.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(recordSupplier1.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    recordSupplier1.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(recordSupplier1.poll(EasyMock.anyLong())).andReturn(records.subList(0, 5))
            .once()
            .andReturn(records.subList(4, 10))
            .once();
    recordSupplier1.close();
    EasyMock.expectLastCall().once();
    final KinesisRecordSupplier recordSupplier2 = mock(KinesisRecordSupplier.class);
    recordSupplier2.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(recordSupplier2.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();
    recordSupplier2.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(recordSupplier2.poll(EasyMock.anyLong())).andReturn(records.subList(0, 5))
            .once()
            .andReturn(records.subList(4, 10))
            .once();
    recordSupplier2.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final SeekableStreamStartSequenceNumbers<String, String> startPartitions = new SeekableStreamStartSequenceNumbers<>(
        STREAM,
        ImmutableMap.of(SHARD_ID1, "0"),
        ImmutableSet.of()
    );

    final SeekableStreamEndSequenceNumbers<String, String> checkpoint1 = new SeekableStreamEndSequenceNumbers<>(
        STREAM,
        ImmutableMap.of(SHARD_ID1, "4")
    );

    final SeekableStreamEndSequenceNumbers<String, String> checkpoint2 = new SeekableStreamEndSequenceNumbers<>(
        STREAM,
        ImmutableMap.of(SHARD_ID1, "9")
    );

    final SeekableStreamEndSequenceNumbers<String, String> endPartitions = new SeekableStreamEndSequenceNumbers<>(
        STREAM,
        ImmutableMap.of(SHARD_ID1, "100") // simulating unlimited
    );
    final KinesisIndexTaskIOConfig ioConfig = new KinesisIndexTaskIOConfig(
        0,
        baseSequenceName,
        startPartitions,
        endPartitions,
        true,
        null,
        null,
        "awsEndpoint",
        null,
        null,
        null,
        null,
        false
    );
    final KinesisIndexTask normalReplica = createTask(
        null,
        DATA_SCHEMA,
        ioConfig,
        null
    );
    ((TestableKinesisIndexTask) normalReplica).setLocalSupplier(recordSupplier1);
    final KinesisIndexTask staleReplica = createTask(
        null,
        DATA_SCHEMA,
        ioConfig,
        null
    );
    ((TestableKinesisIndexTask) staleReplica).setLocalSupplier(recordSupplier2);
    final ListenableFuture<TaskStatus> normalReplicaFuture = runTask(normalReplica);
    // Simulating one replica is slower than the other
    final ListenableFuture<TaskStatus> staleReplicaFuture = ListenableFutures.transformAsync(
        taskExec.submit(() -> {
          Thread.sleep(1000);
          return staleReplica;
        }),
        this::runTask
    );

    while (normalReplica.getRunner().getStatus() != SeekableStreamIndexTaskRunner.Status.PAUSED) {
      Thread.sleep(10);
    }
    staleReplica.getRunner().pause();
    while (staleReplica.getRunner().getStatus() != SeekableStreamIndexTaskRunner.Status.PAUSED) {
      Thread.sleep(10);
    }
    Map<String, String> currentOffsets = ImmutableMap.copyOf(normalReplica.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpoint1.getPartitionSequenceNumberMap(), currentOffsets);

    normalReplica.getRunner().setEndOffsets(currentOffsets, false);
    staleReplica.getRunner().setEndOffsets(currentOffsets, false);

    while (normalReplica.getRunner().getStatus() != SeekableStreamIndexTaskRunner.Status.PAUSED) {
      Thread.sleep(10);
    }
    while (staleReplica.getRunner().getStatus() != SeekableStreamIndexTaskRunner.Status.PAUSED) {
      Thread.sleep(10);
    }
    currentOffsets = ImmutableMap.copyOf(normalReplica.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpoint2.getPartitionSequenceNumberMap(), currentOffsets);
    currentOffsets = ImmutableMap.copyOf(staleReplica.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpoint2.getPartitionSequenceNumberMap(), currentOffsets);

    normalReplica.getRunner().setEndOffsets(currentOffsets, true);
    staleReplica.getRunner().setEndOffsets(currentOffsets, true);

    Assert.assertEquals(TaskState.SUCCESS, normalReplicaFuture.get().getStatusCode());
    Assert.assertEquals(TaskState.SUCCESS, staleReplicaFuture.get().getStatusCode());

    verifyAll();

    Assert.assertEquals(2, checkpointRequestsHash.size());

    // Check metrics
    Assert.assertEquals(10, normalReplica.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, normalReplica.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, normalReplica.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    final List<SegmentDescriptor> descriptors = new ArrayList<>();
    descriptors.add(sd("2008/P1D", 0));
    descriptors.add(sd("2009/P1D", 0));
    descriptors.add(sd("2010/P1D", 0));
    descriptors.add(sd("2010/P1D", 1));
    descriptors.add(sd("2011/P1D", 0));
    descriptors.add(sd("2011/P1D", 1));
    descriptors.add(sd("2012/P1D", 0));
    descriptors.add(sd("2013/P1D", 0));
    assertEqualsExceptVersion(descriptors, publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID1, "9"))
        ),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );
  }

  @Test
  public void testSequencesFromContext() throws IOException
  {
    final TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
    // Here the sequence number is 1 meaning that one incremental handoff was done by the failed task
    // and this task should start reading from offset 2 for partition 0 (not offset 1, because end is inclusive)
    checkpoints.put(0, ImmutableMap.of(SHARD_ID0, "0", SHARD_ID1, "0"));
    checkpoints.put(1, ImmutableMap.of(SHARD_ID0, "0", SHARD_ID1, "1"));
    checkpoints.put(2, ImmutableMap.of(SHARD_ID0, "1", SHARD_ID1, "3"));
    final Map<String, Object> context = new HashMap<>();
    context.put("checkpoints", OBJECT_MAPPER.writerFor(KinesisSupervisor.CHECKPOINTS_TYPE_REF)
                                            .writeValueAsString(checkpoints));

    final KinesisIndexTask task = createTask(
        "task1",
        DATA_SCHEMA,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(
                STREAM,
                ImmutableMap.of(SHARD_ID0, "0", SHARD_ID1, "0"),
                ImmutableSet.of(SHARD_ID0)
            ),
            new SeekableStreamEndSequenceNumbers<>(STREAM, ImmutableMap.of(SHARD_ID0, "1", SHARD_ID1, "5")),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        ),
        context
    );

    task.getRunner().setToolbox(toolboxFactory.build(task));
    task.getRunner().initializeSequences();
    final CopyOnWriteArrayList<SequenceMetadata<String, String>> sequences = task.getRunner().getSequences();

    Assert.assertEquals(3, sequences.size());

    SequenceMetadata<String, String> sequenceMetadata = sequences.get(0);
    Assert.assertEquals(checkpoints.get(0), sequenceMetadata.getStartOffsets());
    Assert.assertEquals(checkpoints.get(1), sequenceMetadata.getEndOffsets());
    Assert.assertEquals(
        task.getIOConfig().getStartSequenceNumbers().getExclusivePartitions(),
        sequenceMetadata.getExclusiveStartPartitions()
    );
    Assert.assertTrue(sequenceMetadata.isCheckpointed());

    sequenceMetadata = sequences.get(1);
    Assert.assertEquals(checkpoints.get(1), sequenceMetadata.getStartOffsets());
    Assert.assertEquals(checkpoints.get(2), sequenceMetadata.getEndOffsets());
    Assert.assertEquals(checkpoints.get(1).keySet(), sequenceMetadata.getExclusiveStartPartitions());
    Assert.assertTrue(sequenceMetadata.isCheckpointed());

    sequenceMetadata = sequences.get(2);
    Assert.assertEquals(checkpoints.get(2), sequenceMetadata.getStartOffsets());
    Assert.assertEquals(
        task.getIOConfig().getEndSequenceNumbers().getPartitionSequenceNumberMap(),
        sequenceMetadata.getEndOffsets()
    );
    Assert.assertEquals(checkpoints.get(2).keySet(), sequenceMetadata.getExclusiveStartPartitions());
    Assert.assertFalse(sequenceMetadata.isCheckpointed());
  }

  /**
   * Tests handling of a closed shard. The task is initially given an unlimited end sequence number and
   * eventually gets an EOS marker which causes it to stop reading.
   */
  @Test(timeout = 120_000L)
  public void testEndOfShard() throws Exception
  {
    recordSupplier.assign(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(recordSupplier.getEarliestSequenceNumber(EasyMock.anyObject())).andReturn("0").anyTimes();

    recordSupplier.seek(EasyMock.anyObject(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    List<OrderedPartitionableRecord<String, String>> eosRecord = ImmutableList.of(
        new OrderedPartitionableRecord<>(STREAM, SHARD_ID1, KinesisSequenceNumber.END_OF_SHARD_MARKER, null)
    );

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong()))
            .andReturn(records.subList(2, 5)).once()
            .andReturn(eosRecord).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(
                STREAM,
                ImmutableMap.of(SHARD_ID1, "2"), ImmutableSet.of()
            ),
            new SeekableStreamEndSequenceNumbers<>(
                STREAM,
                ImmutableMap.of(SHARD_ID1, KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER)
            ),
            true,
            null,
            null,
            "awsEndpoint",
            null,
            null,
            null,
            null,
            false
        )

    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    verifyAll();

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(1, task.getRunner().getRowIngestionMeters().getThrownAway()); // EOS marker

    // Check published metadata
    SegmentDescriptor desc1 = sd("2010/P1D", 0);
    SegmentDescriptor desc2 = sd("2011/P1D", 0);
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(
                STREAM,
                ImmutableMap.of(SHARD_ID1, KinesisSequenceNumber.END_OF_SHARD_MARKER)
            )
        ),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", publishedDescriptors.get(1)));
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


  private TaskLock getLock(final Task task, final Interval interval)
  {
    return Iterables.find(
        taskLockbox.findLocksForTask(task),
        lock -> lock.getInterval().contains(interval)
    );
  }

  private KinesisIndexTask createTask(
      final String taskId,
      final KinesisIndexTaskIOConfig ioConfig
  ) throws JsonProcessingException
  {
    return createTask(taskId, DATA_SCHEMA, ioConfig, null);
  }

  private KinesisIndexTask createTask(
      final String taskId,
      final DataSchema dataSchema,
      final KinesisIndexTaskIOConfig ioConfig
  ) throws JsonProcessingException
  {
    return createTask(taskId, dataSchema, ioConfig, null);
  }

  private KinesisIndexTask createTask(
      final String taskId,
      final DataSchema dataSchema,
      final KinesisIndexTaskIOConfig ioConfig,
      @Nullable final Map<String, Object> context
  ) throws JsonProcessingException
  {
    boolean resetOffsetAutomatically = false;
    int maxRowsInMemory = 1000;
    final KinesisIndexTaskTuningConfig tuningConfig = new KinesisIndexTaskTuningConfig(
        maxRowsInMemory,
        null,
        maxRowsPerSegment,
        maxTotalRows,
        new Period("P1Y"),
        null,
        null,
        null,
        null,
        true,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        true,
        null,
        null,
        null,
        null,
        null,
        null,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        maxRecordsPerPoll,
        intermediateHandoffPeriod
    );
    return createTask(taskId, dataSchema, ioConfig, tuningConfig, context);
  }

  private KinesisIndexTask createTask(
      final String taskId,
      final DataSchema dataSchema,
      final KinesisIndexTaskIOConfig ioConfig,
      final KinesisIndexTaskTuningConfig tuningConfig,
      @Nullable final Map<String, Object> context
  ) throws JsonProcessingException
  {
    if (context != null) {
      if (!context.containsKey(SeekableStreamSupervisor.CHECKPOINTS_CTX_KEY)) {
        final TreeMap<Integer, Map<String, String>> checkpoints = new TreeMap<>();
        checkpoints.put(0, ioConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap());
        final String checkpointsJson = OBJECT_MAPPER
            .writerFor(KinesisSupervisor.CHECKPOINTS_TYPE_REF)
            .writeValueAsString(checkpoints);
        context.put(SeekableStreamSupervisor.CHECKPOINTS_CTX_KEY, checkpointsJson);
      }
    }

    return new TestableKinesisIndexTask(
        taskId,
        null,
        cloneDataSchema(dataSchema),
        tuningConfig,
        ioConfig,
        context,
        null,
        null,
        rowIngestionMetersFactory,
        null,
        appenderatorsManager
    );
  }

  private static DataSchema cloneDataSchema(final DataSchema dataSchema)
  {
    return new DataSchema(
        dataSchema.getDataSource(),
        dataSchema.getParserMap(),
        dataSchema.getAggregators(),
        dataSchema.getGranularitySpec(),
        dataSchema.getTransformSpec(),
        OBJECT_MAPPER
    );
  }

  private QueryRunnerFactoryConglomerate makeTimeseriesOnlyConglomerate()
  {
    IntervalChunkingQueryRunnerDecorator queryRunnerDecorator = new IntervalChunkingQueryRunnerDecorator(
        null,
        null,
        null
    )
    {
      @Override
      public <T> QueryRunner<T> decorate(
          QueryRunner<T> delegate,
          QueryToolChest<T, ? extends Query<T>> toolChest
      )
      {
        return delegate;
      }
    };
    return new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.of(
            TimeseriesQuery.class,
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(queryRunnerDecorator),
                new TimeseriesQueryEngine(),
                (query, future) -> {
                  // do nothing
                }
            )
        )
    );
  }

  private void makeToolboxFactory() throws IOException
  {
    directory = tempFolder.newFolder();
    final TestUtils testUtils = new TestUtils();
    rowIngestionMetersFactory = testUtils.getRowIngestionMetersFactory();
    final ObjectMapper objectMapper = testUtils.getTestObjectMapper();
    objectMapper.setInjectableValues(((InjectableValues.Std) objectMapper.getInjectableValues()).addValue(
        AWSCredentialsConfig.class,
        new AWSCredentialsConfig()
    ));
    for (Module module : new KinesisIndexingServiceModule().getJacksonModules()) {
      objectMapper.registerModule(module);
    }
    final TaskConfig taskConfig = new TaskConfig(
        new File(directory, "baseDir").getPath(),
        new File(directory, "baseTaskDir").getPath(),
        null,
        50000,
        null,
        true,
        null,
        null,
        null
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
        Execs.directExecutor(), // queryExecutorService
        EasyMock.createMock(MonitorScheduler.class),
        new SegmentLoaderFactory(null, testUtils.getTestObjectMapper()),
        testUtils.getTestObjectMapper(),
        testUtils.getTestIndexIO(),
        MapCache.create(1024),
        new CacheConfig(),
        new CachePopulatorStats(),
        testUtils.getTestIndexMergerV9(),
        EasyMock.createNiceMock(DruidNodeAnnouncer.class),
        EasyMock.createNiceMock(DruidNode.class),
        new LookupNodeService("tier"),
        new DataNodeService("tier", 1, ServerType.INDEXER_EXECUTOR, 0),
        new SingleFileTaskReportFileWriter(reportsFile),
        null
    );
  }

  private void destroyToolboxFactory()
  {
    toolboxFactory = null;
    taskStorage = null;
    taskLockbox = null;
    metadataStorageCoordinator = null;
  }


  private List<SegmentDescriptor> publishedDescriptors()
  {
    return metadataStorageCoordinator.getUsedSegmentsForInterval(
        DATA_SCHEMA.getDataSource(),
        Intervals.of("0000/3000")
    ).stream().map(DataSegment::toDescriptor).collect(Collectors.toList());
  }

  private void unlockAppenderatorBasePersistDirForTask(KinesisIndexTask task)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
  {
    Method unlockBasePersistDir = ((AppenderatorImpl) task.getAppenderator()).getClass()
                                                                             .getDeclaredMethod(
                                                                                 "unlockBasePersistDirectory");
    unlockBasePersistDir.setAccessible(true);
    unlockBasePersistDir.invoke(task.getAppenderator());
  }

  private File getSegmentDirectory()
  {
    return new File(directory, "segments");
  }


  private List<String> readSegmentColumn(final String column, final SegmentDescriptor descriptor) throws IOException
  {
    File indexBasePath = new File(
        StringUtils.format(
            "%s/%s/%s_%s/%s/%d",
            getSegmentDirectory(),
            DATA_SCHEMA.getDataSource(),
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
    QueryableIndex index = indexIO.loadIndex(outputLocation);
    DictionaryEncodedColumn<String> theColumn = (DictionaryEncodedColumn<String>) index.getColumnHolder(column)
                                                                                       .getColumn();
    List<String> values = new ArrayList<>();
    for (int i = 0; i < theColumn.length(); i++) {
      int id = theColumn.getSingleValueRow(i);
      String value = theColumn.lookupName(id);
      values.add(value);
    }
    return values;
  }

  public long countEvents(final Task task)
  {
    // Do a query.
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(DATA_SCHEMA.getDataSource())
                                  .aggregators(
                                      ImmutableList.of(
                                          new LongSumAggregatorFactory("rows", "rows")
                                      )
                                  ).granularity(Granularities.ALL)
                                  .intervals("0000/3000")
                                  .build();

    List<Result<TimeseriesResultValue>> results = task.getQueryRunner(query).run(QueryPlus.wrap(query)).toList();

    return results.isEmpty() ? 0L : DimensionHandlerUtils.nullToZero(results.get(0).getValue().getLongMetric("rows"));
  }

  private static List<byte[]> jb(
      String timestamp,
      String dim1,
      String dim2,
      String dimLong,
      String dimFloat,
      String met1
  )
  {
    try {
      return Collections.singletonList(new ObjectMapper().writeValueAsBytes(
          ImmutableMap.builder()
                      .put("timestamp", timestamp)
                      .put("dim1", dim1)
                      .put("dim2", dim2)
                      .put("dimLong", dimLong)
                      .put("dimFloat", dimFloat)
                      .put("met1", met1)
                      .build()
      ));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private SegmentDescriptor sd(final String intervalString, final int partitionNum)
  {
    final Interval interval = Intervals.of(intervalString);
    return new SegmentDescriptor(interval, "fakeVersion", partitionNum);
  }

  private void assertEqualsExceptVersion(List<SegmentDescriptor> descriptors1, List<SegmentDescriptor> descriptors2)
  {
    Assert.assertEquals(descriptors1.size(), descriptors2.size());
    final Comparator<SegmentDescriptor> comparator = (s1, s2) -> {
      final int intervalCompare = Comparators.intervalsByStartThenEnd().compare(s1.getInterval(), s2.getInterval());
      if (intervalCompare == 0) {
        return Integer.compare(s1.getPartitionNumber(), s2.getPartitionNumber());
      } else {
        return intervalCompare;
      }
    };

    final List<SegmentDescriptor> copy1 = new ArrayList<>(descriptors1);
    final List<SegmentDescriptor> copy2 = new ArrayList<>(descriptors2);
    copy1.sort(comparator);
    copy2.sort(comparator);

    for (int i = 0; i < copy1.size(); i++) {
      Assert.assertEquals(copy1.get(i).getInterval(), copy2.get(i).getInterval());
      Assert.assertEquals(copy1.get(i).getPartitionNumber(), copy2.get(i).getPartitionNumber());
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

  @JsonTypeName("index_kinesis")
  private static class TestableKinesisIndexTask extends KinesisIndexTask
  {
    private KinesisRecordSupplier localSupplier;

    @JsonCreator
    private TestableKinesisIndexTask(
        @JsonProperty("id") String id,
        @JsonProperty("resource") TaskResource taskResource,
        @JsonProperty("dataSchema") DataSchema dataSchema,
        @JsonProperty("tuningConfig") KinesisIndexTaskTuningConfig tuningConfig,
        @JsonProperty("ioConfig") KinesisIndexTaskIOConfig ioConfig,
        @JsonProperty("context") Map<String, Object> context,
        @JacksonInject ChatHandlerProvider chatHandlerProvider,
        @JacksonInject AuthorizerMapper authorizerMapper,
        @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
        @JacksonInject AWSCredentialsConfig awsCredentialsConfig,
        @JacksonInject AppenderatorsManager appenderatorsManager
    )
    {
      super(
          id,
          taskResource,
          dataSchema,
          tuningConfig,
          ioConfig,
          context,
          chatHandlerProvider,
          authorizerMapper,
          rowIngestionMetersFactory,
          awsCredentialsConfig,
          appenderatorsManager
      );
    }

    private void setLocalSupplier(KinesisRecordSupplier recordSupplier)
    {
      this.localSupplier = recordSupplier;
    }

    @Override
    protected KinesisRecordSupplier newTaskRecordSupplier()
    {
      return localSupplier == null ? recordSupplier : localSupplier;
    }
  }

}

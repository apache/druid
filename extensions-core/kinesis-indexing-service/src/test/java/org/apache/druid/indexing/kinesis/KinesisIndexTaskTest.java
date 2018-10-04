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

import cloud.localstack.Localstack;
import cloud.localstack.TestUtils;
import cloud.localstack.docker.LocalstackDockerTestRunner;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import com.amazonaws.http.SdkHttpMetadata;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.MapCache;
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
import org.apache.druid.indexing.common.Counters;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskReportFileWriter;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.actions.LocalTaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionToolbox;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTaskTest;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.MetadataTaskStorage;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamPartitions;
import org.apache.druid.indexing.seekablestream.common.Record;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.test.TestDataSegmentAnnouncer;
import org.apache.druid.indexing.test.TestDataSegmentKiller;
import org.apache.druid.java.util.common.CompressionUtils;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
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
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoaderLocalCacheManager;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorImpl;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
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

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@RunWith(LocalstackDockerTestRunner.class)
@LocalstackDockerProperties(services = {"kinesis"})
public class KinesisIndexTaskTest
{
  static {
    TestUtils.setEnv("AWS_CBOR_DISABLE", "1");
    if (Localstack.useSSL()) {
      TestUtils.disableSslCertChecking();
    }
  }

  private static final Logger log = new Logger(KinesisIndexTaskTest.class);
  private static final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
  // private static final long POLL_RETRY_MS = 100;
  private static int streamPosFix = 0;
  private static String shardId1 = "shardId-000000000001";
  private static String shardId0 = "shardId-000000000000";
  private static final List<PutRecordsRequestEntry> records = ImmutableList.of(
      generateRequestEntry(
          "1",
          JB("2008", "a", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2009", "b", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2010", "c", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2011", "d", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2011", "e", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0")
      ),
      generateRequestEntry("1", StringUtils.toUtf8("unparseable")),
      generateRequestEntry(
          "1",
          StringUtils.toUtf8("unparseable2")
      ),
      generateRequestEntry("1", "{}".getBytes()),
      generateRequestEntry(
          "1",
          JB("2013", "f", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2049", "f", "y", "notanumber", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2049", "f", "y", "10", "notanumber", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2049", "f", "y", "10", "20.0", "notanumber")
      ),
      generateRequestEntry(
          "123123",
          JB("2012", "g", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "123123",
          JB("2011", "h", "y", "10", "20.0", "1.0")
      )
  );


  private static ServiceEmitter emitter;
  private static ListeningExecutorService taskExec;

  private final List<Task> runningTasks = Lists.newArrayList();

  private long handoffConditionTimeout = 0;
  private boolean reportParseExceptions = false;
  private boolean logParseExceptions = true;
  private Integer maxParseExceptions = null;
  private Integer maxSavedParseExceptions = null;
  private boolean resetOffsetAutomatically = false;
  private boolean doHandoff = true;
  private int maxRowsInMemory = 1000;
  private Long maxTotalRows = null;
  private Period intermediateHandoffPeriod = null;

  private TaskToolboxFactory toolboxFactory;
  private IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private TaskStorage taskStorage;
  private TaskLockbox taskLockbox;
  private File directory;
  private String stream;
  private final boolean isIncrementalHandoffSupported = false;
  private final Set<Integer> checkpointRequestsHash = Sets.newHashSet();
  private File reportsFile;
  private RowIngestionMetersFactory rowIngestionMetersFactory;

  private int handoffCount = 0;

  private static final DataSchema DATA_SCHEMA = new DataSchema(
      "test_ds",
      objectMapper.convertValue(
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
      objectMapper
  );

  private static PutRecordsRequestEntry generateRequestEntry(String partition, byte[] data)
  {
    return new PutRecordsRequestEntry().withPartitionKey(partition)
                                       .withData(ByteBuffer.wrap(data));
  }

  private static PutRecordsRequest generateRecordsRequests(String stream, int first, int last)
  {
    return new PutRecordsRequest()
        .withStreamName(stream)
        .withRecords(records.subList(first, last));
  }

  private static PutRecordsRequest generateRecordsRequests(String stream)
  {
    return new PutRecordsRequest()
        .withStreamName(stream)
        .withRecords(records);
  }

  private static String getStreamName()
  {
    return "stream-" + streamPosFix++;
  }


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
            Execs.makeThreadFactory("kafka-task-test-%d")
        )
    );
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
    stream = getStreamName();
    reportsFile = File.createTempFile("KinesisIndexTaskTestReports-" + System.currentTimeMillis(), "json");

    // sleep required because of kinesalite
    Thread.sleep(500);
    makeToolboxFactory();
  }

  @After
  public void tearDownTest()
  {
    synchronized (runningTasks) {
      for (Task task : runningTasks) {
        task.stopGracefully();
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


  @Test(timeout = 60_000L)
  public void testRunAfterDataInserted() throws Exception
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 2)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 4)
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
            null,
            null,
            null,
            false
        )

    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(new SeekableStreamPartitions<>(
            stream,
            ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 4)
            )
        )),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunBeforeDataInserted() throws Exception
  {
    AmazonKinesis kinesis = getKinesisClientInstance();

    // insert 1 row to get starting seq number
    List<PutRecordsResultEntry> res = insertData(kinesis, new PutRecordsRequest()
        .withStreamName(stream)
        .withRecords(
            ImmutableList.of(
                generateRequestEntry(
                    "123123",
                    JB("2055", "z", "y", "10", "20.0", "1.0")
                )
            )
        )
    );


    final KinesisIndexTask task = createTask(
        null,
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId0,
                getSequenceNumber(res, shardId0, 0)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId0,
                Record.END_OF_SHARD_MARKER
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
            null,
            null,
            null,
            false
        )

    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getStatus() != SeekableStreamIndexTask.Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    List<PutRecordsResultEntry> res2 = insertData(kinesis, generateRecordsRequests(stream));

    // force shard 0 to close
    kinesis.splitShard(stream, shardId0, "somenewshardpls234234234");

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2011/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2012/P1D", 0);
    SegmentDescriptor desc3 = SD(task, "2055/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamPartitions<>(
                stream,
                ImmutableMap.of(
                    shardId0,
                    Record.END_OF_SHARD_MARKER
                )
            )),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("h"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("g"), readSegmentColumn("dim1", desc2));
    Assert.assertEquals(ImmutableList.of("z"), readSegmentColumn("dim1", desc3));
  }


  @Test(timeout = 60_000L)
  public void testRunWithMinimumMessageTime() throws Exception
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 0)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 4)
            )),
            true,
            null,
            DateTimes.of("2010"),
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getStatus() != SeekableStreamIndexTask.Status.READING) {
      Thread.sleep(10);
    }

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(2, task.getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamPartitions<>(
                stream,
                ImmutableMap.of(
                    shardId1,
                    getSequenceNumber(res, shardId1, 4)
                )
            )),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunWithMaximumMessageTime() throws Exception
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 0)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 4)
            )),
            true,
            null,
            null,
            DateTimes.of("2010"),
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getStatus() != SeekableStreamIndexTask.Status.READING) {
      Thread.sleep(10);
    }

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(2, task.getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2008/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2009/P1D", 0);
    SegmentDescriptor desc3 = SD(task, "2010/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamPartitions<>(
                stream,
                ImmutableMap.of(
                    shardId1,
                    getSequenceNumber(res, shardId1, 4)
                )
            )),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("a"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("b"), readSegmentColumn("dim1", desc2));
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc3));
  }

  @Test(timeout = 60_000L)
  public void testRunWithTransformSpec() throws Exception
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

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
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 0)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 4)
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getStatus() != SeekableStreamIndexTask.Status.READING) {
      Thread.sleep(10);
    }

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(1, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(4, task.getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2009/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamPartitions<>(
                stream,
                ImmutableMap.of(
                    shardId1,
                    getSequenceNumber(res, shardId1, 4)
                )
            )),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("b"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("bb"), readSegmentColumn("dim1t", desc1));
  }

  @Test(timeout = 60_000L)
  public void testRunOnNothing() throws Exception
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 2)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 2)
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(0, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRowIngestionMeters().getThrownAway());

    // Check published metadata
    Assert.assertEquals(ImmutableSet.of(), publishedDescriptors());
  }

  @Test(timeout = 60_000L)
  public void testReportParseExceptions() throws Exception
  {
    reportParseExceptions = true;

    // these will be ignored because reportParseExceptions is true
    maxParseExceptions = 1000;
    maxSavedParseExceptions = 2;

    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 2)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 6)
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.FAILED, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(1, task.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRowIngestionMeters().getThrownAway());

    // Check published metadata
    Assert.assertEquals(ImmutableSet.of(), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));
  }

  @Test(timeout = 60_000L)
  public void testMultipleParseExceptionsSuccess() throws Exception
  {
    reportParseExceptions = false;
    maxParseExceptions = 7;
    maxSavedParseExceptions = 7;

    // Insert data
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 2)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 12)
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
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
    Assert.assertEquals(null, status.getErrorMsg());

    // Check metrics
    Assert.assertEquals(4, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(3, task.getRowIngestionMeters().getProcessedWithError());
    Assert.assertEquals(4, task.getRowIngestionMeters().getUnparseable());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    SegmentDescriptor desc3 = SD(task, "2013/P1D", 0);
    SegmentDescriptor desc4 = SD(task, "2049/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3, desc4), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamPartitions<>(
                stream,
                ImmutableMap.of(
                    shardId1,
                    getSequenceNumber(res, shardId1, 12)
                )
            )),
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

  @Test(timeout = 60_000L)
  public void testMultipleParseExceptionsFailure() throws Exception
  {
    reportParseExceptions = false;
    maxParseExceptions = 2;
    maxSavedParseExceptions = 2;

    // Insert data
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 2)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 9)
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
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
    IndexTaskTest.checkTaskStatusErrorMsgForParseExceptionsExceeded(status);

    // Check metrics
    Assert.assertEquals(3, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRowIngestionMeters().getProcessedWithError());
    Assert.assertEquals(3, task.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRowIngestionMeters().getThrownAway());

    // Check published metadata
    Assert.assertEquals(ImmutableSet.of(), publishedDescriptors());
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

  @Test(timeout = 60_000L)
  public void testRunReplicas() throws Exception
  {
    // Insert data
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    final KinesisIndexTask task1 = createTask(
        null,
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 2)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 4)
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
            null,
            null,
            null,
            false
        )
    );
    final KinesisIndexTask task2 = createTask(
        null,
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 2)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 4)
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
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

    // Check metrics
    Assert.assertEquals(3, task1.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(3, task2.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task2.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamPartitions<>(
                stream,
                ImmutableMap.of(
                    shardId1,
                    getSequenceNumber(res, shardId1, 4)
                )
            )),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunConflicting() throws Exception
  {
    // Insert data
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    final KinesisIndexTask task1 = createTask(
        null,
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 2)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 4)
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
            null,
            null,
            null,
            false
        )
    );
    final KinesisIndexTask task2 = createTask(
        null,
        new KinesisIOConfig(
            "sequence1",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 3)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 9)
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
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

    // Check metrics
    Assert.assertEquals(3, task1.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(3, task2.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(4, task2.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata, should all be from the first task
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamPartitions<>(
                stream,
                ImmutableMap.of(
                    shardId1,
                    getSequenceNumber(res, shardId1, 4)
                )
            )),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunConflictingWithoutTransactions() throws Exception
  {
    // Insert data
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    final KinesisIndexTask task1 = createTask(
        null,
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 2)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 4)
            )),
            false,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
            null,
            null,
            null,
            false
        )
    );
    final KinesisIndexTask task2 = createTask(
        null,
        new KinesisIOConfig(
            "sequence1",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 3)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 9)
            )),
            false,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
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
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task1.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(3, task2.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(4, task2.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc3 = SD(task2, "2011/P1D", 1);
    SegmentDescriptor desc4 = SD(task2, "2013/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3, desc4), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc3));
    Assert.assertEquals(ImmutableList.of("f"), readSegmentColumn("dim1", desc4));
  }

  @Test(timeout = 60_000L)
  public void testRunOneTaskTwoPartitions() throws Exception
  {
    // Insert data
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    final KinesisIndexTask task = createTask(
        null,
        new KinesisIOConfig(
            "sequence1",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 2),
                shardId0,
                getSequenceNumber(res, shardId0, 0)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 4),
                shardId0,
                getSequenceNumber(res, shardId0, 1)
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
            null,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);


    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(5, task.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    // desc3 will not be created in KinesisIndexTask (0.12.x) as it does not create per Kafka partition Druid segments
    SegmentDescriptor desc3 = SD(task, "2011/P1D", 1);
    SegmentDescriptor desc4 = SD(task, "2012/P1D", 0);
    Assert.assertEquals(isIncrementalHandoffSupported
                        ? ImmutableSet.of(desc1, desc2, desc4)
                        : ImmutableSet.of(desc1, desc2, desc3, desc4), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(new SeekableStreamPartitions<>(stream, ImmutableMap.of(
            shardId1,
            getSequenceNumber(res, shardId1, 4),
            shardId0,
            getSequenceNumber(res, shardId0, 1)
        ))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("g"), readSegmentColumn("dim1", desc4));

    // Check desc2/desc3 without strong ordering because two partitions are interleaved nondeterministically
    Assert.assertEquals(
        isIncrementalHandoffSupported
        ? ImmutableSet.of(ImmutableList.of("d", "e", "h"))
        : ImmutableSet.of(ImmutableList.of("d", "e"), ImmutableList.of("h")),
        isIncrementalHandoffSupported
        ? ImmutableSet.of(readSegmentColumn("dim1", desc2))
        : ImmutableSet.of(readSegmentColumn("dim1", desc2), readSegmentColumn("dim1", desc3))
    );
  }

  @Test(timeout = 60_000L)
  public void testRunTwoTasksTwoPartitions() throws Exception
  {
    // Insert data
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream));

    final KinesisIndexTask task1 = createTask(
        null,
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 2)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 4)
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
            null,
            null,
            null,
            false
        )
    );
    final KinesisIndexTask task2 = createTask(
        null,
        new KinesisIOConfig(
            "sequence1",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId0,
                getSequenceNumber(res, shardId0, 0)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId0,
                getSequenceNumber(res, shardId0, 1)
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
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

    // Check metrics
    Assert.assertEquals(3, task1.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(2, task2.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task2.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    SegmentDescriptor desc3 = SD(task2, "2011/P1D", 1);
    SegmentDescriptor desc4 = SD(task2, "2012/P1D", 0);

    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3, desc4), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 4),
                shardId0,
                getSequenceNumber(res, shardId0, 1)
            ))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    // Check desc2/desc3 without strong ordering because two partitions are interleaved nondeterministically
    Assert.assertEquals(
        isIncrementalHandoffSupported
        ? ImmutableSet.of(ImmutableList.of("d", "e", "h"))
        : ImmutableSet.of(ImmutableList.of("d", "e"), ImmutableList.of("h")),
        isIncrementalHandoffSupported
        ? ImmutableSet.of(readSegmentColumn("dim1", desc2))
        : ImmutableSet.of(readSegmentColumn("dim1", desc2), readSegmentColumn("dim1", desc3))
    );
    Assert.assertEquals(ImmutableList.of("g"), readSegmentColumn("dim1", desc4));
  }

  @Test(timeout = 60_000L)
  public void testRestore() throws Exception
  {
    // Insert data
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> res = insertData(kinesis, generateRecordsRequests(stream, 0, 4));

    final KinesisIndexTask task1 = createTask(
        "task1",
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 2)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
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
    task1.stopGracefully();
    unlockAppenderatorBasePersistDirForTask(task1);

    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    List<PutRecordsResultEntry> res2 = insertData(kinesis, generateRecordsRequests(stream, 4, 5));

    // Start a new task
    final KinesisIndexTask task2 = createTask(
        task1.getId(),
        new KinesisIOConfig(
            "sequence0",
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                getSequenceNumber(res, shardId1, 2)
            )),
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                Record.END_OF_SHARD_MARKER
            )),
            true,
            null,
            null,
            null,
            Localstack.getEndpointKinesis(),
            null,
            null,
            TestUtils.TEST_ACCESS_KEY,
            TestUtils.TEST_SECRET_KEY,
            ImmutableSet.of(shardId1),
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    while (countEvents(task2) < 1) {
      Thread.sleep(25);
    }

    // force shard to close
    kinesis.splitShard(stream, shardId1, "somerandomshardidhah1213123");

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(2, task1.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(1, task2.getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task2.getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KinesisDataSourceMetadata(
            new SeekableStreamPartitions<>(stream, ImmutableMap.of(
                shardId1,
                Record.END_OF_SHARD_MARKER
            ))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
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
            if (task.isReady(toolbox.getTaskActionClient())) {
              return task.run(toolbox);
            } else {
              throw new ISE("Task is not ready");
            }
          }
          catch (Exception e) {
            log.warn(e, "Task failed");
            return TaskStatus.failure(task.getId(), Throwables.getStackTraceAsString(e));
          }
        }
    );
  }


  private TaskLock getLock(final Task task, final Interval interval)
  {
    return Iterables.find(
        taskLockbox.findLocksForTask(task),
        new Predicate<TaskLock>()
        {
          @Override
          public boolean apply(TaskLock lock)
          {
            return lock.getInterval().contains(interval);
          }
        }
    );
  }

  private KinesisIndexTask createTask(
      final String taskId,
      final KinesisIOConfig ioConfig
  )
  {
    return createTask(taskId, DATA_SCHEMA, ioConfig);
  }

  private KinesisIndexTask createTask(
      final String taskId,
      final KinesisIOConfig ioConfig,
      final Map<String, Object> context
  )
  {
    return createTask(taskId, DATA_SCHEMA, ioConfig, context);
  }

  private KinesisIndexTask createTask(
      final String taskId,
      final DataSchema dataSchema,
      final KinesisIOConfig ioConfig
  )
  {
    final KinesisTuningConfig tuningConfig = new KinesisTuningConfig(
        1000,
        null,
        null,
        new Period("P1Y"),
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
        maxSavedParseExceptions
    );
    final Map<String, Object> context = null;
    final KinesisIndexTask task = new KinesisIndexTask(
        taskId,
        null,
        cloneDataSchema(dataSchema),
        tuningConfig,
        ioConfig,
        context,
        null,
        null,
        rowIngestionMetersFactory
    );
    return task;
  }


  private KinesisIndexTask createTask(
      final String taskId,
      final DataSchema dataSchema,
      final KinesisIOConfig ioConfig,
      final Map<String, Object> context
  )
  {
    final KinesisTuningConfig tuningConfig = new KinesisTuningConfig(
        maxRowsInMemory,
        null,
        null,
        new Period("P1Y"),
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
        maxSavedParseExceptions
    );
    if (isIncrementalHandoffSupported) {
      context.put(SeekableStreamSupervisor.IS_INCREMENTAL_HANDOFF_SUPPORTED, true);
    }

    final KinesisIndexTask task = new KinesisIndexTask(
        taskId,
        null,
        cloneDataSchema(dataSchema),
        tuningConfig,
        ioConfig,
        context,
        null,
        null,
        rowIngestionMetersFactory
    );
    return task;
  }

  private static DataSchema cloneDataSchema(final DataSchema dataSchema)
  {
    return new DataSchema(
        dataSchema.getDataSource(),
        dataSchema.getParserMap(),
        dataSchema.getAggregators(),
        dataSchema.getGranularitySpec(),
        dataSchema.getTransformSpec(),
        objectMapper
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
          QueryRunner<T> delegate, QueryToolChest<T, ? extends Query<T>> toolChest
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
    final org.apache.druid.indexing.common.TestUtils testUtils = new org.apache.druid.indexing.common.TestUtils();
    rowIngestionMetersFactory = testUtils.getRowIngestionMetersFactory();
    final ObjectMapper objectMapper = testUtils.getTestObjectMapper();
    for (Module module : new KinesisIndexingServiceModule().getJacksonModules()) {
      objectMapper.registerModule(module);
    }
    final TaskConfig taskConfig = new TaskConfig(
        new File(directory, "taskBaseDir").getPath(),
        null,
        null,
        50000,
        null,
        false,
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
    taskLockbox = new TaskLockbox(taskStorage);
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
              @Nullable Integer taskGroupId,
              String baseSequenceName,
              @Nullable DataSourceMetadata previousDataSourceMetadata,
              @Nullable DataSourceMetadata currentDataSourceMetadata
          )
          {
            log.info("Adding checkpoint hash to the set");
            checkpointRequestsHash.add(
                Objects.hash(
                    supervisorId,
                    taskGroupId,
                    previousDataSourceMetadata,
                    currentDataSourceMetadata
                )
            );
            return true;
          }
        },
        new Counters()
    );
    final TaskActionClientFactory taskActionClientFactory = new LocalTaskActionClientFactory(
        taskStorage,
        taskActionToolbox
    );
    final SegmentHandoffNotifierFactory handoffNotifierFactory = dataSource -> new SegmentHandoffNotifier()
    {
      @Override
      public boolean registerSegmentHandoffCallback(
          SegmentDescriptor descriptor, Executor exec, Runnable handOffRunnable
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
    final DataSegmentPusher dataSegmentPusher = new LocalDataSegmentPusher(dataSegmentPusherConfig, objectMapper);
    SegmentLoaderConfig segmentLoaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return Lists.newArrayList();
      }
    };
    toolboxFactory = new TaskToolboxFactory(
        taskConfig,
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
        MoreExecutors.sameThreadExecutor(), // queryExecutorService
        EasyMock.createMock(MonitorScheduler.class),
        new SegmentLoaderFactory(
            new SegmentLoaderLocalCacheManager(null, segmentLoaderConfig, testUtils.getTestObjectMapper())
        ),
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
        new TaskReportFileWriter(reportsFile)
    );
  }

  private void destroyToolboxFactory()
  {
    toolboxFactory = null;
    taskStorage = null;
    taskLockbox = null;
    metadataStorageCoordinator = null;
  }


  private Set<SegmentDescriptor> publishedDescriptors()
  {
    return FluentIterable.from(
        metadataStorageCoordinator.getUsedSegmentsForInterval(
            DATA_SCHEMA.getDataSource(),
            Intervals.of("0000/3000")
        )
    ).transform(DataSegment::toDescriptor).toSet();
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
    IndexIO indexIO = new org.apache.druid.indexing.common.TestUtils().getTestIndexIO();
    QueryableIndex index = indexIO.loadIndex(outputLocation);
    DictionaryEncodedColumn<String> theColumn = index.getColumn(column).getDictionaryEncoding();
    List<String> values = Lists.newArrayList();
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

    List<Result<TimeseriesResultValue>> results =
        task.getQueryRunner(query).run(QueryPlus.wrap(query), ImmutableMap.of()).toList();

    return results.isEmpty() ? 0L : DimensionHandlerUtils.nullToZero(results.get(0).getValue().getLongMetric("rows"));
  }

  private static byte[] JB(String timestamp, String dim1, String dim2, String dimLong, String dimFloat, String met1)
  {
    try {
      return new ObjectMapper().writeValueAsBytes(
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
      throw Throwables.propagate(e);
    }
  }

  private SegmentDescriptor SD(final Task task, final String intervalString, final int partitionNum)
  {
    final Interval interval = Intervals.of(intervalString);
    return new SegmentDescriptor(interval, getLock(task, interval).getVersion(), partitionNum);
  }

  private IngestionStatsAndErrorsTaskReportData getTaskReportData() throws IOException
  {
    Map<String, TaskReport> taskReports = objectMapper.readValue(
        reportsFile,
        new TypeReference<Map<String, TaskReport>>()
        {
        }
    );
    return IngestionStatsAndErrorsTaskReportData.getPayloadFromTaskReports(
        taskReports
    );
  }

  private AmazonKinesis getKinesisClientInstance() throws InterruptedException
  {
    AmazonKinesis kinesis = TestUtils.getClientKinesis();
    SdkHttpMetadata createRes = kinesis.createStream(stream, 2).getSdkHttpMetadata();
    // sleep required because of kinesalite
    Thread.sleep(500);
    Assert.assertTrue(isResponseOk(createRes));
    return kinesis;
  }

  private static String getSequenceNumber(List<PutRecordsResultEntry> entries, String shardId, int offset)
  {
    List<PutRecordsResultEntry> sortedEntries = entries.stream()
                                                       .filter(e -> e.getShardId().equals(shardId))
                                                       .sorted(Comparator.comparing(e -> KinesisSequenceNumber.of(e.getSequenceNumber())))
                                                       .collect(Collectors.toList());
    return sortedEntries.get(offset).getSequenceNumber();
  }


  private static boolean isResponseOk(SdkHttpMetadata sdkHttpMetadata)
  {
    return sdkHttpMetadata.getHttpStatusCode() == 200;
  }

  private static List<PutRecordsResultEntry> insertData(
      AmazonKinesis kinesis,
      PutRecordsRequest req
  )
  {
    PutRecordsResult res = kinesis.putRecords(req);
    Assert.assertTrue(isResponseOk(res.getSdkHttpMetadata()));
    Assert.assertEquals((int) res.getFailedRecordCount(), 0);
    return res.getRecords();
  }
}

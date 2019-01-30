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

package org.apache.druid.indexing.kafka;

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
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.test.TestingCluster;
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
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.test.TestBroker;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.MetadataTaskStorage;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner.Status;
import org.apache.druid.indexing.seekablestream.SeekableStreamPartitions;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RunWith(Parameterized.class)
public class KafkaIndexTaskTest
{
  private static final Logger log = new Logger(KafkaIndexTaskTest.class);
  private static final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
  private static final long POLL_RETRY_MS = 100;

  private static TestingCluster zkServer;
  private static TestBroker kafkaServer;
  private static ServiceEmitter emitter;
  private static ListeningExecutorService taskExec;
  private static int topicPostfix;

  private final List<Task> runningTasks = new ArrayList<>();

  private long handoffConditionTimeout = 0;
  private boolean reportParseExceptions = false;
  private boolean logParseExceptions = true;
  private Integer maxParseExceptions = null;
  private Integer maxSavedParseExceptions = null;
  private boolean resetOffsetAutomatically = false;
  private boolean doHandoff = true;
  private Integer maxRowsPerSegment = null;
  private Long maxTotalRows = null;
  private Period intermediateHandoffPeriod = null;

  private TaskToolboxFactory toolboxFactory;
  private IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private TaskStorage taskStorage;
  private TaskLockbox taskLockbox;
  private File directory;
  private String topic;
  private List<ProducerRecord<byte[], byte[]>> records;
  private final boolean isIncrementalHandoffSupported;
  private final Set<Integer> checkpointRequestsHash = new HashSet<>();
  private File reportsFile;
  private RowIngestionMetersFactory rowIngestionMetersFactory;

  private int handoffCount = 0;

  // This should be removed in versions greater that 0.12.x
  // isIncrementalHandoffSupported should always be set to true in those later versions
  @Parameterized.Parameters(name = "isIncrementalHandoffSupported = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{true}, new Object[]{false});
  }

  public KafkaIndexTaskTest(boolean isIncrementalHandoffSupported)
  {
    this.isIncrementalHandoffSupported = isIncrementalHandoffSupported;
  }

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

  private static List<ProducerRecord<byte[], byte[]>> generateRecords(String topic)
  {
    return ImmutableList.of(
        new ProducerRecord<>(topic, 0, null, JB("2008", "a", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, JB("2009", "b", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, JB("2010", "c", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, JB("2011", "d", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, JB("2011", "e", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, JB("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8("unparseable")),
        new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8("unparseable2")),
        new ProducerRecord<>(topic, 0, null, null),
        new ProducerRecord<>(topic, 0, null, JB("2013", "f", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, JB("2049", "f", "y", "notanumber", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, JB("2049", "f", "y", "10", "notanumber", "1.0")),
        new ProducerRecord<>(topic, 0, null, JB("2049", "f", "y", "10", "20.0", "notanumber")),
        new ProducerRecord<>(topic, 1, null, JB("2012", "g", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 1, null, JB("2011", "h", "y", "10", "20.0", "1.0"))
    );
  }

  private static String getTopicName()
  {
    return "topic" + topicPostfix++;
  }

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derby = new TestDerbyConnector.DerbyConnectorRule();

  @BeforeClass
  public static void setupClass() throws Exception
  {
    emitter = new ServiceEmitter(
        "service",
        "host",
        new NoopEmitter()
    );
    emitter.start();
    EmittingLogger.registerEmitter(emitter);

    zkServer = new TestingCluster(1);
    zkServer.start();

    kafkaServer = new TestBroker(
        zkServer.getConnectString(),
        null,
        1,
        ImmutableMap.of("num.partitions", "2")
    );
    kafkaServer.start();

    taskExec = MoreExecutors.listeningDecorator(
        Executors.newCachedThreadPool(
            Execs.makeThreadFactory("kafka-task-test-%d")
        )
    );
  }

  @Before
  public void setupTest() throws IOException
  {
    handoffConditionTimeout = 0;
    reportParseExceptions = false;
    logParseExceptions = true;
    maxParseExceptions = null;
    maxSavedParseExceptions = null;
    doHandoff = true;
    topic = getTopicName();
    records = generateRecords(topic);
    reportsFile = File.createTempFile("KafkaIndexTaskTestReports-" + System.currentTimeMillis(), "json");
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

    kafkaServer.close();
    kafkaServer = null;

    zkServer.stop();
    zkServer = null;

    emitter.close();
  }

  @Test(timeout = 60_000L)
  public void testRunAfterDataInserted() throws Exception
  {
    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunBeforeDataInserted() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getRunner().getStatus() != Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
  }

  @Test(timeout = 60_000L)
  public void testIncrementalHandOff() throws Exception
  {
    if (!isIncrementalHandoffSupported) {
      return;
    }
    final String baseSequenceName = "sequence0";
    // as soon as any segment has more than one record, incremental publishing should happen
    maxRowsPerSegment = 2;

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }
    Map<String, Object> consumerProps = kafkaServer.consumerProperties();
    consumerProps.put("max.poll.records", "1");

    final SeekableStreamPartitions<Integer, Long> startPartitions = new SeekableStreamPartitions<>(
        topic,
        ImmutableMap.of(
            0,
            0L,
            1,
            0L
        )
    );
    // Checkpointing will happen at either checkpoint1 or checkpoint2 depending on ordering
    // of events fetched across two partitions from Kafka
    final SeekableStreamPartitions<Integer, Long> checkpoint1 = new SeekableStreamPartitions<>(
        topic,
        ImmutableMap.of(
            0,
            5L,
            1,
            0L
        )
    );
    final SeekableStreamPartitions<Integer, Long> checkpoint2 = new SeekableStreamPartitions<>(
        topic,
        ImmutableMap.of(
            0,
            4L,
            1,
            2L
        )
    );
    final SeekableStreamPartitions<Integer, Long> endPartitions = new SeekableStreamPartitions<>(
        topic,
        ImmutableMap.of(
            0,
            10L,
            1,
            2L
        )
    );
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            baseSequenceName,
            startPartitions,
            endPartitions,
            consumerProps,
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);
    while (task.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    final Map<Integer, Long> currentOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());
    Assert.assertTrue(checkpoint1.getPartitionSequenceNumberMap().equals(currentOffsets)
                      || checkpoint2.getPartitionSequenceNumberMap()
                                    .equals(currentOffsets));
    task.getRunner().setEndOffsets(currentOffsets, false);
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    Assert.assertEquals(1, checkpointRequestsHash.size());
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                DATA_SCHEMA.getDataSource(),
                0,
                new KafkaDataSourceMetadata(startPartitions),
                new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, currentOffsets))
            )
        )
    );

    // Check metrics
    Assert.assertEquals(8, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(1, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2008/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2009/P1D", 0);
    SegmentDescriptor desc3 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc4 = SD(task, "2011/P1D", 0);
    SegmentDescriptor desc5 = SD(task, "2011/P1D", 1);
    SegmentDescriptor desc6 = SD(task, "2012/P1D", 0);
    SegmentDescriptor desc7 = SD(task, "2013/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3, desc4, desc5, desc6, desc7), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 10L, 1, 2L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("a"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("b"), readSegmentColumn("dim1", desc2));
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc3));
    Assert.assertTrue((ImmutableList.of("d", "e").equals(readSegmentColumn("dim1", desc4))
                       && ImmutableList.of("h").equals(readSegmentColumn("dim1", desc5))) ||
                      (ImmutableList.of("d", "h").equals(readSegmentColumn("dim1", desc4))
                       && ImmutableList.of("e").equals(readSegmentColumn("dim1", desc5))));
    Assert.assertEquals(ImmutableList.of("g"), readSegmentColumn("dim1", desc6));
    Assert.assertEquals(ImmutableList.of("f"), readSegmentColumn("dim1", desc7));
  }

  @Test(timeout = 60_000L)
  public void testIncrementalHandOffMaxTotalRows() throws Exception
  {
    if (!isIncrementalHandoffSupported) {
      return;
    }
    final String baseSequenceName = "sequence0";
    // incremental publish should happen every 3 records
    maxRowsPerSegment = Integer.MAX_VALUE;
    maxTotalRows = 3L;

    // Insert data
    int numToAdd = records.size() - 2;

    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (int i = 0; i < numToAdd; i++) {
        kafkaProducer.send(records.get(i)).get();
      }

      Map<String, Object> consumerProps = kafkaServer.consumerProperties();
      consumerProps.put("max.poll.records", "1");

      final SeekableStreamPartitions<Integer, Long> startPartitions = new SeekableStreamPartitions<>(
          topic,
          ImmutableMap.of(
              0,
              0L,
              1,
              0L
          )
      );
      final SeekableStreamPartitions<Integer, Long> checkpoint1 = new SeekableStreamPartitions<>(
          topic,
          ImmutableMap.of(
              0,
              3L,
              1,
              0L
          )
      );
      final SeekableStreamPartitions<Integer, Long> checkpoint2 = new SeekableStreamPartitions<>(
          topic,
          ImmutableMap.of(
              0,
              10L,
              1,
              0L
          )
      );

      final SeekableStreamPartitions<Integer, Long> endPartitions = new SeekableStreamPartitions<>(
          topic,
          ImmutableMap.of(
              0,
              10L,
              1,
              2L
          )
      );
      final KafkaIndexTask task = createTask(
          null,
          new KafkaIndexTaskIOConfig(
              0,
              baseSequenceName,
              startPartitions,
              endPartitions,
              consumerProps,
              KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
              true,
              null,
              null,
              false
          )
      );
      final ListenableFuture<TaskStatus> future = runTask(task);
      while (task.getRunner().getStatus() != Status.PAUSED) {
        Thread.sleep(10);
      }
      final Map<Integer, Long> currentOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());

      Assert.assertTrue(checkpoint1.getPartitionSequenceNumberMap().equals(currentOffsets));
      task.getRunner().setEndOffsets(currentOffsets, false);

      while (task.getRunner().getStatus() != Status.PAUSED) {
        Thread.sleep(10);
      }

      // add remaining records
      for (int i = numToAdd; i < records.size(); i++) {
        kafkaProducer.send(records.get(i)).get();
      }
      final Map<Integer, Long> nextOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());

      Assert.assertTrue(checkpoint2.getPartitionSequenceNumberMap().equals(nextOffsets));
      task.getRunner().setEndOffsets(nextOffsets, false);

      Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

      Assert.assertEquals(2, checkpointRequestsHash.size());
      Assert.assertTrue(
          checkpointRequestsHash.contains(
              Objects.hash(
                  DATA_SCHEMA.getDataSource(),
                  0,
                  new KafkaDataSourceMetadata(startPartitions),
                  new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, currentOffsets))
              )
          )
      );
      Assert.assertTrue(
          checkpointRequestsHash.contains(
              Objects.hash(
                  DATA_SCHEMA.getDataSource(),
                  0,
                  new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, currentOffsets)),
                  new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, nextOffsets))
              )
          )
      );

      // Check metrics
      Assert.assertEquals(8, task.getRunner().getRowIngestionMeters().getProcessed());
      Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getUnparseable());
      Assert.assertEquals(1, task.getRunner().getRowIngestionMeters().getThrownAway());

      // Check published metadata
      SegmentDescriptor desc1 = SD(task, "2008/P1D", 0);
      SegmentDescriptor desc2 = SD(task, "2009/P1D", 0);
      SegmentDescriptor desc3 = SD(task, "2010/P1D", 0);
      SegmentDescriptor desc4 = SD(task, "2011/P1D", 0);
      SegmentDescriptor desc5 = SD(task, "2011/P1D", 1);
      SegmentDescriptor desc6 = SD(task, "2012/P1D", 0);
      SegmentDescriptor desc7 = SD(task, "2013/P1D", 0);
      Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3, desc4, desc5, desc6, desc7), publishedDescriptors());
      Assert.assertEquals(
          new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 10L, 1, 2L))),
          metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
      );

      // Check segments in deep storage
      Assert.assertEquals(ImmutableList.of("a"), readSegmentColumn("dim1", desc1));
      Assert.assertEquals(ImmutableList.of("b"), readSegmentColumn("dim1", desc2));
      Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc3));
      Assert.assertTrue((ImmutableList.of("d", "e").equals(readSegmentColumn("dim1", desc4))
                         && ImmutableList.of("h").equals(readSegmentColumn("dim1", desc5))) ||
                        (ImmutableList.of("d", "h").equals(readSegmentColumn("dim1", desc4))
                         && ImmutableList.of("e").equals(readSegmentColumn("dim1", desc5))));
      Assert.assertEquals(ImmutableList.of("g"), readSegmentColumn("dim1", desc6));
      Assert.assertEquals(ImmutableList.of("f"), readSegmentColumn("dim1", desc7));
    }
  }

  @Test(timeout = 60_000L)
  public void testTimeBasedIncrementalHandOff() throws Exception
  {
    if (!isIncrementalHandoffSupported) {
      return;
    }
    final String baseSequenceName = "sequence0";
    // as soon as any segment hits maxRowsPerSegment or intermediateHandoffPeriod, incremental publishing should happen
    maxRowsPerSegment = Integer.MAX_VALUE;
    intermediateHandoffPeriod = new Period().withSeconds(0);

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records.subList(0, 2)) {
        kafkaProducer.send(record).get();
      }
    }
    Map<String, Object> consumerProps = kafkaServer.consumerProperties();
    consumerProps.put("max.poll.records", "1");

    final SeekableStreamPartitions<Integer, Long> startPartitions = new SeekableStreamPartitions<>(
        topic,
        ImmutableMap.of(
            0,
            0L,
            1,
            0L
        )
    );
    // Checkpointing will happen at checkpoint
    final SeekableStreamPartitions<Integer, Long> checkpoint = new SeekableStreamPartitions<>(
        topic,
        ImmutableMap.of(
            0,
            1L,
            1,
            0L
        )
    );
    final SeekableStreamPartitions<Integer, Long> endPartitions = new SeekableStreamPartitions<>(
        topic,
        ImmutableMap.of(
            0,
            2L,
            1,
            0L
        )
    );
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            baseSequenceName,
            startPartitions,
            endPartitions,
            consumerProps,
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);

    // task will pause for checkpointing
    while (task.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    final Map<Integer, Long> currentOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());
    Assert.assertTrue(checkpoint.getPartitionSequenceNumberMap().equals(currentOffsets));
    task.getRunner().setEndOffsets(currentOffsets, false);
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    Assert.assertEquals(1, checkpointRequestsHash.size());
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                DATA_SCHEMA.getDataSource(),
                0,
                new KafkaDataSourceMetadata(startPartitions),
                new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(
                    topic,
                    checkpoint.getPartitionSequenceNumberMap()
                ))
            )
        )
    );

    // Check metrics
    Assert.assertEquals(2, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2008/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2009/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L, 1, 0L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("a"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("b"), readSegmentColumn("dim1", desc2));
  }

  @Test(timeout = 60_000L)
  public void testIncrementalHandOffReadsThroughEndOffsets() throws Exception
  {
    if (!isIncrementalHandoffSupported) {
      return;
    }

    List<ProducerRecord<byte[], byte[]>> records = ImmutableList.of(
        new ProducerRecord<>(topic, 0, null, JB("2008", "a", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, JB("2009", "b", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, JB("2010", "c", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, JB("2011", "d", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, JB("2011", "D", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, JB("2012", "e", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, JB("2009", "B", "y", "10", "20.0", "1.0"))
    );

    final String baseSequenceName = "sequence0";
    // as soon as any segment has more than one record, incremental publishing should happen
    maxRowsPerSegment = 2;

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }
    Map<String, Object> consumerProps = kafkaServer.consumerProperties();
    consumerProps.put("max.poll.records", "1");

    final SeekableStreamPartitions<Integer, Long> startPartitions = new SeekableStreamPartitions<>(
        topic,
        ImmutableMap.of(0, 0L)
    );
    final SeekableStreamPartitions<Integer, Long> checkpoint1 = new SeekableStreamPartitions<>(
        topic,
        ImmutableMap.of(0, 5L)
    );
    final SeekableStreamPartitions<Integer, Long> endPartitions = new SeekableStreamPartitions<>(
        topic,
        ImmutableMap.of(0, 7L)
    );

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            baseSequenceName,
            startPartitions,
            endPartitions,
            consumerProps,
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);
    while (task.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    final Map<Integer, Long> currentOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());
    Assert.assertTrue(checkpoint1.getPartitionSequenceNumberMap().equals(currentOffsets));

    // actual checkpoint offset is 5, but simulating behavior of publishing set end offset call, to ensure this task
    // will continue reading through the end offset of the checkpointed sequence
    task.getRunner().setEndOffsets(ImmutableMap.of(0, 6L), true);

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // processed count would be 5 if it stopped at it's current offsets
    Assert.assertEquals(6, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());
  }

  @Test(timeout = 60_000L)
  public void testRunWithMinimumMessageTime() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 0L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            DateTimes.of("2010"),
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getRunner().getStatus() != Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(2, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunWithMaximumMessageTime() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 0L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            DateTimes.of("2010"),
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getRunner().getStatus() != Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(2, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2008/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2009/P1D", 0);
    SegmentDescriptor desc3 = SD(task, "2010/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L))),
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
    final KafkaIndexTask task = createTask(
        null,
        DATA_SCHEMA.withTransformSpec(
            new TransformSpec(
                new SelectorDimFilter("dim1", "b", null),
                ImmutableList.of(
                    new ExpressionTransform("dim1t", "concat(dim1,dim1)", ExprMacroTable.nil())
                )
            )
        ),
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 0L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getRunner().getStatus() != Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(1, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(4, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2009/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("b"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("bb"), readSegmentColumn("dim1t", desc1));
  }

  @Test(timeout = 60_000L)
  public void testRunOnNothing() throws Exception
  {
    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    Assert.assertEquals(ImmutableSet.of(), publishedDescriptors());
  }

  @Test(timeout = 60_000L)
  public void testHandoffConditionTimeoutWhenHandoffOccurs() throws Exception
  {
    handoffConditionTimeout = 5_000;

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
  }

  @Test(timeout = 60_000L)
  public void testHandoffConditionTimeoutWhenHandoffDoesNotOccur() throws Exception
  {
    doHandoff = false;
    handoffConditionTimeout = 100;

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
  }

  @Test(timeout = 60_000L)
  public void testReportParseExceptions() throws Exception
  {
    reportParseExceptions = true;

    // these will be ignored because reportParseExceptions is true
    maxParseExceptions = 1000;
    maxSavedParseExceptions = 2;

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 7L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.FAILED, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(1, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    Assert.assertEquals(ImmutableSet.of(), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));
  }

  @Test(timeout = 60_000L)
  public void testMultipleParseExceptionsSuccess() throws Exception
  {
    reportParseExceptions = false;
    maxParseExceptions = 6;
    maxSavedParseExceptions = 6;

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 13L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
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
    Assert.assertEquals(4, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessedWithError());
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(1, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    SegmentDescriptor desc3 = SD(task, "2013/P1D", 0);
    SegmentDescriptor desc4 = SD(task, "2049/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3, desc4), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 13L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED, 4,
            RowIngestionMeters.PROCESSED_WITH_ERROR, 3,
            RowIngestionMeters.UNPARSEABLE, 3,
            RowIngestionMeters.THROWN_AWAY, 1
        )
    );
    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    Map<String, Object> unparseableEvents = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        Arrays.asList(
            "Found unparseable columns in row: [MapBasedInputRow{timestamp=2049-01-01T00:00:00.000Z, event={timestamp=2049, dim1=f, dim2=y, dimLong=10, dimFloat=20.0, met1=notanumber}, dimensions=[dim1, dim1t, dim2, dimLong, dimFloat]}], exceptions: [Unable to parse value[notanumber] for field[met1],]",
            "Found unparseable columns in row: [MapBasedInputRow{timestamp=2049-01-01T00:00:00.000Z, event={timestamp=2049, dim1=f, dim2=y, dimLong=10, dimFloat=notanumber, met1=1.0}, dimensions=[dim1, dim1t, dim2, dimLong, dimFloat]}], exceptions: [could not convert value [notanumber] to float,]",
            "Found unparseable columns in row: [MapBasedInputRow{timestamp=2049-01-01T00:00:00.000Z, event={timestamp=2049, dim1=f, dim2=y, dimLong=notanumber, dimFloat=20.0, met1=1.0}, dimensions=[dim1, dim1t, dim2, dimLong, dimFloat]}], exceptions: [could not convert value [notanumber] to long,]",
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
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 10L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
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
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getProcessedWithError());
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

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
    final KafkaIndexTask task1 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );
    final KafkaIndexTask task2 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(3, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunConflicting() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );
    final KafkaIndexTask task2 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            1,
            "sequence1",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 3L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 10L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    // Run first task
    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskState.FAILED, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(3, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(3, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(1, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata, should all be from the first task
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunConflictingWithoutTransactions() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            false,
            null,
            null,
            false
        )
    );
    final KafkaIndexTask task2 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            1,
            "sequence1",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 3L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 10L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            false,
            null,
            null,
            false
        )
    );

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

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
    Assert.assertEquals(3, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(3, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(3, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(1, task2.getRunner().getRowIngestionMeters().getThrownAway());

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
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L, 1, 0L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L, 1, 2L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
      kafkaProducer.flush();
    }

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(5, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    // desc3 will not be created in KafkaIndexTask (0.12.x) as it does not create per Kafka partition Druid segments
    SegmentDescriptor desc3 = SD(task, "2011/P1D", 1);
    SegmentDescriptor desc4 = SD(task, "2012/P1D", 0);
    Assert.assertEquals(isIncrementalHandoffSupported
                        ? ImmutableSet.of(desc1, desc2, desc4)
                        : ImmutableSet.of(desc1, desc2, desc3, desc4), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L, 1, 2L))),
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
    final KafkaIndexTask task1 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );
    final KafkaIndexTask task2 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            1,
            "sequence1",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(1, 0L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(1, 1L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(1, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    SegmentDescriptor desc3 = SD(task2, "2012/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L, 1, 1L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
    Assert.assertEquals(ImmutableList.of("g"), readSegmentColumn("dim1", desc3));
  }

  @Test(timeout = 60_000L)
  public void testRestore() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);

    // Insert some data, but not enough for the task to finish
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : Iterables.limit(records, 4)) {
        kafkaProducer.send(record).get();
      }
    }

    while (countEvents(task1) != 2) {
      Thread.sleep(25);
    }

    Assert.assertEquals(2, countEvents(task1));

    // Stop without publishing segment
    task1.stopGracefully(toolboxFactory.build(task1).getConfig());
    unlockAppenderatorBasePersistDirForTask(task1);

    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    // Start a new task
    final KafkaIndexTask task2 = createTask(
        task1.getId(),
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert remaining data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : Iterables.skip(records, 4)) {
        kafkaProducer.send(record).get();
      }
    }

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(2, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(1, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunWithPauseAndResume() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Insert some data, but not enough for the task to finish
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : Iterables.limit(records, 4)) {
        kafkaProducer.send(record).get();
      }
      kafkaProducer.flush();
    }

    while (countEvents(task) != 2) {
      Thread.sleep(25);
    }

    Assert.assertEquals(2, countEvents(task));
    Assert.assertEquals(Status.READING, task.getRunner().getStatus());

    Map<Integer, Long> currentOffsets = objectMapper.readValue(
        task.getRunner().pause().getEntity().toString(),
        new TypeReference<Map<Integer, Long>>()
        {
        }
    );
    Assert.assertEquals(Status.PAUSED, task.getRunner().getStatus());

    // Insert remaining data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : Iterables.skip(records, 4)) {
        kafkaProducer.send(record).get();
      }
    }

    try {
      future.get(10, TimeUnit.SECONDS);
      Assert.fail("Task completed when it should have been paused");
    }
    catch (TimeoutException e) {
      // carry on..
    }

    Assert.assertEquals(currentOffsets, task.getRunner().getCurrentOffsets());

    task.getRunner().resume();

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    Assert.assertEquals(task.getRunner().getEndOffsets(), task.getRunner().getCurrentOffsets());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunWithOffsetOutOfRangeExceptionAndPause() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 2L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    runTask(task);

    while (!task.getRunner().getStatus().equals(Status.READING)) {
      Thread.sleep(2000);
    }

    task.getRunner().pause();

    while (!task.getRunner().getStatus().equals(Status.PAUSED)) {
      Thread.sleep(25);
    }
  }

  @Test(timeout = 60_000L)
  public void testRunWithOffsetOutOfRangeExceptionAndNextOffsetGreaterThanLeastAvailable() throws Exception
  {
    resetOffsetAutomatically = true;
    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 200L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 500L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            false
        )
    );

    runTask(task);

    while (!task.getRunner().getStatus().equals(Status.READING)) {
      Thread.sleep(20);
    }

    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(task.getRunner().getStatus(), Status.READING);
      // Offset should not be reset
      Assert.assertTrue(task.getRunner().getCurrentOffsets().get(0) == 200L);
    }
  }

  @Test(timeout = 60_000L)
  public void testRunContextSequenceAheadOfStartingOffsets() throws Exception
  {
    // This tests the case when a replacement task is created in place of a failed test
    // which has done some incremental handoffs, thus the context will contain starting
    // sequence offsets from which the task should start reading and ignore the start offsets
    if (!isIncrementalHandoffSupported) {
      return;
    }
    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final TreeMap<Integer, Map<Integer, Long>> sequences = new TreeMap<>();
    // Here the sequence number is 1 meaning that one incremental handoff was done by the failed task
    // and this task should start reading from offset 2 for partition 0
    sequences.put(1, ImmutableMap.of(0, 2L));
    final Map<String, Object> context = new HashMap<>();
    context.put("checkpoints", objectMapper.writerWithType(new TypeReference<TreeMap<Integer, Map<Integer, Long>>>()
    {
    }).writeValueAsString(sequences));

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            // task should ignore these and use sequence info sent in the context
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 0L)),
            new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
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
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(topic, ImmutableMap.of(0, 5L))),
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
            return TaskStatus.failure(task.getId());
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

  private KafkaIndexTask createTask(
      final String taskId,
      final KafkaIndexTaskIOConfig ioConfig
  )
  {
    return createTask(taskId, DATA_SCHEMA, ioConfig);
  }

  private KafkaIndexTask createTask(
      final String taskId,
      final KafkaIndexTaskIOConfig ioConfig,
      final Map<String, Object> context
  )
  {
    return createTask(taskId, DATA_SCHEMA, ioConfig, context);
  }

  private KafkaIndexTask createTask(
      final String taskId,
      final DataSchema dataSchema,
      final KafkaIndexTaskIOConfig ioConfig
  )
  {
    final KafkaIndexTaskTuningConfig tuningConfig = new KafkaIndexTaskTuningConfig(
        1000,
        null,
        maxRowsPerSegment,
        maxTotalRows,
        new Period("P1Y"),
        null,
        null,
        null,
        true,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        null,
        intermediateHandoffPeriod,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions
    );
    final Map<String, Object> context = isIncrementalHandoffSupported
                                        ? ImmutableMap.of(
        SeekableStreamSupervisor.IS_INCREMENTAL_HANDOFF_SUPPORTED,
        true
    )
                                        : null;
    final KafkaIndexTask task = new KafkaIndexTask(
        taskId,
        null,
        cloneDataSchema(dataSchema),
        tuningConfig,
        ioConfig,
        context,
        null,
        null,
        rowIngestionMetersFactory,
        objectMapper
    );
    task.setPollRetryMs(POLL_RETRY_MS);
    return task;
  }


  private KafkaIndexTask createTask(
      final String taskId,
      final DataSchema dataSchema,
      final KafkaIndexTaskIOConfig ioConfig,
      final Map<String, Object> context
  )
  {
    final KafkaIndexTaskTuningConfig tuningConfig = new KafkaIndexTaskTuningConfig(
        1000,
        null,
        maxRowsPerSegment,
        null,
        new Period("P1Y"),
        null,
        null,
        null,
        true,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        null,
        null,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions
    );
    if (isIncrementalHandoffSupported) {
      context.put(SeekableStreamSupervisor.IS_INCREMENTAL_HANDOFF_SUPPORTED, true);
    }

    final KafkaIndexTask task = new KafkaIndexTask(
        taskId,
        null,
        cloneDataSchema(dataSchema),
        tuningConfig,
        ioConfig,
        context,
        null,
        null,
        rowIngestionMetersFactory,
        objectMapper
    );
    task.setPollRetryMs(POLL_RETRY_MS);
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
      public <T> QueryRunner<T> decorate(QueryRunner<T> delegate, QueryToolChest<T, ? extends Query<T>> toolChest)
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
    for (Module module : new KafkaIndexTaskModule().getJacksonModules()) {
      objectMapper.registerModule(module);
    }
    final TaskConfig taskConfig = new TaskConfig(
        new File(directory, "taskBaseDir").getPath(),
        null,
        null,
        50000,
        null,
        true,
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
    final DataSegmentPusher dataSegmentPusher = new LocalDataSegmentPusher(dataSegmentPusherConfig, objectMapper);
    SegmentLoaderConfig segmentLoaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return new ArrayList<>();
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
        Execs.directExecutor(), // queryExecutorService
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

  private void unlockAppenderatorBasePersistDirForTask(KafkaIndexTask task)
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
}

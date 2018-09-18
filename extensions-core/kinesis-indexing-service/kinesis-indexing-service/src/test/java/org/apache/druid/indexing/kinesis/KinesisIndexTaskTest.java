///*
// * Licensed to Metamarkets Group Inc. (Metamarkets) under one
// * or more contributor license agreements. See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership. Metamarkets licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License. You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied. See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package org.apache.druid.indexing.kinesis;
//
//import com.fasterxml.jackson.core.type.TypeReference;
//import com.fasterxml.jackson.databind.Module;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.google.common.base.Charsets;
//import com.google.common.base.Function;
//import com.google.common.base.Predicate;
//import com.google.common.base.Predicates;
//import com.google.common.base.Throwables;
//import com.google.common.collect.FluentIterable;
//import com.google.common.collect.ImmutableList;
//import com.google.common.collect.ImmutableMap;
//import com.google.common.collect.ImmutableSet;
//import com.google.common.collect.Iterables;
//import com.google.common.collect.Lists;
//import com.google.common.io.Files;
//import com.google.common.util.concurrent.ListenableFuture;
//import com.google.common.util.concurrent.ListeningExecutorService;
//import com.google.common.util.concurrent.MoreExecutors;
//import com.metamx.common.logger.Logger;
//import com.metamx.emitter.EmittingLogger;
//import com.metamx.emitter.core.LoggingEmitter;
//import com.metamx.emitter.service.ServiceEmitter;
//import com.metamx.metrics.MonitorScheduler;
//import org.apache.druid.client.cache.CacheConfig;
//import org.apache.druid.client.cache.MapCache;
//import org.apache.druid.concurrent.Execs;
//import org.apache.druid.data.input.impl.DimensionsSpec;
//import org.apache.druid.data.input.impl.JSONParseSpec;
//import org.apache.druid.data.input.impl.JSONPathFieldSpec;
//import org.apache.druid.data.input.impl.JSONPathSpec;
//import org.apache.druid.data.input.impl.StringInputRowParser;
//import org.apache.druid.data.input.impl.TimestampSpec;
//import org.apache.druid.indexing.common.SegmentLoaderFactory;
//import org.apache.druid.indexing.common.TaskLock;
//import org.apache.druid.indexing.common.TaskStatus;
//import org.apache.druid.indexing.common.TaskToolbox;
//import org.apache.druid.indexing.common.TaskToolboxFactory;
//import org.apache.druid.indexing.common.TestUtils;
//import org.apache.druid.indexing.common.actions.LocalTaskActionClientFactory;
//import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
//import org.apache.druid.indexing.common.actions.TaskActionToolbox;
//import org.apache.druid.indexing.common.config.TaskConfig;
//import org.apache.druid.indexing.common.config.TaskStorageConfig;
//import org.apache.druid.indexing.common.task.Task;
//import org.apache.druid.indexing.kinesis.test.TestBroker;
//import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
//import org.apache.druid.indexing.overlord.MetadataTaskStorage;
//import org.apache.druid.indexing.overlord.TaskLockbox;
//import org.apache.druid.indexing.overlord.TaskStorage;
//import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
//import org.apache.druid.indexing.test.TestDataSegmentAnnouncer;
//import org.apache.druid.indexing.test.TestDataSegmentKiller;
//import org.apache.druid.jackson.DefaultObjectMapper;
//import org.apache.druid.java.util.common.CompressionUtils;
//import org.apache.druid.java.util.common.ISE;
//import org.apache.druid.java.util.common.granularity.Granularities;
//import org.apache.druid.java.util.common.guava.Sequences;
//import org.apache.druid.metadata.EntryExistsException;
//import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
//import org.apache.druid.metadata.SQLMetadataStorageActionHandlerFactory;
//import org.apache.druid.metadata.TestDerbyConnector;
//import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
//import org.apache.druid.query.Druids;
//import org.apache.druid.query.IntervalChunkingQueryRunnerDecorator;
//import org.apache.druid.query.Query;
//import org.apache.druid.query.QueryRunner;
//import org.apache.druid.query.QueryRunnerFactory;
//import org.apache.druid.query.QueryRunnerFactoryConglomerate;
//import org.apache.druid.query.QueryToolChest;
//import org.apache.druid.query.QueryWatcher;
//import org.apache.druid.query.Result;
//import org.apache.druid.query.SegmentDescriptor;
//import org.apache.druid.query.aggregation.AggregatorFactory;
//import org.apache.druid.query.aggregation.CountAggregatorFactory;
//import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
//import org.apache.druid.query.timeseries.TimeseriesQuery;
//import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
//import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
//import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
//import org.apache.druid.query.timeseries.TimeseriesResultValue;
//import org.apache.druid.segment.IndexIO;
//import org.apache.druid.segment.QueryableIndex;
//import org.apache.druid.segment.column.DictionaryEncodedColumn;
//import org.apache.druid.segment.indexing.DataSchema;
//import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
//import org.apache.druid.segment.loading.DataSegmentPusher;
//import org.apache.druid.segment.loading.LocalDataSegmentPusher;
//import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
//import org.apache.druid.segment.loading.SegmentLoaderConfig;
//import org.apache.druid.segment.loading.SegmentLoaderLocalCacheManager;
//import org.apache.druid.segment.loading.StorageLocationConfig;
//import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifier;
//import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
//import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
//import org.apache.druid.timeline.DataSegment;
//import org.apache.curator.test.TestingCluster;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.easymock.EasyMock;
//import org.joda.time.DateTime;
//import org.joda.time.Interval;
//import org.joda.time.Period;
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.rules.TemporaryFolder;
//import org.junit.runner.RunWith;
//import org.junit.runners.Parameterized;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.Callable;
//import java.util.concurrent.Executor;
//import java.util.concurrent.Executors;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//
//@RunWith(Parameterized.class)
//public class KinesisIndexTaskTest
//{
//  private final boolean buildV9Directly;
//  private long handoffConditionTimeout = 0;
//  private boolean reportParseExceptions = false;
//  private boolean doHandoff = true;
//
//  private TestingCluster zkServer;
//  private TestBroker kafkaServer;
//  private ServiceEmitter emitter;
//  private ListeningExecutorService taskExec;
//  private TaskToolboxFactory toolboxFactory;
//  private IndexerMetadataStorageCoordinator metadataStorageCoordinator;
//  private TaskStorage taskStorage;
//  private TaskLockbox taskLockbox;
//  private File directory;
//
//  private final List<Task> runningTasks = Lists.newArrayList();
//
//  private static final Logger log = new Logger(KinesisIndexTaskTest.class);
//  private static final ObjectMapper objectMapper = new DefaultObjectMapper();
//
//  private static final DataSchema DATA_SCHEMA;
//
//  private static final List<ProducerRecord<byte[], byte[]>> RECORDS = ImmutableList.of(
//      new ProducerRecord<byte[], byte[]>("topic0", 0, null, JB("2008", "a", "y", 1.0f)),
//      new ProducerRecord<byte[], byte[]>("topic0", 0, null, JB("2009", "b", "y", 1.0f)),
//      new ProducerRecord<byte[], byte[]>("topic0", 0, null, JB("2010", "c", "y", 1.0f)),
//      new ProducerRecord<byte[], byte[]>("topic0", 0, null, JB("2011", "d", "y", 1.0f)),
//      new ProducerRecord<byte[], byte[]>("topic0", 0, null, JB("2011", "e", "y", 1.0f)),
//      new ProducerRecord<byte[], byte[]>("topic0", 0, null, "unparseable".getBytes()),
//      new ProducerRecord<byte[], byte[]>("topic0", 0, null, null),
//      new ProducerRecord<byte[], byte[]>("topic0", 0, null, JB("2013", "f", "y", 1.0f)),
//      new ProducerRecord<byte[], byte[]>("topic0", 1, null, JB("2012", "g", "y", 1.0f)),
//      new ProducerRecord<byte[], byte[]>("topic0", 1, null, JB("2011", "h", "y", 1.0f))
//  );
//
//  static {
//    DATA_SCHEMA = new DataSchema(
//        "test_ds",
//        objectMapper.convertValue(
//            new StringInputRowParser(
//                new JSONParseSpec(
//                    new TimestampSpec("timestamp", "iso", null),
//                    new DimensionsSpec(
//                        DimensionsSpec.getDefaultSchemas(ImmutableList.<String>of("dim1", "dim2")),
//                        null,
//                        null
//                    ),
//                    new JSONPathSpec(true, ImmutableList.<JSONPathFieldSpec>of()),
//                    ImmutableMap.<String, Boolean>of()
//                ),
//                Charsets.UTF_8.name()
//            ),
//            Map.class
//        ),
//        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
//        new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
//        objectMapper
//    );
//  }
//
//  @Parameterized.Parameters(name = "buildV9Directly = {0}")
//  public static Iterable<Object[]> constructorFeeder()
//  {
//    return ImmutableList.of(new Object[]{true}, new Object[]{false});
//  }
//
//  public KinesisIndexTaskTest(boolean buildV9Directly)
//  {
//    this.buildV9Directly = buildV9Directly;
//  }
//
//  @Rule
//  public final TemporaryFolder tempFolder = new TemporaryFolder();
//
//  @Rule
//  public final TestDerbyConnector.DerbyConnectorRule derby = new TestDerbyConnector.DerbyConnectorRule();
//
//  @Before
//  public void setUp() throws Exception
//  {
//    emitter = new ServiceEmitter(
//        "service",
//        "host",
//        new LoggingEmitter(
//            log,
//            LoggingEmitter.Level.ERROR,
//            new DefaultObjectMapper()
//        )
//    );
//    emitter.start();
//    EmittingLogger.registerEmitter(emitter);
//
//    makeToolboxFactory();
//
//    zkServer = new TestingCluster(1);
//    zkServer.start();
//
//    kafkaServer = new TestBroker(
//        zkServer.getConnectString(),
//        tempFolder.newFolder(),
//        1,
//        ImmutableMap.of("num.partitions", "2")
//    );
//    kafkaServer.start();
//
//    taskExec = MoreExecutors.listeningDecorator(
//        Executors.newCachedThreadPool(
//            Execs.makeThreadFactory("kinesis-task-test-%d")
//        )
//    );
//
//    handoffConditionTimeout = 0;
//    reportParseExceptions = false;
//    doHandoff = true;
//  }
//
//  @After
//  public void tearDown() throws Exception
//  {
//    emitter.close();
//
//    synchronized (runningTasks) {
//      for (Task task : runningTasks) {
//        task.stopGracefully();
//      }
//
//      runningTasks.clear();
//    }
//
//    taskExec.shutdown();
//    taskExec.awaitTermination(9999, TimeUnit.DAYS);
//
//    kafkaServer.close();
//    kafkaServer = null;
//
//    zkServer.stop();
//    zkServer = null;
//
//    destroyToolboxFactory();
//  }
//
//  @Test(timeout = 60_000L)
//  public void testRunAfterDataInserted() throws Exception
//  {
//    // Insert data
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    final KinesisIndexTask task = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 5L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    final ListenableFuture<TaskStatus> future = runTask(task);
//
//    // Wait for task to exit
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get().getStatusCode());
//
//    // Check metrics
//    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());
//
//    // Check published metadata
//    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
//    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
//    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
//    Assert.assertEquals(
//        new KinesisDataSourceMetadata(new KinesisPartitions("topic0", ImmutableMap.of(0, 5L))),
//        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
//    );
//
//    // Check segments in deep storage
//    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
//    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
//  }
//
//  @Test(timeout = 60_000L)
//  public void testRunBeforeDataInserted() throws Exception
//  {
//    final KinesisIndexTask task = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 5L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    final ListenableFuture<TaskStatus> future = runTask(task);
//
//    // Wait for the task to start reading
//    while (task.getStatus() != KinesisIndexTask.Status.READING) {
//      Thread.sleep(10);
//    }
//
//    // Insert data
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    // Wait for task to exit
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get().getStatusCode());
//
//    // Check metrics
//    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());
//
//    // Check published metadata
//    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
//    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
//    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
//    Assert.assertEquals(
//        new KinesisDataSourceMetadata(new KinesisPartitions("topic0", ImmutableMap.of(0, 5L))),
//        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
//    );
//
//    // Check segments in deep storage
//    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
//    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
//  }
//
//  @Test(timeout = 60_000L)
//  public void testRunWithMinimumMessageTime() throws Exception
//  {
//    final KinesisIndexTask task = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 0L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 5L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            new DateTime("2010"),
//            false
//        ),
//        null,
//        null
//    );
//
//    final ListenableFuture<TaskStatus> future = runTask(task);
//
//    // Wait for the task to start reading
//    while (task.getStatus() != KinesisIndexTask.Status.READING) {
//      Thread.sleep(10);
//    }
//
//    // Insert data
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    // Wait for task to exit
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get().getStatusCode());
//
//    // Check metrics
//    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(2, task.getFireDepartmentMetrics().thrownAway());
//
//    // Check published metadata
//    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
//    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
//    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
//    Assert.assertEquals(
//        new KinesisDataSourceMetadata(new KinesisPartitions("topic0", ImmutableMap.of(0, 5L))),
//        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
//    );
//
//    // Check segments in deep storage
//    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
//    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
//  }
//
//  @Test(timeout = 60_000L)
//  public void testRunOnNothing() throws Exception
//  {
//    // Insert data
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    final KinesisIndexTask task = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    final ListenableFuture<TaskStatus> future = runTask(task);
//
//    // Wait for task to exit
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get().getStatusCode());
//
//    // Check metrics
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());
//
//    // Check published metadata
//    Assert.assertEquals(ImmutableSet.of(), publishedDescriptors());
//  }
//
//  @Test(timeout = 60_000L)
//  public void testHandoffConditionTimeoutWhenHandoffOccurs() throws Exception
//  {
//    handoffConditionTimeout = 5_000;
//
//    // Insert data
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    final KinesisIndexTask task = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 5L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    final ListenableFuture<TaskStatus> future = runTask(task);
//
//    // Wait for task to exit
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get().getStatusCode());
//
//    // Check metrics
//    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());
//
//    // Check published metadata
//    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
//    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
//    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
//    Assert.assertEquals(
//        new KinesisDataSourceMetadata(new KinesisPartitions("topic0", ImmutableMap.of(0, 5L))),
//        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
//    );
//
//    // Check segments in deep storage
//    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
//    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
//  }
//
//  @Test(timeout = 60_000L)
//  public void testHandoffConditionTimeoutWhenHandoffDoesNotOccur() throws Exception
//  {
//    doHandoff = false;
//    handoffConditionTimeout = 100;
//
//    // Insert data
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    final KinesisIndexTask task = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 5L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    final ListenableFuture<TaskStatus> future = runTask(task);
//
//    // Wait for task to exit
//    Assert.assertEquals(TaskStatus.Status.FAILED, future.get().getStatusCode());
//
//    // Check metrics
//    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());
//
//    // Check published metadata
//    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
//    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
//    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
//    Assert.assertEquals(
//        new KinesisDataSourceMetadata(new KinesisPartitions("topic0", ImmutableMap.of(0, 5L))),
//        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
//    );
//
//    // Check segments in deep storage
//    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
//    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
//  }
//
//  @Test(timeout = 60_000L)
//  public void testReportParseExceptions() throws Exception
//  {
//    reportParseExceptions = true;
//
//    // Insert data
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    final KinesisIndexTask task = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 7L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    final ListenableFuture<TaskStatus> future = runTask(task);
//
//    // Wait for task to exit
//    Assert.assertEquals(TaskStatus.Status.FAILED, future.get().getStatusCode());
//
//    // Check metrics
//    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());
//
//    // Check published metadata
//    Assert.assertEquals(ImmutableSet.of(), publishedDescriptors());
//    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));
//  }
//
//  @Test(timeout = 60_000L)
//  public void testRunReplicas() throws Exception
//  {
//    final KinesisIndexTask task1 = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 5L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//    final KinesisIndexTask task2 = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 5L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    final ListenableFuture<TaskStatus> future1 = runTask(task1);
//    final ListenableFuture<TaskStatus> future2 = runTask(task2);
//
//    // Insert data
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    // Wait for tasks to exit
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future1.get().getStatusCode());
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future2.get().getStatusCode());
//
//    // Check metrics
//    Assert.assertEquals(3, task1.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task1.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task1.getFireDepartmentMetrics().thrownAway());
//    Assert.assertEquals(3, task2.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task2.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task2.getFireDepartmentMetrics().thrownAway());
//
//    // Check published segments & metadata
//    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
//    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
//    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
//    Assert.assertEquals(
//        new KinesisDataSourceMetadata(new KinesisPartitions("topic0", ImmutableMap.of(0, 5L))),
//        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
//    );
//
//    // Check segments in deep storage
//    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
//    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
//  }
//
//  @Test(timeout = 60_000L)
//  public void testRunConflicting() throws Exception
//  {
//    final KinesisIndexTask task1 = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 5L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//    final KinesisIndexTask task2 = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence1",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 3L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 8L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    // Insert data
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    // Run first task
//    final ListenableFuture<TaskStatus> future1 = runTask(task1);
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future1.get().getStatusCode());
//
//    // Run second task
//    final ListenableFuture<TaskStatus> future2 = runTask(task2);
//    Assert.assertEquals(TaskStatus.Status.FAILED, future2.get().getStatusCode());
//
//    // Check metrics
//    Assert.assertEquals(3, task1.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task1.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task1.getFireDepartmentMetrics().thrownAway());
//    Assert.assertEquals(3, task2.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(2, task2.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task2.getFireDepartmentMetrics().thrownAway());
//
//    // Check published segments & metadata, should all be from the first task
//    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
//    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
//    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
//    Assert.assertEquals(
//        new KinesisDataSourceMetadata(new KinesisPartitions("topic0", ImmutableMap.of(0, 5L))),
//        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
//    );
//
//    // Check segments in deep storage
//    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
//    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
//  }
//
//  @Test(timeout = 60_000L)
//  public void testRunConflictingWithoutTransactions() throws Exception
//  {
//    final KinesisIndexTask task1 = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 5L)),
//            kafkaServer.consumerProperties(),
//            false,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//    final KinesisIndexTask task2 = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence1",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 3L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 8L)),
//            kafkaServer.consumerProperties(),
//            false,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    // Insert data
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    // Run first task
//    final ListenableFuture<TaskStatus> future1 = runTask(task1);
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future1.get().getStatusCode());
//
//    // Check published segments & metadata
//    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
//    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
//    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
//    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));
//
//    // Run second task
//    final ListenableFuture<TaskStatus> future2 = runTask(task2);
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future2.get().getStatusCode());
//
//    // Check metrics
//    Assert.assertEquals(3, task1.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task1.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task1.getFireDepartmentMetrics().thrownAway());
//    Assert.assertEquals(3, task2.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(2, task2.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task2.getFireDepartmentMetrics().thrownAway());
//
//    // Check published segments & metadata
//    SegmentDescriptor desc3 = SD(task2, "2011/P1D", 1);
//    SegmentDescriptor desc4 = SD(task2, "2013/P1D", 0);
//    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3, desc4), publishedDescriptors());
//    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));
//
//    // Check segments in deep storage
//    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
//    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
//    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc3));
//    Assert.assertEquals(ImmutableList.of("f"), readSegmentDim1(desc4));
//  }
//
//  @Test(timeout = 60_000L)
//  public void testRunOneTaskTwoPartitions() throws Exception
//  {
//    final KinesisIndexTask task = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L, 1, 0L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 5L, 1, 2L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    final ListenableFuture<TaskStatus> future = runTask(task);
//
//    // Insert data
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    // Wait for tasks to exit
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get().getStatusCode());
//
//    // Check metrics
//    Assert.assertEquals(5, task.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());
//
//    // Check published segments & metadata
//    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
//    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
//    SegmentDescriptor desc3 = SD(task, "2011/P1D", 1);
//    SegmentDescriptor desc4 = SD(task, "2012/P1D", 0);
//    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3, desc4), publishedDescriptors());
//    Assert.assertEquals(
//        new KinesisDataSourceMetadata(new KinesisPartitions("topic0", ImmutableMap.of(0, 5L, 1, 2L))),
//        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
//    );
//
//    // Check segments in deep storage
//    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
//    Assert.assertEquals(ImmutableList.of("g"), readSegmentDim1(desc4));
//
//    // Check desc2/desc3 without strong ordering because two partitions are interleaved nondeterministically
//    Assert.assertEquals(
//        ImmutableSet.of(ImmutableList.of("d", "e"), ImmutableList.of("h")),
//        ImmutableSet.of(readSegmentDim1(desc2), readSegmentDim1(desc3))
//    );
//  }
//
//  @Test(timeout = 60_000L)
//  public void testRunTwoTasksTwoPartitions() throws Exception
//  {
//    final KinesisIndexTask task1 = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 5L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//    final KinesisIndexTask task2 = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence1",
//            new KinesisPartitions("topic0", ImmutableMap.of(1, 0L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(1, 1L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    final ListenableFuture<TaskStatus> future1 = runTask(task1);
//    final ListenableFuture<TaskStatus> future2 = runTask(task2);
//
//    // Insert data
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    // Wait for tasks to exit
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future1.get().getStatusCode());
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future2.get().getStatusCode());
//
//    // Check metrics
//    Assert.assertEquals(3, task1.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task1.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task1.getFireDepartmentMetrics().thrownAway());
//    Assert.assertEquals(1, task2.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task2.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task2.getFireDepartmentMetrics().thrownAway());
//
//    // Check published segments & metadata
//    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
//    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
//    SegmentDescriptor desc3 = SD(task2, "2012/P1D", 0);
//    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());
//    Assert.assertEquals(
//        new KinesisDataSourceMetadata(new KinesisPartitions("topic0", ImmutableMap.of(0, 5L, 1, 1L))),
//        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
//    );
//
//    // Check segments in deep storage
//    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
//    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
//    Assert.assertEquals(ImmutableList.of("g"), readSegmentDim1(desc3));
//  }
//
//  @Test(timeout = 60_000L)
//  public void testRestore() throws Exception
//  {
//    final KinesisIndexTask task1 = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 5L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    final ListenableFuture<TaskStatus> future1 = runTask(task1);
//
//    // Insert some data, but not enough for the task to finish
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : Iterables.limit(RECORDS, 4)) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    while (countEvents(task1) != 2) {
//      Thread.sleep(25);
//    }
//
//    Assert.assertEquals(2, countEvents(task1));
//
//    // Stop without publishing segment
//    task1.stopGracefully();
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future1.get().getStatusCode());
//
//    // Start a new task
//    final KinesisIndexTask task2 = createTask(
//        task1.getId(),
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 5L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    final ListenableFuture<TaskStatus> future2 = runTask(task2);
//
//    // Insert remaining data
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : Iterables.skip(RECORDS, 4)) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    // Wait for task to exit
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future2.get().getStatusCode());
//
//    // Check metrics
//    Assert.assertEquals(2, task1.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task1.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task1.getFireDepartmentMetrics().thrownAway());
//    Assert.assertEquals(1, task2.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task2.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task2.getFireDepartmentMetrics().thrownAway());
//
//    // Check published segments & metadata
//    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
//    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
//    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
//    Assert.assertEquals(
//        new KinesisDataSourceMetadata(new KinesisPartitions("topic0", ImmutableMap.of(0, 5L))),
//        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
//    );
//
//    // Check segments in deep storage
//    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
//    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
//  }
//
//  @Test(timeout = 60_000L)
//  public void testRunWithPauseAndResume() throws Exception
//  {
//    final KinesisIndexTask task = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 5L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    final ListenableFuture<TaskStatus> future = runTask(task);
//
//    // Insert some data, but not enough for the task to finish
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : Iterables.limit(RECORDS, 4)) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    while (countEvents(task) != 2) {
//      Thread.sleep(25);
//    }
//
//    Assert.assertEquals(2, countEvents(task));
//    Assert.assertEquals(KinesisIndexTask.Status.READING, task.getStatus());
//
//    Map<Integer, Long> currentOffsets = objectMapper.readValue(
//        task.pause(0).getEntity().toString(),
//        new TypeReference<Map<Integer, Long>>()
//        {
//        }
//    );
//    Assert.assertEquals(KinesisIndexTask.Status.PAUSED, task.getStatus());
//
//    // Insert remaining data
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : Iterables.skip(RECORDS, 4)) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    try {
//      future.get(10, TimeUnit.SECONDS);
//      Assert.fail("Task completed when it should have been paused");
//    }
//    catch (TimeoutException e) {
//      // carry on..
//    }
//
//    Assert.assertEquals(currentOffsets, task.getCurrentOffsets());
//
//    task.resume();
//
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get().getStatusCode());
//    Assert.assertEquals(task.getEndOffsets(), task.getCurrentOffsets());
//
//    // Check metrics
//    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());
//
//    // Check published metadata
//    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
//    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
//    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
//    Assert.assertEquals(
//        new KinesisDataSourceMetadata(new KinesisPartitions("topic0", ImmutableMap.of(0, 5L))),
//        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
//    );
//
//    // Check segments in deep storage
//    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
//    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
//  }
//
//  @Test(timeout = 60_000L)
//  public void testRunAndPauseAfterReadWithModifiedEndOffsets() throws Exception
//  {
//    final KinesisIndexTask task = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 1L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 3L)),
//            kafkaServer.consumerProperties(),
//            true,
//            true,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    final ListenableFuture<TaskStatus> future = runTask(task);
//
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    while (task.getStatus() != KinesisIndexTask.Status.PAUSED) {
//      Thread.sleep(25);
//    }
//
//    // reached the end of the assigned offsets and paused instead of publishing
//    Assert.assertEquals(task.getEndOffsets(), task.getCurrentOffsets());
//    Assert.assertEquals(KinesisIndexTask.Status.PAUSED, task.getStatus());
//
//    Assert.assertEquals(ImmutableMap.of(0, 3L), task.getEndOffsets());
//    Map<Integer, Long> newEndOffsets = ImmutableMap.of(0, 4L);
//    task.setEndOffsets(newEndOffsets, false);
//    Assert.assertEquals(newEndOffsets, task.getEndOffsets());
//    Assert.assertEquals(KinesisIndexTask.Status.PAUSED, task.getStatus());
//    task.resume();
//
//    while (task.getStatus() != KinesisIndexTask.Status.PAUSED) {
//      Thread.sleep(25);
//    }
//
//    // reached the end of the updated offsets and paused
//    Assert.assertEquals(newEndOffsets, task.getCurrentOffsets());
//    Assert.assertEquals(KinesisIndexTask.Status.PAUSED, task.getStatus());
//
//    // try again but with resume flag == true
//    newEndOffsets = ImmutableMap.of(0, 6L);
//    task.setEndOffsets(newEndOffsets, true);
//    Assert.assertEquals(newEndOffsets, task.getEndOffsets());
//    Assert.assertNotEquals(KinesisIndexTask.Status.PAUSED, task.getStatus());
//
//    while (task.getStatus() != KinesisIndexTask.Status.PAUSED) {
//      Thread.sleep(25);
//    }
//
//    Assert.assertEquals(newEndOffsets, task.getCurrentOffsets());
//    Assert.assertEquals(KinesisIndexTask.Status.PAUSED, task.getStatus());
//
//    task.resume();
//
//    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get().getStatusCode());
//
//    // Check metrics
//    Assert.assertEquals(4, task.getFireDepartmentMetrics().processed());
//    Assert.assertEquals(1, task.getFireDepartmentMetrics().unparseable());
//    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());
//
//    // Check published metadata
//    SegmentDescriptor desc1 = SD(task, "2009/P1D", 0);
//    SegmentDescriptor desc2 = SD(task, "2010/P1D", 0);
//    SegmentDescriptor desc3 = SD(task, "2011/P1D", 0);
//    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());
//    Assert.assertEquals(
//        new KinesisDataSourceMetadata(new KinesisPartitions("topic0", ImmutableMap.of(0, 6L))),
//        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
//    );
//
//    // Check segments in deep storage
//    Assert.assertEquals(ImmutableList.of("b"), readSegmentDim1(desc1));
//    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc2));
//    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc3));
//  }
//
//  @Test(timeout = 30_000L)
//  public void testRunWithOffsetOutOfRangeExceptionAndPause() throws Exception
//  {
//    final KinesisIndexTask task = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 2L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 5L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        null
//    );
//
//    runTask(task);
//
//    while (!task.getStatus().equals(KinesisIndexTask.Status.READING)) {
//      Thread.sleep(2000);
//    }
//
//    task.pause(0);
//
//    while (!task.getStatus().equals(KinesisIndexTask.Status.PAUSED)) {
//      Thread.sleep(25);
//    }
//  }
//
//  @Test(timeout = 30_000L)
//  public void testRunWithOffsetOutOfRangeExceptionAndNextOffsetGreaterThanLeastAvailable() throws Exception
//  {
//    // Insert data
//    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
//      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
//        kafkaProducer.send(record).get();
//      }
//    }
//
//    final KinesisIndexTask task = createTask(
//        null,
//        new KinesisIOConfig(
//            "sequence0",
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 200L)),
//            new KinesisPartitions("topic0", ImmutableMap.of(0, 500L)),
//            kafkaServer.consumerProperties(),
//            true,
//            false,
//            null,
//            false
//        ),
//        null,
//        true
//    );
//
//    runTask(task);
//
//    while (!task.getStatus().equals(KinesisIndexTask.Status.READING)) {
//      Thread.sleep(2000);
//    }
//
//    int i = 0;
//    while(i++ < 5) {
//      Assert.assertEquals(task.getStatus(), KinesisIndexTask.Status.READING);
//      // Offset should not be reset
//      Assert.assertTrue(task.getCurrentOffsets().get(0) == 200L);
//      Thread.sleep(2000);
//    }
//  }
//
//  private ListenableFuture<TaskStatus> runTask(final Task task)
//  {
//    try {
//      taskStorage.insert(task, TaskStatus.running(task.getId()));
//    }
//    catch (EntryExistsException e) {
//      // suppress
//    }
//    taskLockbox.syncFromStorage();
//    final TaskToolbox toolbox = toolboxFactory.build(task);
//    synchronized (runningTasks) {
//      runningTasks.add(task);
//    }
//    return taskExec.submit(
//        new Callable<TaskStatus>()
//        {
//          @Override
//          public TaskStatus call() throws Exception
//          {
//            try {
//              if (task.isReady(toolbox.getTaskActionClient())) {
//                return task.run(toolbox);
//              } else {
//                throw new ISE("Task is not ready");
//              }
//            }
//            catch (Exception e) {
//              log.warn(e, "Task failed");
//              return TaskStatus.failure(task.getId());
//            }
//          }
//        }
//    );
//  }
//
//  private TaskLock getLock(final Task task, final Interval interval)
//  {
//    return Iterables.find(
//        taskLockbox.findLocksForTask(task),
//        new Predicate<TaskLock>()
//        {
//          @Override
//          public boolean apply(TaskLock lock)
//          {
//            return lock.getInterval().contains(interval);
//          }
//        }
//    );
//  }
//
//  private KinesisIndexTask createTask(
//      final String taskId,
//      final KinesisIOConfig ioConfig,
//      final Integer maxRowsPerSegment,
//      final Boolean resetOffsetAutomatically
//  )
//  {
//    final KinesisTuningConfig tuningConfig = new KinesisTuningConfig(
//        1000,
//        maxRowsPerSegment,
//        new Period("P1Y"),
//        null,
//        null,
//        null,
//        buildV9Directly,
//        reportParseExceptions,
//        handoffConditionTimeout,
//        resetOffsetAutomatically
//    );
//    return new KinesisIndexTask(
//        taskId,
//        null,
//        DATA_SCHEMA,
//        tuningConfig,
//        ioConfig,
//        null,
//        null
//    );
//  }
//
//  private QueryRunnerFactoryConglomerate makeTimeseriesOnlyConglomerate()
//  {
//    return new DefaultQueryRunnerFactoryConglomerate(
//        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>of(
//            TimeseriesQuery.class,
//            new TimeseriesQueryRunnerFactory(
//                new TimeseriesQueryQueryToolChest(
//                    new IntervalChunkingQueryRunnerDecorator(null, null, null)
//                    {
//                      @Override
//                      public <T> QueryRunner<T> decorate(
//                          QueryRunner<T> delegate, QueryToolChest<T, ? extends Query<T>> toolChest
//                      )
//                      {
//                        return delegate;
//                      }
//                    }
//                ),
//                new TimeseriesQueryEngine(),
//                new QueryWatcher()
//                {
//                  @Override
//                  public void registerQuery(Query query, ListenableFuture future)
//                  {
//                    // do nothing
//                  }
//                }
//            )
//        )
//    );
//  }
//
//  private void makeToolboxFactory() throws IOException
//  {
//    directory = tempFolder.newFolder();
//    final TestUtils testUtils = new TestUtils();
//    final ObjectMapper objectMapper = testUtils.getTestObjectMapper();
//    for (Module module : new KinesisIndexingServiceModule().getJacksonModules()) {
//      objectMapper.registerModule(module);
//    }
//    final TaskConfig taskConfig = new TaskConfig(
//        new File(directory, "taskBaseDir").getPath(),
//        null,
//        null,
//        50000,
//        null,
//        false,
//        null,
//        null
//    );
//    final TestDerbyConnector derbyConnector = derby.getConnector();
//    derbyConnector.createDataSourceTable();
//    derbyConnector.createPendingSegmentsTable();
//    derbyConnector.createSegmentTable();
//    derbyConnector.createRulesTable();
//    derbyConnector.createConfigTable();
//    derbyConnector.createTaskTables();
//    derbyConnector.createAuditTable();
//    taskStorage = new MetadataTaskStorage(
//        derbyConnector,
//        new TaskStorageConfig(null),
//        new SQLMetadataStorageActionHandlerFactory(
//            derbyConnector,
//            derby.metadataTablesConfigSupplier().get(),
//            objectMapper
//        )
//    );
//    metadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
//        testUtils.getTestObjectMapper(),
//        derby.metadataTablesConfigSupplier().get(),
//        derbyConnector
//    );
//    taskLockbox = new TaskLockbox(taskStorage);
//    final TaskActionToolbox taskActionToolbox = new TaskActionToolbox(
//        taskLockbox,
//        metadataStorageCoordinator,
//        emitter,
//        new SupervisorManager(null)
//    );
//    final TaskActionClientFactory taskActionClientFactory = new LocalTaskActionClientFactory(
//        taskStorage,
//        taskActionToolbox
//    );
//    final SegmentHandoffNotifierFactory handoffNotifierFactory = new SegmentHandoffNotifierFactory()
//    {
//      @Override
//      public SegmentHandoffNotifier createSegmentHandoffNotifier(String dataSource)
//      {
//        return new SegmentHandoffNotifier()
//        {
//          @Override
//          public boolean registerSegmentHandoffCallback(
//              SegmentDescriptor descriptor, Executor exec, Runnable handOffRunnable
//          )
//          {
//            if (doHandoff) {
//              // Simulate immediate handoff
//              exec.execute(handOffRunnable);
//            }
//            return true;
//          }
//
//          @Override
//          public void start()
//          {
//            //Noop
//          }
//
//          @Override
//          public void close()
//          {
//            //Noop
//          }
//        };
//      }
//    };
//    final LocalDataSegmentPusherConfig dataSegmentPusherConfig = new LocalDataSegmentPusherConfig();
//    dataSegmentPusherConfig.storageDirectory = getSegmentDirectory();
//    final DataSegmentPusher dataSegmentPusher = new LocalDataSegmentPusher(dataSegmentPusherConfig, objectMapper);
//    toolboxFactory = new TaskToolboxFactory(
//        taskConfig,
//        taskActionClientFactory,
//        emitter,
//        dataSegmentPusher,
//        new TestDataSegmentKiller(),
//        null, // DataSegmentMover
//        null, // DataSegmentArchiver
//        new TestDataSegmentAnnouncer(),
//        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
//        handoffNotifierFactory,
//        makeTimeseriesOnlyConglomerate(),
//        MoreExecutors.sameThreadExecutor(), // queryExecutorService
//        EasyMock.createMock(MonitorScheduler.class),
//        new SegmentLoaderFactory(
//            new SegmentLoaderLocalCacheManager(
//                null,
//                new SegmentLoaderConfig()
//                {
//                  @Override
//                  public List<StorageLocationConfig> getLocations()
//                  {
//                    return Lists.newArrayList();
//                  }
//                }, testUtils.getTestObjectMapper()
//            )
//        ),
//        testUtils.getTestObjectMapper(),
//        testUtils.getTestIndexMerger(),
//        testUtils.getTestIndexIO(),
//        MapCache.create(1024),
//        new CacheConfig(),
//        testUtils.getTestIndexMergerV9()
//    );
//  }
//
//  private void destroyToolboxFactory()
//  {
//    toolboxFactory = null;
//    taskStorage = null;
//    taskLockbox = null;
//    metadataStorageCoordinator = null;
//  }
//
//  private Set<SegmentDescriptor> publishedDescriptors() throws IOException
//  {
//    return FluentIterable.from(
//        metadataStorageCoordinator.getUsedSegmentsForInterval(
//            DATA_SCHEMA.getDataSource(),
//            new Interval("0000/3000")
//        )
//    ).transform(
//        new Function<DataSegment, SegmentDescriptor>()
//        {
//          @Override
//          public SegmentDescriptor apply(DataSegment input)
//          {
//            return input.toDescriptor();
//          }
//        }
//    ).toSet();
//  }
//
//  private File getSegmentDirectory()
//  {
//    return new File(directory, "segments");
//  }
//
//  private List<String> readSegmentDim1(final SegmentDescriptor descriptor) throws IOException
//  {
//    File indexZip = new File(
//        String.format(
//            "%s/%s/%s_%s/%s/%d/index.zip",
//            getSegmentDirectory(),
//            DATA_SCHEMA.getDataSource(),
//            descriptor.getInterval().getStart(),
//            descriptor.getInterval().getEnd(),
//            descriptor.getVersion(),
//            descriptor.getPartitionNumber()
//        )
//    );
//    File outputLocation = new File(
//        directory,
//        String.format(
//            "%s_%s_%s_%s",
//            descriptor.getInterval().getStart(),
//            descriptor.getInterval().getEnd(),
//            descriptor.getVersion(),
//            descriptor.getPartitionNumber()
//        )
//    );
//    outputLocation.mkdir();
//    CompressionUtils.unzip(
//        Files.asByteSource(indexZip),
//        outputLocation,
//        Predicates.<Throwable>alwaysFalse(),
//        false
//    );
//    IndexIO indexIO = new TestUtils().getTestIndexIO();
//    QueryableIndex index = indexIO.loadIndex(outputLocation);
//    DictionaryEncodedColumn<String> dim1 = index.getColumn("dim1").getDictionaryEncoding();
//    List<String> values = Lists.newArrayList();
//    for (int i = 0; i < dim1.length(); i++) {
//      int id = dim1.getSingleValueRow(i);
//      String value = dim1.lookupName(id);
//      values.add(value);
//    }
//    return values;
//  }
//
//  public long countEvents(final Task task) throws Exception
//  {
//    // Do a query.
//    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
//                                  .dataSource(DATA_SCHEMA.getDataSource())
//                                  .aggregators(
//                                      ImmutableList.<AggregatorFactory>of(
//                                          new LongSumAggregatorFactory("rows", "rows")
//                                      )
//                                  ).granularity(Granularities.ALL)
//                                  .intervals("0000/3000")
//                                  .build();
//
//    ArrayList<Result<TimeseriesResultValue>> results = Sequences.toList(
//        task.getQueryRunner(query).run(query, ImmutableMap.<String, Object>of()),
//        Lists.<Result<TimeseriesResultValue>>newArrayList()
//    );
//
//    return results.isEmpty() ? 0 : results.get(0).getValue().getLongMetric("rows");
//  }
//
//  private static byte[] JB(String timestamp, String dim1, String dim2, double met1)
//  {
//    try {
//      return new ObjectMapper().writeValueAsBytes(
//          ImmutableMap.of("timestamp", timestamp, "dim1", dim1, "dim2", dim2, "met1", met1)
//      );
//    }
//    catch (Exception e) {
//      throw Throwables.propagate(e);
//    }
//  }
//
//  private SegmentDescriptor SD(final Task task, final String intervalString, final int partitionNum)
//  {
//    final Interval interval = new Interval(intervalString);
//    return new SegmentDescriptor(interval, getLock(task, interval).getVersion(), partitionNum);
//  }
//}

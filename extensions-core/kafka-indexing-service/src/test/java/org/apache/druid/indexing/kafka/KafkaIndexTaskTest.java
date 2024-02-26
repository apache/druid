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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.test.TestingCluster;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.data.input.kafkainput.KafkaInputFormat;
import org.apache.druid.data.input.kafkainput.KafkaStringHeaderFormat;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.IndexTaskTest;
import org.apache.druid.indexing.common.task.ParseExceptionReport;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisor;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.test.TestBroker;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner.Status;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTestBase;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.RowIngestionMetersTotals;
import org.apache.druid.segment.incremental.RowMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.timeline.DataSegment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.joda.time.Period;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
@RunWith(Parameterized.class)
public class KafkaIndexTaskTest extends SeekableStreamIndexTaskTestBase
{
  private static final long POLL_RETRY_MS = 100;
  private static final Iterable<Header> SAMPLE_HEADERS = ImmutableList.of(new Header()
  {
    @Override
    public String key()
    {
      return "encoding";
    }

    @Override
    public byte[] value()
    {
      return "application/json".getBytes(StandardCharsets.UTF_8);
    }
  });
  private static final InputFormat KAFKA_INPUT_FORMAT = new KafkaInputFormat(
      new KafkaStringHeaderFormat(null),
      INPUT_FORMAT,
      INPUT_FORMAT,
      "kafka.testheader.", "kafka.key", "kafka.timestamp", "kafka.topic"
  );

  private static TestingCluster zkServer;
  private static TestBroker kafkaServer;
  private static ServiceEmitter emitter;
  private static int topicPostfix;

  static final Module TEST_MODULE = new SimpleModule("kafkaTestModule").registerSubtypes(
      new NamedType(TestKafkaInputFormat.class, "testKafkaInputFormat"),
      new NamedType(TestKafkaFormatWithMalformedDataDetection.class, "testKafkaFormatWithMalformedDataDetection")
  );

  static {
    Stream.concat(
        new KafkaIndexTaskModule().getJacksonModules().stream(),
        Stream.of(TEST_MODULE)
    ).forEach(OBJECT_MAPPER::registerModule);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK},
        new Object[]{LockGranularity.SEGMENT}
    );
  }

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

  private String topic;
  private List<ProducerRecord<byte[], byte[]>> records;

  private static List<ProducerRecord<byte[], byte[]>> generateRecords(String topic)
  {
    return ImmutableList.of(
        new ProducerRecord<byte[], byte[]>(topic, 0, null, jbb("2008", "a", "y", "10", "20.0", "1.0"), SAMPLE_HEADERS),
        new ProducerRecord<byte[], byte[]>(topic, 0, null, jbb("2009", "b", "y", "10", "20.0", "1.0"), SAMPLE_HEADERS),
        new ProducerRecord<byte[], byte[]>(topic, 0, null, jbb("2010", "c", "y", "10", "20.0", "1.0"), SAMPLE_HEADERS),
        new ProducerRecord<byte[], byte[]>(topic, 0, null, jbb("2011", "d", "y", "10", "20.0", "1.0"), SAMPLE_HEADERS),
        new ProducerRecord<byte[], byte[]>(topic, 0, null, jbb("2011", "e", "y", "10", "20.0", "1.0"), SAMPLE_HEADERS),
        new ProducerRecord<>(topic, 0, null, jbb("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8("unparseable")),
        new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8("")),
        new ProducerRecord<>(topic, 0, null, null),
        new ProducerRecord<>(topic, 0, null, jbb("2013", "f", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2049", "f", "y", "notanumber", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2049", "f", "y", "10", "notanumber", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2049", "f", "y", "10", "20.0", "notanumber")),
        new ProducerRecord<>(topic, 1, null, jbb("2012", "g", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 1, null, jbb("2011", "h", "y", "10", "20.0", "1.0"))
    );
  }

  private static List<ProducerRecord<byte[], byte[]>> generateSinglePartitionRecords(String topic)
  {
    return ImmutableList.of(
        new ProducerRecord<>(topic, 0, null, jbb("2008", "a", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2009", "b", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2010", "c", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2011", "d", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2011", "D", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2012", "e", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2009", "B", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2008", "A", "x", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2009", "B", "x", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2010", "C", "x", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2011", "D", "x", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2011", "d", "x", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2012", "E", "x", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jbb("2009", "b", "x", "10", "20.0", "1.0"))
    );
  }

  private static String getTopicName()
  {
    return "topic" + topicPostfix++;
  }

  public KafkaIndexTaskTest(LockGranularity lockGranularity)
  {
    super(lockGranularity);
  }

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
    taskExec.awaitTermination(20, TimeUnit.MINUTES);

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
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );
    Assert.assertTrue(task.supportsQueries());

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(2, 5)).totalProcessed(3));
    Assert.assertTrue(task.getRunner().getFireDepartmentMetrics().isProcessingDone());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testIngestNullColumnAfterDataInserted() throws Exception
  {
    // Insert data
    insertData();

    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        ImmutableList.of(
            new StringDimensionSchema("dim1"),
            new StringDimensionSchema("dim1t"),
            new StringDimensionSchema("unknownDim"),
            new StringDimensionSchema("dim2"),
            new LongDimensionSchema("dimLong"),
            new FloatDimensionSchema("dimFloat")
        )
    );
    final KafkaIndexTask task = createTask(
        null,
        NEW_DATA_SCHEMA.withDimensionsSpec(dimensionsSpec),
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(2, 5)).totalProcessed(3));

    final Collection<DataSegment> segments = publishedSegments();
    for (DataSegment segment : segments) {
      for (int i = 0; i < dimensionsSpec.getDimensions().size(); i++) {
        Assert.assertEquals(dimensionsSpec.getDimensionNames().get(i), segment.getDimensions().get(i));
      }
    }
  }

  @Test(timeout = 60_000L)
  public void testIngestNullColumnAfterDataInserted_storeEmptyColumnsOff_shouldNotStoreEmptyColumns() throws Exception
  {
    // Insert data
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        NEW_DATA_SCHEMA.withDimensionsSpec(
            new DimensionsSpec(
                ImmutableList.of(
                    new StringDimensionSchema("dim1"),
                    new StringDimensionSchema("dim1t"),
                    new StringDimensionSchema("dim2"),
                    new LongDimensionSchema("dimLong"),
                    new FloatDimensionSchema("dimFloat"),
                    new StringDimensionSchema("unknownDim")
                )
            )
        ),
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );
    task.addToContext(Tasks.STORE_EMPTY_COLUMNS_KEY, false);
    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    final Collection<DataSegment> segments = publishedSegments();
    for (DataSegment segment : segments) {
      Assert.assertFalse(segment.getDimensions().contains("unknownDim"));
    }
  }

  @Test(timeout = 60_000L)
  public void testRunAfterDataInsertedWithLegacyParser() throws Exception
  {
    // Insert data
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        OLD_DATA_SCHEMA,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            null,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(2, 5)).totalProcessed(3));
    Assert.assertTrue(task.getRunner().getFireDepartmentMetrics().isProcessingDone());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunBeforeDataInserted() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getRunner().getStatus() != Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    insertData();

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(2, 5)).totalProcessed(3));
    Assert.assertTrue(task.getRunner().getFireDepartmentMetrics().isProcessingDone());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunAfterDataInsertedLiveReport() throws Exception
  {
    // Insert data
    insertData();
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 12L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);
    SeekableStreamIndexTaskRunner runner = task.getRunner();
    while (true) {
      Thread.sleep(1000);
      if (runner.getStatus() == Status.PUBLISHING) {
        break;
      }
    }
    Map rowStats = runner.doGetRowStats();
    Map totals = (Map) rowStats.get("totals");
    RowIngestionMetersTotals buildSegments = (RowIngestionMetersTotals) totals.get("buildSegments");

    Map movingAverages = (Map) rowStats.get("movingAverages");
    Map buildSegments2 = (Map) movingAverages.get("buildSegments");
    HashMap avg_1min = (HashMap) buildSegments2.get("1m");
    HashMap avg_5min = (HashMap) buildSegments2.get("5m");
    HashMap avg_15min = (HashMap) buildSegments2.get("15m");

    runner.resume();

    // Check metrics
    Assert.assertEquals(buildSegments, task.getRunner().getRowIngestionMeters().getTotals());

    Assert.assertEquals(avg_1min.get("processed"), 0.0);
    Assert.assertEquals(avg_5min.get("processed"), 0.0);
    Assert.assertEquals(avg_15min.get("processed"), 0.0);
    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
  }

  @Test(timeout = 60_000L)
  public void testIncrementalHandOff() throws Exception
  {
    final String baseSequenceName = "sequence0";
    // as soon as any segment has more than one record, incremental publishing should happen
    maxRowsPerSegment = 2;

    // Insert data
    insertData();
    Map<String, Object> consumerProps = kafkaServer.consumerProperties();
    consumerProps.put("max.poll.records", "1");

    final SeekableStreamStartSequenceNumbers<KafkaTopicPartition, Long> startPartitions = new SeekableStreamStartSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L, new KafkaTopicPartition(false, topic, 1), 0L),
        ImmutableSet.of()
    );
    // Checkpointing will happen at either checkpoint1 or checkpoint2 depending on ordering
    // of events fetched across two partitions from Kafka
    final SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> checkpoint1 = new SeekableStreamEndSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L, new KafkaTopicPartition(false, topic, 1), 0L)
    );
    final SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> checkpoint2 = new SeekableStreamEndSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 4L, new KafkaTopicPartition(false, topic, 1), 2L)
    );
    final SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> endPartitions = new SeekableStreamEndSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 10L, new KafkaTopicPartition(false, topic, 1), 2L)
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
            INPUT_FORMAT,
            null
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);
    while (task.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    final Map<KafkaTopicPartition, Long> currentOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());
    Assert.assertTrue(checkpoint1.getPartitionSequenceNumberMap().equals(currentOffsets)
                      || checkpoint2.getPartitionSequenceNumberMap()
                                    .equals(currentOffsets));
    task.getRunner().setEndOffsets(currentOffsets, false);
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    Assert.assertEquals(1, checkpointRequestsHash.size());
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                NEW_DATA_SCHEMA.getDataSource(),
                0,
                new KafkaDataSourceMetadata(startPartitions)
            )
        )
    );

    long totalBytes = getTotalSizeOfRecords(0, 10) + getTotalSizeOfRecords(13, 15);
    verifyTaskMetrics(task, RowMeters.with().bytes(totalBytes).unparseable(3).thrownAway(1).totalProcessed(8));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0, ImmutableList.of("a")),
            sdd("2009/P1D", 0, ImmutableList.of("b")),
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"), ImmutableList.of("d", "h")),
            sdd("2011/P1D", 1, ImmutableList.of("h"), ImmutableList.of("e")),
            sdd("2012/P1D", 0, ImmutableList.of("g")),
            sdd("2013/P1D", 0, ImmutableList.of("f"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 10L, new KafkaTopicPartition(false, topic, 1), 2L))
        ),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testIncrementalHandOffMaxTotalRows() throws Exception
  {
    final String baseSequenceName = "sequence0";
    // incremental publish should happen every 3 records
    maxRowsPerSegment = Integer.MAX_VALUE;
    maxTotalRows = 3L;

    // Insert data
    int numToAdd = records.size() - 2;

    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      kafkaProducer.initTransactions();
      kafkaProducer.beginTransaction();
      for (int i = 0; i < numToAdd; i++) {
        kafkaProducer.send(records.get(i)).get();
      }
      kafkaProducer.commitTransaction();
    }

    Map<String, Object> consumerProps = kafkaServer.consumerProperties();
    consumerProps.put("max.poll.records", "1");

    final SeekableStreamStartSequenceNumbers<KafkaTopicPartition, Long> startPartitions = new SeekableStreamStartSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L, new KafkaTopicPartition(false, topic, 1), 0L),
        ImmutableSet.of()
    );
    final SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> checkpoint1 = new SeekableStreamEndSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 3L, new KafkaTopicPartition(false, topic, 1), 0L)
    );
    final SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> checkpoint2 = new SeekableStreamEndSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 10L, new KafkaTopicPartition(false, topic, 1), 0L)
    );

    final SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> endPartitions = new SeekableStreamEndSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 10L, new KafkaTopicPartition(false, topic, 1), 2L)
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
            INPUT_FORMAT,
            null
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);
    while (task.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    final Map<KafkaTopicPartition, Long> currentOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());

    Assert.assertEquals(checkpoint1.getPartitionSequenceNumberMap(), currentOffsets);
    task.getRunner().setEndOffsets(currentOffsets, false);

    while (task.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }

    // add remaining records
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      kafkaProducer.initTransactions();
      kafkaProducer.beginTransaction();
      for (int i = numToAdd; i < records.size(); i++) {
        kafkaProducer.send(records.get(i)).get();
      }
      kafkaProducer.commitTransaction();
    }
    final Map<KafkaTopicPartition, Long> nextOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());


    Assert.assertEquals(checkpoint2.getPartitionSequenceNumberMap(), nextOffsets);
    task.getRunner().setEndOffsets(nextOffsets, false);

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    Assert.assertEquals(2, checkpointRequestsHash.size());
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                NEW_DATA_SCHEMA.getDataSource(),
                0,
                new KafkaDataSourceMetadata(startPartitions)
            )
        )
    );
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                NEW_DATA_SCHEMA.getDataSource(),
                0,
                new KafkaDataSourceMetadata(
                    new SeekableStreamStartSequenceNumbers<>(topic, currentOffsets, ImmutableSet.of())
                )
            )
        )
    );

    long totalBytes = getTotalSizeOfRecords(0, 10) + getTotalSizeOfRecords(13, 15);
    verifyTaskMetrics(task, RowMeters.with().bytes(totalBytes).unparseable(3).thrownAway(1).totalProcessed(8));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0, ImmutableList.of("a")),
            sdd("2009/P1D", 0, ImmutableList.of("b")),
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"), ImmutableList.of("d", "h")),
            sdd("2011/P1D", 1, ImmutableList.of("h"), ImmutableList.of("e")),
            sdd("2012/P1D", 0, ImmutableList.of("g")),
            sdd("2013/P1D", 0, ImmutableList.of("f"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 10L, new KafkaTopicPartition(false, topic, 1), 2L))
        ),
        newDataSchemaMetadata()
    );

    Assert.assertEquals(
        new KafkaDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 10L, new KafkaTopicPartition(false, topic, 1), 2L))
        ),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testTimeBasedIncrementalHandOff() throws Exception
  {
    final String baseSequenceName = "sequence0";
    // as soon as any segment hits maxRowsPerSegment or intermediateHandoffPeriod, incremental publishing should happen
    maxRowsPerSegment = Integer.MAX_VALUE;
    intermediateHandoffPeriod = new Period().withSeconds(0);

    // Insert data
    insertData();
    Map<String, Object> consumerProps = kafkaServer.consumerProperties();
    consumerProps.put("max.poll.records", "1");

    final SeekableStreamStartSequenceNumbers<KafkaTopicPartition, Long> startPartitions = new SeekableStreamStartSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L, new KafkaTopicPartition(false, topic, 1), 0L),
        ImmutableSet.of()
    );
    // Checkpointing will happen at checkpoint
    final SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> checkpoint = new SeekableStreamEndSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 1L, new KafkaTopicPartition(false, topic, 1), 0L)
    );
    final SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> endPartitions = new SeekableStreamEndSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L, new KafkaTopicPartition(false, topic, 1), 0L)
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
            INPUT_FORMAT,
            null
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);

    // task will pause for checkpointing
    while (task.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    final Map<KafkaTopicPartition, Long> currentOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpoint.getPartitionSequenceNumberMap(), currentOffsets);
    task.getRunner().setEndOffsets(currentOffsets, false);
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    Assert.assertEquals(1, checkpointRequestsHash.size());
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                NEW_DATA_SCHEMA.getDataSource(),
                0,
                new KafkaDataSourceMetadata(startPartitions)
            )
        )
    );

    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(0, 2)).totalProcessed(2));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0, ImmutableList.of("a")),
            sdd("2009/P1D", 0, ImmutableList.of("b"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L, new KafkaTopicPartition(false, topic, 1), 0L))
        ),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testCheckpointResetWithSameEndOffsets() throws Exception
  {
    final String baseSequenceName = "sequence0";
    // as soon as any segment hits maxRowsPerSegment or intermediateHandoffPeriod, incremental publishing should happen
    maxRowsPerSegment = Integer.MAX_VALUE;
    intermediateHandoffPeriod = new Period().withMillis(10);

    // Insert data
    insertData();
    Map<String, Object> consumerProps = kafkaServer.consumerProperties();
    consumerProps.put("max.poll.records", "1");

    final SeekableStreamStartSequenceNumbers<KafkaTopicPartition, Long> startPartitions = new SeekableStreamStartSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L, new KafkaTopicPartition(false, topic, 1), 0L),
        ImmutableSet.of()
    );
    final SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> endPartitions = new SeekableStreamEndSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L, new KafkaTopicPartition(false, topic, 1), 0L)
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
            INPUT_FORMAT,
            null
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);

    // task will pause for checkpointing
    while (task.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    long currentNextCheckpointTime = task.getRunner().getNextCheckpointTime();
    final Map<KafkaTopicPartition, Long> nextEndOffsets = task.getRunner().getLastSequenceMetadata().getStartOffsets();
    task.getRunner().setEndOffsets(nextEndOffsets, false);
    long newNextCheckpointTime = task.getRunner().getNextCheckpointTime();
    Assert.assertTrue(
        StringUtils.format(
            "Old checkpoint time: [%d], new checkpoint time: [%d]",
            currentNextCheckpointTime,
            newNextCheckpointTime),
        newNextCheckpointTime > currentNextCheckpointTime);
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
  }

  DataSourceMetadata newDataSchemaMetadata()
  {
    return metadataStorageCoordinator.retrieveDataSourceMetadata(NEW_DATA_SCHEMA.getDataSource());
  }

  @Test(timeout = 60_000L)
  public void testIncrementalHandOffReadsThroughEndOffsets() throws Exception
  {
    records = generateSinglePartitionRecords(topic);

    final String baseSequenceName = "sequence0";
    // as soon as any segment has more than one record, incremental publishing should happen
    maxRowsPerSegment = 2;

    insertData();

    Map<String, Object> consumerProps = kafkaServer.consumerProperties();
    consumerProps.put("max.poll.records", "1");

    final SeekableStreamStartSequenceNumbers<KafkaTopicPartition, Long> startPartitions =
        new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L), ImmutableSet.of());
    final SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> checkpoint1 =
        new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L));
    final SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> checkpoint2 =
        new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 9L));
    final SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> endPartitions =
        new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), Long.MAX_VALUE));

    final KafkaIndexTask normalReplica = createTask(
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
            INPUT_FORMAT,
            null
        )
    );
    final KafkaIndexTask staleReplica = createTask(
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
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> normalReplicaFuture = runTask(normalReplica);
    // Simulating one replica is slower than the other
    final ListenableFuture<TaskStatus> staleReplicaFuture = Futures.transformAsync(
        taskExec.submit(() -> {
          Thread.sleep(1000);
          return staleReplica;
        }),
        (AsyncFunction<Task, TaskStatus>) this::runTask,
        MoreExecutors.directExecutor()
    );

    while (normalReplica.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    staleReplica.getRunner().pause();
    while (staleReplica.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    Map<KafkaTopicPartition, Long> currentOffsets = ImmutableMap.copyOf(normalReplica.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpoint1.getPartitionSequenceNumberMap(), currentOffsets);

    normalReplica.getRunner().setEndOffsets(currentOffsets, false);
    staleReplica.getRunner().setEndOffsets(currentOffsets, false);

    while (normalReplica.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    while (staleReplica.getRunner().getStatus() != Status.PAUSED) {
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

    long totalBytes = getTotalSizeOfRecords(0, 9);
    verifyTaskMetrics(normalReplica, RowMeters.with().bytes(totalBytes).totalProcessed(9));
    verifyTaskMetrics(staleReplica, RowMeters.with().bytes(totalBytes).totalProcessed(9));
  }

  @Test(timeout = 60_000L)
  public void testRunWithMinimumMessageTime() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            DateTimes.of("2010"),
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getRunner().getStatus() != Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    insertData();

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(0, 5)).thrownAway(2).totalProcessed(3));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunWithMaximumMessageTime() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            DateTimes.of("2010"),
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getRunner().getStatus() != Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    insertData();

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(0, 5)).thrownAway(2).totalProcessed(3));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0, ImmutableList.of("a")),
            sdd("2009/P1D", 0, ImmutableList.of("b")),
            sdd("2010/P1D", 0, ImmutableList.of("c"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunWithTransformSpec() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        NEW_DATA_SCHEMA.withTransformSpec(
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
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getRunner().getStatus() != Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    insertData();

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(0, 5)).thrownAway(4).totalProcessed(1));

    // Check published metadata
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    assertEqualsExceptVersion(ImmutableList.of(sdd("2009/P1D", 0)), publishedDescriptors);
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L))),
        newDataSchemaMetadata()
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("b"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("bb"), readSegmentColumn("dim1t", publishedDescriptors.get(0)));
  }

  @Test(timeout = 60_000L)
  public void testKafkaRecordEntityInputFormat() throws Exception
  {
    // Insert data
    insertData(Iterables.limit(records, 3));

    final KafkaIndexTask task = createTask(
        null,
        new DataSchema(
            "test_ds",
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(
                Arrays.asList(
                    new StringDimensionSchema("dim1"),
                    new StringDimensionSchema("dim1t"),
                    new StringDimensionSchema("dim2"),
                    new LongDimensionSchema("dimLong"),
                    new FloatDimensionSchema("dimFloat"),
                    new StringDimensionSchema("kafka.topic"),
                    new LongDimensionSchema("kafka.offset"),
                    new StringDimensionSchema("kafka.header.encoding")
                )
            ),
            new AggregatorFactory[]{
                new DoubleSumAggregatorFactory("met1sum", "met1"),
                new CountAggregatorFactory("rows")
            },
            new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
            null
        ),
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            new TestKafkaInputFormat(INPUT_FORMAT),
            null
        )
    );
    Assert.assertTrue(task.supportsQueries());

    final ListenableFuture<TaskStatus> future = runTask(task);

    while (countEvents(task) != 3) {
      Thread.sleep(25);
    }

    Assert.assertEquals(Status.READING, task.getRunner().getStatus());

    final QuerySegmentSpec interval = OBJECT_MAPPER.readValue(
        "\"2008/2012\"", QuerySegmentSpec.class
    );
    List<Map<String, Object>> scanResultValues = scanData(task, interval);
    //verify that there are no records indexed in the rollbacked time period
    Assert.assertEquals(3, Iterables.size(scanResultValues));

    int i = 0;
    for (Map<String, Object> event : scanResultValues) {
      Assert.assertEquals((long) i++, event.get("kafka.offset"));
      Assert.assertEquals(topic, event.get("kafka.topic"));
      Assert.assertEquals("application/json", event.get("kafka.header.encoding"));
    }
    // insert remaining data
    insertData(Iterables.skip(records, 3));

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(0, 4)).totalProcessed(4));
  }

  @Test(timeout = 60_000L)
  public void testKafkaInputFormat() throws Exception
  {
    // Insert data
    insertData(Iterables.limit(records, 3));

    final KafkaIndexTask task = createTask(
        null,
        new DataSchema(
            "test_ds",
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(
                Arrays.asList(
                    new StringDimensionSchema("dim1"),
                    new StringDimensionSchema("dim1t"),
                    new StringDimensionSchema("dim2"),
                    new LongDimensionSchema("dimLong"),
                    new FloatDimensionSchema("dimFloat"),
                    new StringDimensionSchema("kafka.testheader.encoding")
                )
            ),
            new AggregatorFactory[]{
                new DoubleSumAggregatorFactory("met1sum", "met1"),
                new CountAggregatorFactory("rows")
            },
            new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
            null
        ),
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            KAFKA_INPUT_FORMAT,
            null
        )
    );
    Assert.assertTrue(task.supportsQueries());

    final ListenableFuture<TaskStatus> future = runTask(task);

    while (countEvents(task) != 3) {
      Thread.sleep(25);
    }

    Assert.assertEquals(Status.READING, task.getRunner().getStatus());

    final QuerySegmentSpec interval = OBJECT_MAPPER.readValue(
        "\"2008/2012\"", QuerySegmentSpec.class
    );
    List<Map<String, Object>> scanResultValues = scanData(task, interval);
    Assert.assertEquals(3, Iterables.size(scanResultValues));

    for (Map<String, Object> event : scanResultValues) {
      Assert.assertEquals("application/json", event.get("kafka.testheader.encoding"));
      Assert.assertEquals("y", event.get("dim2"));
    }

    // insert remaining data
    insertData(Iterables.skip(records, 3));

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(0, 4)).totalProcessed(4));
  }

  @Test(timeout = 60_000L)
  public void testRunOnNothing() throws Exception
  {
    // Insert data
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().totalProcessed(0));

    // Check published metadata
    Assert.assertEquals(ImmutableList.of(), publishedDescriptors());
  }

  @Test(timeout = 60_000L)
  public void testHandoffConditionTimeoutWhenHandoffOccurs() throws Exception
  {
    handoffConditionTimeout = 5_000;

    // Insert data
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(2, 5)).totalProcessed(3));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testHandoffConditionTimeoutWhenHandoffDoesNotOccur() throws Exception
  {
    doHandoff = false;
    handoffConditionTimeout = 100;

    // Insert data
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(2, 5)).totalProcessed(3));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L))
        ),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testReportParseExceptions() throws Exception
  {
    reportParseExceptions = true;

    // these will be ignored because reportParseExceptions is true
    maxParseExceptions = 1000;
    maxSavedParseExceptions = 2;

    // Insert data
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 7L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.FAILED, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(2, 6)).unparseable(1).totalProcessed(3));

    // Check published metadata
    Assert.assertEquals(ImmutableList.of(), publishedDescriptors());
    Assert.assertNull(newDataSchemaMetadata());
  }

  @Test(timeout = 60_000L)
  public void testMultipleParseExceptionsSuccess() throws Exception
  {
    reportParseExceptions = false;
    maxParseExceptions = 6;
    maxSavedParseExceptions = 6;

    // Insert data
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 13L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    TaskStatus status = future.get();

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, status.getStatusCode());
    Assert.assertNull(status.getErrorMsg());
    final long totalRecordBytes = getTotalSizeOfRecords(2, 13);
    verifyTaskMetrics(task, RowMeters.with()
                                     .bytes(totalRecordBytes)
                                     .unparseable(3).errors(3)
                                     .thrownAway(1).totalProcessed(4));

    // Check published metadata
    assertEqualsExceptVersion(
        ImmutableList.of(sdd("2010/P1D", 0), sdd("2011/P1D", 0), sdd("2013/P1D", 0), sdd("2049/P1D", 0)),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 13L))),
        newDataSchemaMetadata()
    );

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED, 4,
            RowIngestionMeters.PROCESSED_BYTES, (int) totalRecordBytes,
            RowIngestionMeters.PROCESSED_WITH_ERROR, 3,
            RowIngestionMeters.UNPARSEABLE, 3,
            RowIngestionMeters.THROWN_AWAY, 1
        )
    );
    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    ParseExceptionReport parseExceptionReport =
        ParseExceptionReport.forPhase(reportData, RowIngestionMeters.BUILD_SEGMENTS);

    List<String> expectedMessages = Arrays.asList(
        "Unable to parse value[notanumber] for field[met1]",
        "could not convert value [notanumber] to float",
        "could not convert value [notanumber] to long",
        "Unable to parse [] as the intermediateRow resulted in empty input row (Record: 1)",
        "Unable to parse row [unparseable] (Record: 1)",
        "Encountered row with timestamp[246140482-04-24T15:36:27.903Z] that cannot be represented as a long: [{timestamp=246140482-04-24T15:36:27.903Z, dim1=x, dim2=z, dimLong=10, dimFloat=20.0, met1=1.0}] (Record: 1)"
    );
    Assert.assertEquals(expectedMessages, parseExceptionReport.getErrorMessages());

    List<String> expectedInputs = Arrays.asList(
        "{timestamp=2049, dim1=f, dim2=y, dimLong=10, dimFloat=20.0, met1=notanumber}",
        "{timestamp=2049, dim1=f, dim2=y, dimLong=10, dimFloat=notanumber, met1=1.0}",
        "{timestamp=2049, dim1=f, dim2=y, dimLong=notanumber, dimFloat=20.0, met1=1.0}",
        "",
        "unparseable",
        "{timestamp=246140482-04-24T15:36:27.903Z, dim1=x, dim2=z, dimLong=10, dimFloat=20.0, met1=1.0}"
    );
    Assert.assertEquals(expectedInputs, parseExceptionReport.getInputs());
  }

  @Test(timeout = 60_000L)
  public void testMultipleParseExceptionsFailure() throws Exception
  {
    reportParseExceptions = false;
    maxParseExceptions = 2;
    maxSavedParseExceptions = 2;

    // Insert data
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 10L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    TaskStatus status = future.get();

    // Wait for task to exit
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());
    IndexTaskTest.checkTaskStatusErrorMsgForParseExceptionsExceeded(status);

    final long totalBytes = getTotalSizeOfRecords(2, 8);
    verifyTaskMetrics(task, RowMeters.with().bytes(totalBytes).unparseable(3).totalProcessed(3));

    // Check published metadata
    Assert.assertEquals(ImmutableList.of(), publishedDescriptors());
    Assert.assertNull(newDataSchemaMetadata());

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED, 3,
            RowIngestionMeters.PROCESSED_BYTES, (int) totalBytes,
            RowIngestionMeters.PROCESSED_WITH_ERROR, 0,
            RowIngestionMeters.UNPARSEABLE, 3,
            RowIngestionMeters.THROWN_AWAY, 0
        )
    );
    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    ParseExceptionReport parseExceptionReport =
        ParseExceptionReport.forPhase(reportData, RowIngestionMeters.BUILD_SEGMENTS);

    List<String> expectedMessages = Arrays.asList(
        "Unable to parse [] as the intermediateRow resulted in empty input row (Record: 1)",
        "Unable to parse row [unparseable] (Record: 1)"
    );
    Assert.assertEquals(expectedMessages, parseExceptionReport.getErrorMessages());

    List<String> expectedInputs = Arrays.asList("", "unparseable");
    Assert.assertEquals(expectedInputs, parseExceptionReport.getInputs());
  }

  @Test(timeout = 60_000L)
  public void testRunReplicas() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );
    final KafkaIndexTask task2 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert data
    insertData();

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    final long totalBytes = getTotalSizeOfRecords(2, 5);
    verifyTaskMetrics(task1, RowMeters.with().bytes(totalBytes).totalProcessed(3));
    verifyTaskMetrics(task2, RowMeters.with().bytes(totalBytes).totalProcessed(3));

    // Check published segments & metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunConflicting() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );
    final KafkaIndexTask task2 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            1,
            "sequence1",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 3L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 10L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    // Insert data
    insertData();

    // Run first task
    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskState.FAILED, future2.get().getStatusCode());

    verifyTaskMetrics(task1, RowMeters.with().bytes(getTotalSizeOfRecords(2, 5)).totalProcessed(3));
    verifyTaskMetrics(task2, RowMeters.with().bytes(getTotalSizeOfRecords(3, 10))
                                      .unparseable(3).thrownAway(1).totalProcessed(3));

    // Check published segments & metadata, should all be from the first task
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunConflictingWithoutTransactions() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            false,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );
    final KafkaIndexTask task2 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            1,
            "sequence1",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 3L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 10L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            false,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    // Insert data
    insertData();

    // Run first task
    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    // Check published segments & metadata
    SegmentDescriptorAndExpectedDim1Values desc1 = sdd("2010/P1D", 0, ImmutableList.of("c"));
    SegmentDescriptorAndExpectedDim1Values desc2 = sdd("2011/P1D", 0, ImmutableList.of("d", "e"));
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2), publishedDescriptors());
    Assert.assertNull(newDataSchemaMetadata());

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    verifyTaskMetrics(task1, RowMeters.with().bytes(getTotalSizeOfRecords(2, 5)).totalProcessed(3));
    verifyTaskMetrics(task2, RowMeters.with().bytes(getTotalSizeOfRecords(3, 10))
                                      .unparseable(3).thrownAway(1).totalProcessed(3));

    // Check published segments & metadata
    SegmentDescriptorAndExpectedDim1Values desc3 = sdd("2011/P1D", 1, ImmutableList.of("d", "e"));
    SegmentDescriptorAndExpectedDim1Values desc4 = sdd("2013/P1D", 0, ImmutableList.of("f"));
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2, desc3, desc4), publishedDescriptors());
    Assert.assertNull(newDataSchemaMetadata());
  }

  @Test(timeout = 60_000L)
  public void testRunOneTaskTwoPartitions() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L, new KafkaTopicPartition(false, topic, 1), 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L, new KafkaTopicPartition(false, topic, 1), 2L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Insert data
    insertData();

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    long totalBytes = getTotalSizeOfRecords(2, 5) + getTotalSizeOfRecords(13, 15);
    verifyTaskMetrics(task, RowMeters.with().bytes(totalBytes).totalProcessed(5));

    // Check published segments & metadata
    SegmentDescriptorAndExpectedDim1Values desc1 = sdd("2010/P1D", 0, ImmutableList.of("c"));
    SegmentDescriptorAndExpectedDim1Values desc2 = sdd("2011/P1D", 0, ImmutableList.of("d", "e", "h"));
    // desc3 will not be created in KafkaIndexTask (0.12.x) as it does not create per Kafka partition Druid segments
    @SuppressWarnings("unused")
    SegmentDescriptor desc3 = sd("2011/P1D", 1);
    SegmentDescriptorAndExpectedDim1Values desc4 = sdd("2012/P1D", 0, ImmutableList.of("g"));
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2, desc4), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L, new KafkaTopicPartition(false, topic, 1), 2L))
        ),
        newDataSchemaMetadata()
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
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );
    final KafkaIndexTask task2 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            1,
            "sequence1",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 1), 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 1), 1L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert data
    insertData();

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    verifyTaskMetrics(task1, RowMeters.with().bytes(getTotalSizeOfRecords(2, 5)).totalProcessed(3));
    verifyTaskMetrics(task2, RowMeters.with().bytes(getTotalSizeOfRecords(13, 14)).totalProcessed(1));

    // Check published segments & metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e")),
            sdd("2012/P1D", 0, ImmutableList.of("g"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L, new KafkaTopicPartition(false, topic, 1), 1L))
        ),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRestore() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 6L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);

    // Insert some data, but not enough for the task to finish
    insertData(Iterables.limit(records, 4));

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
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 6L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert remaining data
    insertData(Iterables.skip(records, 4));

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    verifyTaskMetrics(task1, RowMeters.with().bytes(getTotalSizeOfRecords(2, 4)).totalProcessed(2));
    verifyTaskMetrics(task2, RowMeters.with().bytes(getTotalSizeOfRecords(4, 5)).totalProcessed(1));

    // Check published segments & metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 6L))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRestoreAfterPersistingSequences() throws Exception
  {
    records = generateSinglePartitionRecords(topic);
    maxRowsPerSegment = 2;
    Map<String, Object> consumerProps = kafkaServer.consumerProperties();
    consumerProps.put("max.poll.records", "1");

    final KafkaIndexTask task1 = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 10L)),
            consumerProps,
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final SeekableStreamStartSequenceNumbers<KafkaTopicPartition, Long> checkpoint = new SeekableStreamStartSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L),
        ImmutableSet.of(new KafkaTopicPartition(false, topic, 0))
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);

    // Insert some data, but not enough for the task to finish
    insertData(Iterables.limit(records, 5));

    while (task1.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    final Map<KafkaTopicPartition, Long> currentOffsets = ImmutableMap.copyOf(task1.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpoint.getPartitionSequenceNumberMap(), currentOffsets);
    // Set endOffsets to persist sequences
    task1.getRunner().setEndOffsets(ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L), false);

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
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 10L)),
            consumerProps,
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    // Wait for the task to start reading

    // Insert remaining data
    insertData(Iterables.skip(records, 5));

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    verifyTaskMetrics(task1, RowMeters.with().bytes(getTotalSizeOfRecords(0, 5)).totalProcessed(5));
    verifyTaskMetrics(task2, RowMeters.with().bytes(getTotalSizeOfRecords(6, 10)).totalProcessed(4));

    // Check published segments & metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0),
            sdd("2008/P1D", 1),
            sdd("2009/P1D", 0),
            sdd("2009/P1D", 1),
            sdd("2010/P1D", 0),
            sdd("2011/P1D", 0),
            sdd("2012/P1D", 0)
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 10L))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunWithPauseAndResume() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 6L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Insert some data, but not enough for the task to finish
    insertData(Iterables.limit(records, 4));

    while (countEvents(task) != 2) {
      Thread.sleep(25);
    }
    Assert.assertEquals(2, countEvents(task));
    Assert.assertEquals(Status.READING, task.getRunner().getStatus());

    Map<KafkaTopicPartition, Long> currentOffsets = OBJECT_MAPPER.readValue(
        task.getRunner().pause().getEntity().toString(),
        new TypeReference<Map<KafkaTopicPartition, Long>>()
        {
        }
    );
    Assert.assertEquals(Status.PAUSED, task.getRunner().getStatus());
    // Insert remaining data
    insertData(Iterables.skip(records, 4));

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
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(2, 5)).totalProcessed(3));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 6L))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunWithOffsetOutOfRangeExceptionAndPause() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
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
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 200L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 500L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    runTask(task);

    while (!task.getRunner().getStatus().equals(Status.READING)) {
      Thread.sleep(20);
    }

    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(Status.READING, task.getRunner().getStatus());
      // Offset should not be reset
      Assert.assertEquals(200L, (long) task.getRunner().getCurrentOffsets().get(new KafkaTopicPartition(false, topic, 0)));
    }
  }

  @Test(timeout = 60_000L)
  public void testRunContextSequenceAheadOfStartingOffsets() throws Exception
  {
    // Insert data
    insertData();

    final TreeMap<Integer, Map<KafkaTopicPartition, Long>> sequences = new TreeMap<>();
    // Here the sequence number is 1 meaning that one incremental handoff was done by the failed task
    // and this task should start reading from offset 2 for partition 0
    sequences.put(1, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L));
    final Map<String, Object> context = new HashMap<>();
    context.put(
        SeekableStreamSupervisor.CHECKPOINTS_CTX_KEY,
        OBJECT_MAPPER.writerFor(KafkaSupervisor.CHECKPOINTS_TYPE_REF).writeValueAsString(sequences)
    );

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            // task should ignore these and use sequence info sent in the context
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        ),
        context
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(2, 5)).totalProcessed(3));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunWithDuplicateRequest() throws Exception
  {
    // Insert data
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 200L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 500L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    runTask(task);

    while (!task.getRunner().getStatus().equals(Status.READING)) {
      Thread.sleep(20);
    }

    // first setEndOffsets request
    task.getRunner().pause();
    task.getRunner().setEndOffsets(ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 500L), true);
    Assert.assertEquals(Status.READING, task.getRunner().getStatus());

    // duplicate setEndOffsets request
    task.getRunner().pause();
    task.getRunner().setEndOffsets(ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 500L), true);
    Assert.assertEquals(Status.READING, task.getRunner().getStatus());
  }

  @Test(timeout = 60_000L)
  public void testRunTransactionModeRollback() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L), ImmutableSet.of()),

            // End offset is one after 12 real messages + 2 txn control messages (last seen message: offset 13).
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 14L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Insert 2 records initially
    insertData(records.subList(0, 2));

    awaitConsumedOffsets(task, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 1L)); // Consume two real messages
    Assert.assertEquals(2, countEvents(task));
    Assert.assertEquals(Status.READING, task.getRunner().getStatus());

    //verify the 2 indexed records
    final QuerySegmentSpec firstInterval = OBJECT_MAPPER.readValue("\"2008/2010\"", QuerySegmentSpec.class);
    Iterable<Map<String, Object>> scanResultValues = scanData(task, firstInterval);
    Assert.assertEquals(2, Iterables.size(scanResultValues));

    // Insert 3 more records and rollback
    insertData(records.subList(2, 5), true);

    // Insert up through first 8 items
    insertData(records.subList(5, 8));

    awaitConsumedOffsets(task, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 9L)); // Consume 8 real messages + 2 txn controls
    Assert.assertEquals(2, countEvents(task));

    final QuerySegmentSpec rollbackedInterval = OBJECT_MAPPER.readValue("\"2010/2012\"", QuerySegmentSpec.class);
    scanResultValues = scanData(task, rollbackedInterval);
    //verify that there are no records indexed in the rollbacked time period
    Assert.assertEquals(0, Iterables.size(scanResultValues));

    final QuerySegmentSpec endInterval = OBJECT_MAPPER.readValue("\"2008/2049\"", QuerySegmentSpec.class);
    Iterable<Map<String, Object>> scanResultValues1 = scanData(task, endInterval);
    Assert.assertEquals(2, Iterables.size(scanResultValues1));

    // Insert all remaining messages. One will get picked up.
    insertData(Iterables.skip(records, 8));

    // Wait for task to exit and publish
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    Assert.assertEquals(task.getRunner().getEndOffsets(), task.getRunner().getCurrentOffsets());

    long totalBytes = getTotalSizeOfRecords(0, 2) + getTotalSizeOfRecords(5, 11);
    verifyTaskMetrics(task, RowMeters.with().bytes(totalBytes)
                                     .unparseable(3).errors(1).thrownAway(1).totalProcessed(3));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0, ImmutableList.of("a")),
            sdd("2009/P1D", 0, ImmutableList.of("b")),
            sdd("2013/P1D", 0, ImmutableList.of("f")),
            sdd("2049/P1D", 0, ImmutableList.of("f"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 14L))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunUnTransactionMode() throws Exception
  {
    Map<String, Object> configs = kafkaServer.consumerProperties();
    configs.put("isolation.level", "read_uncommitted");
    final KafkaIndexTask task = createTask(
            null,
            new KafkaIndexTaskIOConfig(
                    0,
                    "sequence0",
                    new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L), ImmutableSet.of()),
                    new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 13L)),
                    configs,
                    KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
                    true,
                    null,
                    null,
                    INPUT_FORMAT,
                    null
            )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Insert 2 records initially
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      kafkaProducer.initTransactions();
      kafkaProducer.beginTransaction();
      for (ProducerRecord<byte[], byte[]> record : Iterables.limit(records, 2)) {
        kafkaProducer.send(record).get();
      }
      kafkaProducer.flush();
      kafkaProducer.abortTransaction();
    }

    while (countEvents(task) != 2) {
      Thread.sleep(25);
    }

    Assert.assertEquals(2, countEvents(task));
  }

  @Test(timeout = 60_000L)
  public void testCanStartFromLaterThanEarliestOffset() throws Exception
  {
    final String baseSequenceName = "sequence0";
    maxRowsPerSegment = Integer.MAX_VALUE;
    maxTotalRows = null;

    insertData();

    Map<String, Object> consumerProps = kafkaServer.consumerProperties();
    consumerProps.put("max.poll.records", "1");

    final SeekableStreamStartSequenceNumbers<KafkaTopicPartition, Long> startPartitions = new SeekableStreamStartSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L, new KafkaTopicPartition(false, topic, 1), 1L),
        ImmutableSet.of()
    );

    final SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> endPartitions = new SeekableStreamEndSequenceNumbers<>(
        topic,
        ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 10L, new KafkaTopicPartition(false, topic, 1), 2L)
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
            INPUT_FORMAT,
            null
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
  }

  @Test(timeout = 60_000L)
  public void testRunWithoutDataInserted() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 5L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    Thread.sleep(1000);

    Assert.assertEquals(0, countEvents(task));
    Assert.assertEquals(SeekableStreamIndexTaskRunner.Status.READING, task.getRunner().getStatus());

    task.getRunner().stopGracefully();

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().totalProcessed(0));

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(Collections.emptyList(), publishedDescriptors());
    Assert.assertNull(newDataSchemaMetadata());
  }

  @Test
  public void testSerde() throws Exception
  {
    // This is both a serde test and a regression test for https://github.com/apache/druid/issues/7724.

    final KafkaIndexTask task = createTask(
        "taskid",
        NEW_DATA_SCHEMA.withTransformSpec(
            new TransformSpec(
                null,
                ImmutableList.of(new ExpressionTransform("beep", "nofunc()", ExprMacroTable.nil()))
            )
        ),
        new KafkaIndexTaskIOConfig(
            0,
            "sequence",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of()),
            ImmutableMap.of(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final Task task1 = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsBytes(task), Task.class);
    Assert.assertEquals(task, task1);
  }

  @Test
  public void testCorrectInputSources() throws Exception
  {
    // This is both a serde test and a regression test for https://github.com/apache/druid/issues/7724.

    final KafkaIndexTask task = createTask(
        "taskid",
        NEW_DATA_SCHEMA.withTransformSpec(
            new TransformSpec(
                null,
                ImmutableList.of(new ExpressionTransform("beep", "nofunc()", ExprMacroTable.nil()))
            )
        ),
        new KafkaIndexTaskIOConfig(
            0,
            "sequence",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of()),
            ImmutableMap.of(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    Assert.assertEquals(
        Collections.singleton(
            new ResourceAction(new Resource(
                KafkaIndexTaskModule.SCHEME,
                ResourceType.EXTERNAL
            ), Action.READ)),
        task.getInputSourceResources()
    );
  }


  /**
   * Wait for a task to consume certain offsets (inclusive).
   */
  private void awaitConsumedOffsets(final KafkaIndexTask task, final Map<KafkaTopicPartition, Long> targetOffsets)
      throws InterruptedException
  {
    while (true) {
      final ConcurrentMap<KafkaTopicPartition, Long> currentOffsets = task.getRunner().getCurrentOffsets();

      // For Kafka, currentOffsets are the last read offsets plus one.
      boolean allDone = true;
      for (final Map.Entry<KafkaTopicPartition, Long> entry : targetOffsets.entrySet()) {
        final Long currentOffset = currentOffsets.get(entry.getKey());
        if (currentOffset == null || currentOffset <= entry.getValue()) {
          allDone = false;
          break;
        }
      }

      if (allDone) {
        return;
      } else {
        Thread.sleep(5);
      }
    }
  }

  private List<Map<String, Object>> scanData(final Task task, QuerySegmentSpec spec)
  {
    ScanQuery query = new Druids.ScanQueryBuilder().dataSource(NEW_DATA_SCHEMA.getDataSource())
                                                   .intervals(spec)
                                                   .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                                                   .build();

    return task.getQueryRunner(query)
               .run(QueryPlus.wrap(query))
               .flatMap(result -> Sequences.simple((List<Map<String, Object>>) result.getEvents()))
               .toList();
  }

  private long getRecordSize(ProducerRecord<byte[], byte[]> record)
  {
    return (record.key() == null ? 0L : record.key().length)
           + (record.value() == null ? 0L : record.value().length);
  }

  private long getTotalSizeOfRecords(int startIndexInclusive, int endIndexExclusive)
  {
    return records.subList(startIndexInclusive, endIndexExclusive).stream()
                  .mapToLong(this::getRecordSize)
                  .sum();
  }

  private void insertData() throws ExecutionException, InterruptedException
  {
    insertData(records);
  }

  private void insertData(Iterable<ProducerRecord<byte[], byte[]>> records)
      throws ExecutionException, InterruptedException
  {
    insertData(records, false);
  }

  private void insertData(Iterable<ProducerRecord<byte[], byte[]>> records, boolean rollbackTransaction)
      throws ExecutionException, InterruptedException
  {
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      kafkaProducer.initTransactions();
      kafkaProducer.beginTransaction();
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
      if (rollbackTransaction) {
        kafkaProducer.abortTransaction();
      } else {
        kafkaProducer.commitTransaction();
      }
    }
  }

  private KafkaIndexTask createTask(
      final String taskId,
      final KafkaIndexTaskIOConfig ioConfig
  ) throws JsonProcessingException
  {
    return createTask(taskId, NEW_DATA_SCHEMA, ioConfig);
  }

  private KafkaIndexTask createTask(
      final String taskId,
      final KafkaIndexTaskIOConfig ioConfig,
      final Map<String, Object> context
  ) throws JsonProcessingException
  {
    return createTask(taskId, NEW_DATA_SCHEMA, ioConfig, context);
  }

  private KafkaIndexTask createTask(
      final String taskId,
      final DataSchema dataSchema,
      final KafkaIndexTaskIOConfig ioConfig
  ) throws JsonProcessingException
  {
    final Map<String, Object> context = new HashMap<>();
    return createTask(taskId, dataSchema, ioConfig, context);
  }

  private KafkaIndexTask createTask(
      final String taskId,
      final DataSchema dataSchema,
      final KafkaIndexTaskIOConfig ioConfig,
      final Map<String, Object> context
  ) throws JsonProcessingException
  {
    final KafkaIndexTaskTuningConfig tuningConfig = new KafkaIndexTaskTuningConfig(
        null,
        1000,
        null,
        null,
        maxRowsPerSegment,
        maxTotalRows,
        new Period("P1Y"),
        null,
        null,
        null,
        null,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        null,
        intermediateHandoffPeriod,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        null
    );
    if (!context.containsKey(SeekableStreamSupervisor.CHECKPOINTS_CTX_KEY)) {
      final TreeMap<Integer, Map<KafkaTopicPartition, Long>> checkpoints = new TreeMap<>();
      checkpoints.put(0, ioConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap());
      final String checkpointsJson = OBJECT_MAPPER
          .writerFor(KafkaSupervisor.CHECKPOINTS_TYPE_REF)
          .writeValueAsString(checkpoints);
      context.put(SeekableStreamSupervisor.CHECKPOINTS_CTX_KEY, checkpointsJson);
    }

    final KafkaIndexTask task = new KafkaIndexTask(
        taskId,
        null,
        cloneDataSchema(dataSchema),
        tuningConfig,
        ioConfig,
        context,
        OBJECT_MAPPER
    );
    task.setPollRetryMs(POLL_RETRY_MS);
    return task;
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

  @Override
  protected QueryRunnerFactoryConglomerate makeQueryRunnerConglomerate()
  {
    return new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>builder()
                    .put(
                        TimeseriesQuery.class,
                        new TimeseriesQueryRunnerFactory(
                            new TimeseriesQueryQueryToolChest(),
                            new TimeseriesQueryEngine(),
                            (query, future) -> {
                              // do nothing
                            }
                        )
                    )
                    .put(
                        ScanQuery.class,
                        new ScanQueryRunnerFactory(
                            new ScanQueryQueryToolChest(
                                new ScanQueryConfig(),
                                new DefaultGenericQueryMetricsFactory()
                            ),
                            new ScanQueryEngine(),
                            new ScanQueryConfig()
                        )
                    )
                    .build()
    );
  }

  private void makeToolboxFactory() throws IOException
  {
    directory = tempFolder.newFolder();
    final TestUtils testUtils = new TestUtils();
    final ObjectMapper objectMapper = testUtils.getTestObjectMapper();

    for (Module module : new KafkaIndexTaskModule().getJacksonModules()) {
      objectMapper.registerModule(module);
    }
    objectMapper.registerModule(TEST_MODULE);
    makeToolboxFactory(testUtils, emitter, doHandoff);
  }

  @Test(timeout = 60_000L)
  public void testMultipleLinesJSONText() throws Exception
  {
    reportParseExceptions = false;
    maxParseExceptions = 1000;
    maxSavedParseExceptions = 2;

    // Prepare records and insert data
    //multiple objects in one Kafka record will yield 2 rows in druid
    String wellformed = toJsonString(true, "2049", "d2", "y", "10", "22.0", "2.0") +
                        toJsonString(true, "2049", "d3", "y", "10", "23.0", "3.0");

    //multiple objects in one Kafka record but some objects are in ill-formed format
    //as a result, the whole ProducerRecord will be discarded
    String malformed =
        "{\"timestamp\":2049, \"dim1\": \"d4\", \"dim2\":\"x\", \"dimLong\": 10, \"dimFloat\":\"24.0\", \"met1\":\"2.0\" }" +
        "{\"timestamp\":2049, \"dim1\": \"d5\", \"dim2\":\"y\", \"dimLong\": 10, \"dimFloat\":\"24.0\", \"met1\":invalidFormat }" +
        "{\"timestamp\":2049, \"dim1\": \"d6\", \"dim2\":\"z\", \"dimLong\": 10, \"dimFloat\":\"24.0\", \"met1\":\"3.0\" }";

    List<ProducerRecord<byte[], byte[]>> producerRecords = Arrays.asList(
        new ProducerRecord<>(topic, 0, null, jbb(true, "2049", "d1", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8(wellformed)),
        new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8(malformed)),
        //a well-formed record after ill-formed to demonstrate that the ill-formed can be successfully skipped
        new ProducerRecord<>(topic, 0, null, jbb(true, "2049", "d7", "y", "10", "20.0", "1.0"))
    );
    records = producerRecords;
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 4L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(0, 4)).unparseable(1).totalProcessed(4));

    // Check published metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            // 4 rows at last in druid
            sdd("2049/P1D", 0, ImmutableList.of("d1", "d2", "d3", "d7"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 4L))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testParseExceptionsInIteratorConstructionSuccess() throws Exception
  {
    reportParseExceptions = false;
    maxParseExceptions = 2;
    maxSavedParseExceptions = 2;

    // Prepare records and insert data
    records = Arrays.asList(
        new ProducerRecord<byte[], byte[]>(topic, 0, null,
                                           jbb("2049", "a", "y", "10", "20.0", "1.0"),
                                           SAMPLE_HEADERS),
        new ProducerRecord<byte[], byte[]>(topic, 0, null,
                                           jbb("200", TestKafkaFormatWithMalformedDataDetection.MALFORMED_KEY, "y", "10", "20.0", "1.0"),
                                           SAMPLE_HEADERS),
        new ProducerRecord<byte[], byte[]>(topic, 0, null,
                                           jbb("2009", TestKafkaFormatWithMalformedDataDetection.MALFORMED_KEY, "y", "10", "20.0", "1.0"),
                                           SAMPLE_HEADERS),
        new ProducerRecord<byte[], byte[]>(topic, 0, null,
                                           jbb("2049", "b", "y", "10", "21.0", "1.0"),
                                           SAMPLE_HEADERS)
    );
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 4L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            new TestKafkaFormatWithMalformedDataDetection(INPUT_FORMAT),
            null
        )
    );

    Assert.assertTrue(task.supportsQueries());
    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(0, 4)).unparseable(2).totalProcessed(2));

    // Check published metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            // 2 rows at last in druid
            sdd("2049/P1D", 0, ImmutableList.of("a", "b"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 4L))),
        newDataSchemaMetadata()
    );

    // Verify unparseable data
    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    ParseExceptionReport parseExceptionReport =
        ParseExceptionReport.forPhase(reportData, RowIngestionMeters.BUILD_SEGMENTS);

    List<String> expectedMessages = Arrays.asList(
        "Unable to parse malformed data during iterator construction",
        "Unable to parse malformed data during iterator construction"
    );
    Assert.assertEquals(expectedMessages, parseExceptionReport.getErrorMessages());
  }

  @Test(timeout = 60_000L)
  public void testNoParseExceptionsTaskSucceeds() throws Exception
  {
    reportParseExceptions = false;
    maxParseExceptions = 0;
    maxSavedParseExceptions = 0;

    // Prepare records and insert data
    records = Arrays.asList(
        new ProducerRecord<byte[], byte[]>(topic, 0, null,
                                           jbb("2049", "a", "y", "10", "20.0", "1.0"),
                                           SAMPLE_HEADERS),
        new ProducerRecord<byte[], byte[]>(topic, 0, null,
                                           jbb("2049", "b", "y", "10", "20.0", "1.0"),
                                           SAMPLE_HEADERS)
    );
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            new TestKafkaFormatWithMalformedDataDetection(INPUT_FORMAT),
            null
        )
    );

    Assert.assertTrue(task.supportsQueries());
    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(0, 2)).unparseable(0).totalProcessed(2));

    // Check published metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            // 2 rows at last in druid
            sdd("2049/P1D", 0, ImmutableList.of("a", "b"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 2L))),
        newDataSchemaMetadata()
    );

    // Verify there is no unparseable data in the report since we've no parse exceptions and no saved parse exceptions
    ParseExceptionReport parseExceptionReport =
        ParseExceptionReport.forPhase(getTaskReportData(), RowIngestionMeters.BUILD_SEGMENTS);

    Assert.assertEquals(ImmutableList.of(), parseExceptionReport.getErrorMessages());
  }

  @Test(timeout = 60_000L)
  public void testParseExceptionsBeyondThresholdTaskFails() throws Exception
  {
    reportParseExceptions = false;
    maxParseExceptions = 1;
    maxSavedParseExceptions = 0;

    // Prepare records and insert data
    records = Arrays.asList(
        new ProducerRecord<byte[], byte[]>(topic, 0, null,
                                           jbb("2049", "a", "y", "10", "20.0", "1.0"),
                                           SAMPLE_HEADERS),
        new ProducerRecord<byte[], byte[]>(topic, 0, null,
                                           jbb("200", TestKafkaFormatWithMalformedDataDetection.MALFORMED_KEY, "y1", "10", "20.0", "1.0"),
                                           SAMPLE_HEADERS),
        new ProducerRecord<byte[], byte[]>(topic, 0, null,
                                           jbb("2009", TestKafkaFormatWithMalformedDataDetection.MALFORMED_KEY, "y2", "10", "20.0", "1.0"),
                                           SAMPLE_HEADERS),
        new ProducerRecord<byte[], byte[]>(topic, 0, null,
                                           jbb("2049", "b", "y", "10", "21.0", "1.0"),
                                           SAMPLE_HEADERS),
        new ProducerRecord<byte[], byte[]>(topic, 0, null,
                                           jbb("200", TestKafkaFormatWithMalformedDataDetection.MALFORMED_KEY, "y3", "10", "20.0", "1.0"),
                                           SAMPLE_HEADERS),
        new ProducerRecord<byte[], byte[]>(topic, 0, null,
                                           jbb("2049", "c", "y", "10", "21.0", "1.0"),
                                           SAMPLE_HEADERS)
    );
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(topic, ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 6L)),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            new TestKafkaFormatWithMalformedDataDetection(INPUT_FORMAT),
            null
        )
    );

    Assert.assertTrue(task.supportsQueries());
    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit. Should fail and trip up with the first two bad messages in the stream
    Assert.assertEquals(TaskState.FAILED, future.get().getStatusCode());
    verifyTaskMetrics(task, RowMeters.with().bytes(getTotalSizeOfRecords(0, 3)).unparseable(2).totalProcessed(1));

    // Check there's no published metadata since the task failed
    Assert.assertEquals(ImmutableList.of(), publishedDescriptors());
    Assert.assertNull(newDataSchemaMetadata());

    // Verify there is no unparseable data in the report since we've 0 saved parse exceptions
    ParseExceptionReport parseExceptionReport =
        ParseExceptionReport.forPhase(getTaskReportData(), RowIngestionMeters.BUILD_SEGMENTS);

    Assert.assertEquals(ImmutableList.of(), parseExceptionReport.getErrorMessages());
  }

  @Test(timeout = 60_000L)
  public void testCompletionReportPartitionStats() throws Exception
  {
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(
                topic,
                ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 0L),
                ImmutableSet.of()
            ),
            new SeekableStreamEndSequenceNumbers<>(
                topic,
                ImmutableMap.of(new KafkaTopicPartition(false, topic, 0), 10L)
            ),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);
    TaskStatus status = future.get();

    Assert.assertEquals(TaskState.SUCCESS, status.getStatusCode());
    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();
    Assert.assertEquals(reportData.getRecordsProcessed().size(), 1);
    Assert.assertEquals(reportData.getRecordsProcessed().values().iterator().next(), (Long) 6L);
  }

  @Test(timeout = 60_000L)
  public void testCompletionReportMultiplePartitionStats() throws Exception
  {
    insertData();

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(
                topic,
                ImmutableMap.of(
                    new KafkaTopicPartition(false, topic, 0),
                    0L,
                    new KafkaTopicPartition(false, topic, 1),
                    0L
                ),
                ImmutableSet.of()
            ),
            new SeekableStreamEndSequenceNumbers<>(
                topic,
                ImmutableMap.of(
                    new KafkaTopicPartition(false, topic, 0),
                    10L,
                    new KafkaTopicPartition(false, topic, 1),
                    2L
                )
            ),
            kafkaServer.consumerProperties(),
            KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            true,
            null,
            null,
            INPUT_FORMAT,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);
    TaskStatus status = future.get();

    Assert.assertEquals(TaskState.SUCCESS, status.getStatusCode());
    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();
    Assert.assertEquals(reportData.getRecordsProcessed().size(), 2);
    Assert.assertTrue(reportData.getRecordsProcessed().values().containsAll(ImmutableSet.of(6L, 2L)));
  }

  public static class TestKafkaInputFormat implements InputFormat
  {
    final InputFormat baseInputFormat;

    @JsonCreator
    public TestKafkaInputFormat(@JsonProperty("baseInputFormat") InputFormat baseInputFormat)
    {
      this.baseInputFormat = baseInputFormat;
    }

    @Override
    public boolean isSplittable()
    {
      return false;
    }

    @Override
    public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
    {
      final SettableByteEntity<KafkaRecordEntity> settableByteEntity = (SettableByteEntity<KafkaRecordEntity>) source;
      final InputEntityReader delegate = baseInputFormat.createReader(inputRowSchema, source, temporaryDirectory);
      return new InputEntityReader()
      {
        @Override
        public CloseableIterator<InputRow> read() throws IOException
        {
          KafkaRecordEntity recordEntity = settableByteEntity.getEntity();
          return delegate.read().map(
              r -> {
                MapBasedInputRow row = (MapBasedInputRow) r;
                final Map<String, Object> event = new HashMap<>(row.getEvent());
                event.put("kafka.offset", recordEntity.getRecord().offset());
                event.put("kafka.topic", recordEntity.getRecord().topic());
                event.put(
                    "kafka.header.encoding",
                    new String(
                        recordEntity.getRecord().headers().lastHeader("encoding").value(),
                        StandardCharsets.UTF_8
                    )
                );

                return new MapBasedInputRow(row.getTimestamp(), row.getDimensions(), event);
              }
          );
        }

        @Override
        public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
        {
          return delegate.sample();
        }
      };
    }

    @JsonProperty
    public InputFormat getBaseInputFormat()
    {
      return baseInputFormat;
    }
  }

  /**
   * This test class is a kafka input format that throws a {@link ParseException} when it encounters a malformed value
   * in its input as part of the iterator construction. This should be used only for testing purposes.
   */
  public static class TestKafkaFormatWithMalformedDataDetection implements InputFormat
  {
    public static final String MALFORMED_KEY = "malformed";
    final InputFormat baseInputFormat;

    @JsonCreator
    public TestKafkaFormatWithMalformedDataDetection(@JsonProperty("baseInputFormat") InputFormat baseInputFormat)
    {
      this.baseInputFormat = baseInputFormat;
    }

    @Override
    public boolean isSplittable()
    {
      return false;
    }

    @Override
    public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
    {
      final InputEntityReader delegate = baseInputFormat.createReader(inputRowSchema, source, temporaryDirectory);
      final SettableByteEntity<KafkaRecordEntity> settableByteEntity = (SettableByteEntity<KafkaRecordEntity>) source;

      return new InputEntityReader()
      {
        @Override
        public CloseableIterator<InputRow> read() throws IOException
        {
          KafkaRecordEntity recordEntity = settableByteEntity.getEntity();
          return delegate.read().map(
              r -> {

                MapBasedInputRow row = (MapBasedInputRow) r;
                final Map<String, Object> event = new HashMap<>(row.getEvent());

                if (event.containsValue(MALFORMED_KEY)) {
                  // Then throw an exception
                  throw new ParseException(null, "Unable to parse malformed data during iterator construction");
                }
                event.put("kafka.offset", recordEntity.getRecord().offset());
                event.put("kafka.topic", recordEntity.getRecord().topic());
                event.put(
                    "kafka.header.encoding",
                    new String(
                        recordEntity.getRecord().headers().lastHeader("encoding").value(),
                        StandardCharsets.UTF_8
                    )
                );

                return new MapBasedInputRow(row.getTimestamp(), row.getDimensions(), event);
              }
          );
        }

        @Override
        public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
        {
          return delegate.sample();
        }
      };
    }

    @JsonProperty
    public InputFormat getBaseInputFormat()
    {
      return baseInputFormat;
    }
  }
}

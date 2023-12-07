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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Binder;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorTuningConfig;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.server.security.AuthorizerMapper;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

@SuppressWarnings("unchecked")
@RunWith(Parameterized.class)
public class SeekableStreamIndexTaskRunnerTest extends SeekableStreamIndexTaskTestBase
{
  private static final String STREAM = "stream";

  private static final String DATASOURCE = "test_ds";

  private static final String MESSAGES_TEMPLATE = "{\"id\": <count>, \"age\": 11, \"timestamp\":\"2023-09-01T00:00:01.000\"}";

  private static final String LOCAL_TMP_PATH = "./tmp";

  private static final String BASE_PERSIST_DIR = LOCAL_TMP_PATH + "/persist";

  private static final int START_OFFSET = 10;

  private static RecordSupplier<String, String, ByteEntity> recordSupplier;

  private static ServiceEmitter emitter;

  private static TestSeekableStreamIndexTaskRunner taskRunner;

  public SeekableStreamIndexTaskRunnerTest(LockGranularity lockGranularity)
  {
    super(lockGranularity);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
            new Object[]{LockGranularity.TIME_CHUNK},
            new Object[]{LockGranularity.SEGMENT}
    );
  }

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
                    Execs.makeThreadFactory("runner-task-test-%d")
            )
    );
  }

  @Before
  public void setup() throws IOException
  {
    FileUtils.mkdirp(new File(LOCAL_TMP_PATH));
    FileUtils.mkdirp(new File(BASE_PERSIST_DIR));

    reportsFile = new File(LOCAL_TMP_PATH + "/task-reports.json");
    recordSupplier = new TestRecordSupplier();

    TestUtils testUtils = new TestUtils();
    final ObjectMapper objectMapper = testUtils.getTestObjectMapper();

    for (Module module : new TestIndexTaskModule().getJacksonModules()) {
      objectMapper.registerModule(module);
    }

    makeToolboxFactory(testUtils, emitter, false);
  }

  @After
  public void tearDownTest() throws IOException
  {
    synchronized (runningTasks) {
      for (Task task : runningTasks) {
        task.stopGracefully(toolboxFactory.build(task).getConfig());
      }

      runningTasks.clear();
    }

    reportsFile.delete();
    FileUtils.deleteDirectory(new File(BASE_PERSIST_DIR));
    FileUtils.deleteDirectory(new File(LOCAL_TMP_PATH));

    destroyToolboxFactory();
  }

  @Test
  public void testRunTaskWithIntermediateHandOffByMaxRowsPerSegment() throws ExecutionException, InterruptedException
  {
    TestSeekableStreamIndexTaskIOConfig taskIoConfig = new TestSeekableStreamIndexTaskIOConfig(
            0,
            STREAM,
            new SeekableStreamStartSequenceNumbers<>(STREAM, Collections.singletonMap("0", String.valueOf(START_OFFSET)), Collections.emptySet()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, Collections.singletonMap("0", "9223372036854776000")),
            null,
            null,
            null,
            new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false)
            );

    TestSeekableStreamIndexTaskTuningConfig taskTuningConfig = new TestSeekableStreamIndexTaskTuningConfig(
            null,
            2,
            null,
            false,
            5,
            null,
            null,
            new File(BASE_PERSIST_DIR),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
    );

    TestSeekableStreamIndexTask task = new TestSeekableStreamIndexTask(
            "id1",
            null,
            getDataSchema(),
            taskTuningConfig,
            taskIoConfig,
            null,
            "0"
    );

    taskRunner = new TestSeekableStreamIndexTaskRunner(task, getDataSchema().getParser(), task.authorizerMapper, LockGranularity.TIME_CHUNK);

    final ListenableFuture<TaskStatus> future = runTask(task);

    Thread.sleep(3 * 1000L); // wait for task to start

    Assert.assertEquals(SeekableStreamIndexTaskRunner.Status.PAUSED, taskRunner.getStatus());

    taskRunner.pause();

    String currOffset = taskRunner.getCurrentOffsets().get("0");
    Response response = taskRunner.setEndOffsets(Collections.singletonMap("0", currOffset), false); // will resume
    Assert.assertEquals(response.getStatus(), 200);

    Assert.assertEquals(SeekableStreamIndexTaskRunner.Status.READING, taskRunner.getStatus());

    Thread.sleep(3 * 1000L); // wait for publishing segment & reading more data
    taskRunner.stopGracefully();

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    publishedDescriptors();
    publishedSegments();
  }

  @Test
  public void testRunTaskWithoutIntermediateHandOff() throws ExecutionException, InterruptedException
  {
    TestSeekableStreamIndexTaskIOConfig taskIoConfig = new TestSeekableStreamIndexTaskIOConfig(
            0,
            STREAM,
            new SeekableStreamStartSequenceNumbers<>(STREAM, Collections.singletonMap("0", String.valueOf(START_OFFSET)), Collections.emptySet()),
            new SeekableStreamEndSequenceNumbers<>(STREAM, Collections.singletonMap("0", "9223372036854776000")),
            null,
            null,
            null,
            new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false)
    );

    TestSeekableStreamIndexTaskTuningConfig taskTuningConfig = new TestSeekableStreamIndexTaskTuningConfig(
            null,
            null,
            null,
            false,
            1000000,
            2000000L,
            null,
            new File(BASE_PERSIST_DIR),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
    );

    TestSeekableStreamIndexTask task = new TestSeekableStreamIndexTask(
            "id1",
            null,
            getDataSchema(),
            taskTuningConfig,
            taskIoConfig,
            null,
            "0"
    );

    taskRunner = new TestSeekableStreamIndexTaskRunner(task, getDataSchema().getParser(), task.authorizerMapper, LockGranularity.TIME_CHUNK);

    final ListenableFuture<TaskStatus> future = runTask(task);

    Thread.sleep(3 * 1000L); // wait for task to start

    taskRunner.stopGracefully();

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    publishedDescriptors();
    publishedSegments();
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
                    ).put(
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

  public static class TestRecordSupplier implements RecordSupplier<String, String, ByteEntity>
  {
    @Override
    public void assign(Set<StreamPartition<String>> streamPartitions)
    {
    }

    @Override
    public void seek(StreamPartition<String> partition, String sequenceNumber)
    {
    }

    @Override
    public void seekToEarliest(Set<StreamPartition<String>> streamPartitions)
    {
    }

    @Override
    public void seekToLatest(Set<StreamPartition<String>> streamPartitions)
    {
    }

    @Override
    public Collection<StreamPartition<String>> getAssignment()
    {
      return Collections.singletonList(new StreamPartition<>(STREAM, "0"));
    }

    @Override
    public @NotNull List<OrderedPartitionableRecord<String, String, ByteEntity>> poll(long timeout)
    {
      return Collections.emptyList();
    }

    @Override
    public String getLatestSequenceNumber(StreamPartition<String> partition)
    {
      return "10";
    }

    @Override
    public String getEarliestSequenceNumber(StreamPartition<String> partition)
    {
      return "10";
    }

    @Override
    public boolean isOffsetAvailable(StreamPartition<String> partition, OrderedSequenceNumber<String> offset)
    {
      return false;
    }

    @Override
    public String getPosition(StreamPartition<String> partition)
    {
      return "10";
    }

    @Override
    public Set<String> getPartitionIds(String stream)
    {
      return Sets.newHashSet("0");
    }

    @Override
    public void close()
    {
    }
  }

  public static class TestSeekableStreamSupervisorTuningConfig implements SeekableStreamSupervisorTuningConfig
  {
    @Override
    public Integer getWorkerThreads()
    {
      return 1;
    }

    @Override
    public Long getChatRetries()
    {
      return 1L;
    }

    @Override
    public Duration getHttpTimeout()
    {
      return new Period("PT10S").toStandardDuration();
    }

    @Override
    public Duration getShutdownTimeout()
    {
      return new Period("PT30S").toStandardDuration();
    }

    @Override
    public Duration getRepartitionTransitionDuration()
    {
      return new Period("PT2M").toStandardDuration();
    }

    @Override
    public Duration getOffsetFetchPeriod()
    {
      return new Period("PT10S").toStandardDuration();
    }

    @Override
    public TestSeekableStreamIndexTaskTuningConfig convertToTaskTuningConfig()
    {
      return new TestSeekableStreamIndexTaskTuningConfig(
                null,
                null,
                null,
                false,
                null,
                null,
                null,
                new File(BASE_PERSIST_DIR),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
      );
    }
  }

  public static class TestSeekableStreamIndexTaskTuningConfig extends SeekableStreamIndexTaskTuningConfig
  {
    @JsonCreator
    public TestSeekableStreamIndexTaskTuningConfig(
        @JsonProperty("appendableIndexSpec") @Nullable AppendableIndexSpec appendableIndexSpec,
        @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
        @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
        @JsonProperty("skipBytesInMemoryOverheadCheck") @Nullable Boolean skipBytesInMemoryOverheadCheck,
        @JsonProperty("maxRowsPerSegment") @Nullable Integer maxRowsPerSegment,
        @JsonProperty("maxTotalRows") @Nullable Long maxTotalRows,
        @JsonProperty("intermediatePersistPeriod") @Nullable Period intermediatePersistPeriod,
        @JsonProperty("basePersistDirectory") @Nullable File basePersistDirectory,
        @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
        @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
        @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
        @Deprecated @JsonProperty("reportParseExceptions") @Nullable Boolean reportParseExceptions,
        @JsonProperty("handoffConditionTimeout") @Nullable Long handoffConditionTimeout,
        @JsonProperty("resetOffsetAutomatically") @Nullable Boolean resetOffsetAutomatically,
        @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
        @JsonProperty("intermediateHandoffPeriod") @Nullable Period intermediateHandoffPeriod,
        @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
        @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
        @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions
    )
    {
      super(
                appendableIndexSpec,
                maxRowsInMemory,
                maxBytesInMemory,
                skipBytesInMemoryOverheadCheck,
                maxRowsPerSegment,
                maxTotalRows,
                intermediatePersistPeriod,
                basePersistDirectory,
                maxPendingPersists,
                indexSpec,
                indexSpecForIntermediatePersists,
                reportParseExceptions,
                handoffConditionTimeout,
                resetOffsetAutomatically,
                false,
                segmentWriteOutMediumFactory,
                intermediateHandoffPeriod,
                logParseExceptions,
                maxParseExceptions,
                maxSavedParseExceptions
      );
    }

    @Override
    public SeekableStreamIndexTaskTuningConfig withBasePersistDirectory(File dir)
    {
      return new TestSeekableStreamIndexTaskTuningConfig(
                getAppendableIndexSpec(),
                getMaxRowsInMemory(),
                getMaxBytesInMemory(),
                isSkipBytesInMemoryOverheadCheck(),
                getMaxRowsPerSegment(),
                getMaxTotalRows(),
                getIntermediatePersistPeriod(),
                dir,
                0,
                getIndexSpec(),
                getIndexSpecForIntermediatePersists(),
                isReportParseExceptions(),
                getHandoffConditionTimeout(),
                isResetOffsetAutomatically(),
                getSegmentWriteOutMediumFactory(),
                getIntermediateHandoffPeriod(),
                isLogParseExceptions(),
                getMaxParseExceptions(),
                getMaxSavedParseExceptions()
        );
    }

    @Override
    public String toString()
    {
      return "TestSeekableStreamIndexTaskTuningConfig{" +
                "maxRowsInMemory=" + getMaxRowsInMemory() +
                ", maxRowsPerSegment=" + getMaxRowsPerSegment() +
                ", maxTotalRows=" + getMaxTotalRows() +
                ", maxBytesInMemory=" + getMaxBytesInMemory() +
                ", skipBytesInMemoryOverheadCheck=" + isSkipBytesInMemoryOverheadCheck() +
                ", intermediatePersistPeriod=" + getIntermediatePersistPeriod() +
                ", indexSpec=" + getIndexSpec() +
                ", indexSpecForIntermediatePersists=" + getIndexSpecForIntermediatePersists() +
                ", reportParseExceptions=" + isReportParseExceptions() +
                ", handoffConditionTimeout=" + getHandoffConditionTimeout() +
                ", resetOffsetAutomatically=" + isResetOffsetAutomatically() +
                ", segmentWriteOutMediumFactory=" + getSegmentWriteOutMediumFactory() +
                ", intermediateHandoffPeriod=" + getIntermediateHandoffPeriod() +
                ", logParseExceptions=" + isLogParseExceptions() +
                ", maxParseExceptions=" + getMaxParseExceptions() +
                ", maxSavedParseExceptions=" + getMaxSavedParseExceptions() +
                '}';
    }
  }

  public static class TestSeekableStreamIndexTask extends SeekableStreamIndexTask<String, String, ByteEntity>
  {
    @JsonCreator
    public TestSeekableStreamIndexTask(
            @JsonProperty("id") String id,
            @JsonProperty("resource") TaskResource taskResource,
            @JsonProperty("dataSchema") DataSchema dataSchema,
            @JsonProperty("tuningConfig") TestSeekableStreamIndexTaskTuningConfig tuningConfig,
            @JsonProperty("ioConfig") TestSeekableStreamIndexTaskIOConfig ioConfig,
            @JsonProperty("context") Map<String, Object> context,
            @JsonProperty("groupId") String groupId
    )
    {
      super(id,
              taskResource,
              dataSchema,
              tuningConfig,
              ioConfig,
              context,
              groupId
      );
    }

    @Override
    protected SeekableStreamIndexTaskRunner<String, String, ByteEntity> createTaskRunner()
    {
      return taskRunner;
    }

    @Override
    protected RecordSupplier<String, String, ByteEntity> newTaskRecordSupplier(final TaskToolbox toolbox)
    {
      return recordSupplier;
    }

    @Override
    public String getType()
    {
      return "index_test";
    }
  }

  private static DataSchema getDataSchema()
  {
    List<DimensionSchema> dimensions = new ArrayList<>();
    dimensions.add(StringDimensionSchema.create("id"));

    return new DataSchema(
            DATASOURCE,
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(dimensions),
            new AggregatorFactory[]{
                    // new CountAggregatorFactory("count")
            },
            new UniformGranularitySpec(
                    Granularities.HOUR,
                    Granularities.MINUTE,
                    false,
                    ImmutableList.of()
            ),
            null
    );
  }

  public static class TestSeekableStreamIndexTaskRunner extends SeekableStreamIndexTaskRunner<String, String, ByteEntity>
  {
    private int count = 0;

    public TestSeekableStreamIndexTaskRunner(SeekableStreamIndexTask<String, String, ByteEntity> task, InputRowParser<ByteBuffer> parser, AuthorizerMapper authorizerMapper, LockGranularity lockGranularityToUse)
    {
      super(task, parser, authorizerMapper, lockGranularityToUse);
    }

    @Override
    protected boolean isEndOfShard(String seqNum)
    {
      return false;
    }

    @Override
    protected TreeMap<Integer, Map<String, String>> getCheckPointsFromContext(TaskToolbox toolbox, String checkpointsString)
    {
      return null;
    }

    @Override
    protected String getNextStartOffset(String sequenceNumber)
    {
      return sequenceNumber;
    }

    @Override
    protected SeekableStreamEndSequenceNumbers<String, String> deserializePartitionsFromMetadata(ObjectMapper mapper, Object object)
    {
      return mapper.convertValue(object, mapper.getTypeFactory().constructParametricType(
                SeekableStreamEndSequenceNumbers.class,
                String.class,
                String.class
      ));
    }

    @Override
    protected @NotNull List<OrderedPartitionableRecord<String, String, ByteEntity>> getRecords(RecordSupplier<String, String, ByteEntity> recordSupplier, TaskToolbox toolbox)
    {
      int currOffset = START_OFFSET + (count++);
      String msg = MESSAGES_TEMPLATE.replace("<count>", String.valueOf(count));

      System.out.println(msg);

      return Collections.singletonList(
              new OrderedPartitionableRecord<>(STREAM, "0", String.valueOf(currOffset) , Collections.singletonList(new ByteEntity(msg.getBytes(StandardCharsets.UTF_8))))
      );
    }

    @Override
    protected SeekableStreamDataSourceMetadata<String, String> createDataSourceMetadata(SeekableStreamSequenceNumbers<String, String> partitions)
    {
      return new TestSeekableStreamDataSourceMetadata(partitions);
    }

    @Override
    protected OrderedSequenceNumber<String> createSequenceNumber(String sequenceNumber)
    {
      return new TestSequenceNumber(sequenceNumber);
    }

    @Override
    protected void possiblyResetDataSourceMetadata(TaskToolbox toolbox, RecordSupplier<String, String, ByteEntity> recordSupplier, Set<StreamPartition<String>> assignment)
    {
    }

    @Override
    protected boolean isEndOffsetExclusive()
    {
      return true;
    }

    @Override
    protected TypeReference<List<SequenceMetadata<String, String>>> getSequenceMetadataTypeReference()
    {
      return new TypeReference<List<SequenceMetadata<String, String>>>()
      {
      };
    }
  }

  private static class TestSequenceNumber extends OrderedSequenceNumber<String>
  {
    private TestSequenceNumber(String sequenceNumber)
    {
      super(sequenceNumber, false);
    }

    public static TestSequenceNumber of(String sequenceNumber)
    {
      return new TestSequenceNumber(sequenceNumber);
    }

    @Override
    public int compareTo(OrderedSequenceNumber<String> o)
    {
      return this.get().compareTo(o.get());
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestSequenceNumber that = (TestSequenceNumber) o;
      return Objects.equals(this.get(), that.get());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(this.get());
    }
  }

  private static class TestSeekableStreamDataSourceMetadata extends SeekableStreamDataSourceMetadata<String, String>
  {
    @JsonCreator
    public TestSeekableStreamDataSourceMetadata(
            @JsonProperty("partitions") SeekableStreamSequenceNumbers<String, String> partitions
    )
    {
      super(partitions);
    }

    @Override
    public DataSourceMetadata asStartMetadata()
    {
      final SeekableStreamSequenceNumbers<String, String> sequenceNumbers = getSeekableStreamSequenceNumbers();
      if (sequenceNumbers instanceof SeekableStreamEndSequenceNumbers) {
        return createConcreteDataSourceMetaData(
                    ((SeekableStreamEndSequenceNumbers<String, String>) sequenceNumbers).asStartPartitions(true)
        );
      } else {
        return this;
      }
    }

    @Override
    protected SeekableStreamDataSourceMetadata<String, String> createConcreteDataSourceMetaData(
            SeekableStreamSequenceNumbers<String, String> seekableStreamSequenceNumbers
    )
    {
      return new TestSeekableStreamDataSourceMetadata(seekableStreamSequenceNumbers);
    }
  }

  public static class TestSeekableStreamIndexTaskIOConfig extends SeekableStreamIndexTaskIOConfig<String, String>
  {
    @JsonCreator
    public TestSeekableStreamIndexTaskIOConfig(
            @JsonProperty("taskGroupId") Integer taskGroupId,
            @JsonProperty("baseSequenceName") String baseSequenceName,
            @JsonProperty("startSequenceNumbers") SeekableStreamStartSequenceNumbers<String, String> startSequenceNumbers,
            @JsonProperty("endSequenceNumbers") SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers,
            @JsonProperty("useTransaction") Boolean useTransaction,
            @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
            @JsonProperty("maximumMessageTime") DateTime maximumMessageTime,
            @JsonProperty("inputFormat") InputFormat inputFormat)
    {
      super(taskGroupId,
              baseSequenceName,
              startSequenceNumbers,
              endSequenceNumbers,
              useTransaction,
              minimumMessageTime,
              maximumMessageTime,
              inputFormat
      );
    }
  }

  public static class TestIndexTaskModule implements DruidModule
  {
    public static final String SCHEME = "test-pha";

    @Override
    public List<? extends Module> getJacksonModules()
    {
      return ImmutableList.of(
                new SimpleModule(getClass().getSimpleName())
                        .registerSubtypes(
                                new NamedType(TestSeekableStreamIndexTask.class, "index_test"),
                                new NamedType(TestSeekableStreamIndexTaskIOConfig.class, SCHEME),
                                new NamedType(TestSeekableStreamIndexTaskTuningConfig.class, SCHEME),
                                new NamedType(TestSeekableStreamSupervisorTuningConfig.class, SCHEME)
                        )
      );
    }

    @Override
    public void configure(Binder binder)
    {
      // Nothing to do.
    }
  }
}

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoscaler;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIngestionSpec;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorReportPayload;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorTuningConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.DefaultAutoScaler;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.DummyAutoScaler;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;

public class SeekableStreamSupervisorSpecTest extends EasyMockSupport
{
  private SeekableStreamSupervisorIngestionSpec ingestionSchema;
  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private ServiceEmitter emitter;
  private RowIngestionMetersFactory rowIngestionMetersFactory;
  private DataSchema dataSchema;
  private SeekableStreamSupervisorTuningConfig seekableStreamSupervisorTuningConfig;
  private SeekableStreamSupervisorIOConfig seekableStreamSupervisorIOConfig;
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private SeekableStreamIndexTaskClientFactory taskClientFactory;
  private static final String STREAM = "stream";
  private static final String DATASOURCE = "testDS";
  private SeekableStreamSupervisorSpec spec;
  private SupervisorStateManagerConfig supervisorConfig;

  private SeekableStreamSupervisor supervisor4;

  private SeekableStreamIndexTaskClientFactory indexTaskClientFactory;
  private ObjectMapper mapper;
  private DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private SupervisorStateManagerConfig supervisorStateManagerConfig;

  @Before
  public void setUp()
  {
    ingestionSchema = EasyMock.mock(SeekableStreamSupervisorIngestionSpec.class);
    taskStorage = EasyMock.mock(TaskStorage.class);
    taskMaster = EasyMock.mock(TaskMaster.class);
    indexerMetadataStorageCoordinator = EasyMock.mock(IndexerMetadataStorageCoordinator.class);
    emitter = EasyMock.mock(ServiceEmitter.class);
    rowIngestionMetersFactory = EasyMock.mock(RowIngestionMetersFactory.class);
    dataSchema = EasyMock.mock(DataSchema.class);
    seekableStreamSupervisorTuningConfig = EasyMock.mock(SeekableStreamSupervisorTuningConfig.class);
    seekableStreamSupervisorIOConfig = EasyMock.mock(SeekableStreamSupervisorIOConfig.class);
    taskClientFactory = EasyMock.mock(SeekableStreamIndexTaskClientFactory.class);
    spec = EasyMock.mock(SeekableStreamSupervisorSpec.class);
    supervisorConfig = new SupervisorStateManagerConfig();
    indexTaskClientFactory = EasyMock.mock(SeekableStreamIndexTaskClientFactory.class);
    mapper = EasyMock.mock(ObjectMapper.class);
    monitorSchedulerConfig = EasyMock.mock(DruidMonitorSchedulerConfig.class);
    supervisorStateManagerConfig = EasyMock.mock(SupervisorStateManagerConfig.class);
    supervisor4 = EasyMock.mock(SeekableStreamSupervisor.class);
  }

  private abstract class BaseTestSeekableStreamSupervisor extends SeekableStreamSupervisor<String, String, ByteEntity>
  {
    private BaseTestSeekableStreamSupervisor()
    {
      super(
              "testSupervisorId",
              taskStorage,
              taskMaster,
              indexerMetadataStorageCoordinator,
              taskClientFactory,
              OBJECT_MAPPER,
              spec,
              rowIngestionMetersFactory,
              false
      );
    }

    @Override
    protected String baseTaskName()
    {
      return "test";
    }

    @Override
    protected void updatePartitionLagFromStream()
    {
        // do nothing
    }

    @Nullable
    @Override
    protected Map<String, Long> getPartitionRecordLag()
    {
      return null;
    }

    @Nullable
    @Override
    protected Map<String, Long> getPartitionTimeLag()
    {
      return null;
    }

    @Override
    protected SeekableStreamIndexTaskIOConfig createTaskIoConfig(
            int groupId,
            Map<String, String> startPartitions,
            Map<String, String> endPartitions,
            String baseSequenceName,
            DateTime minimumMessageTime,
            DateTime maximumMessageTime,
            Set<String> exclusiveStartSequenceNumberPartitions,
            SeekableStreamSupervisorIOConfig ioConfig
    )
    {
      return new SeekableStreamIndexTaskIOConfig<String, String>(
              groupId,
              baseSequenceName,
              new SeekableStreamStartSequenceNumbers<>(STREAM, startPartitions, exclusiveStartSequenceNumberPartitions),
              new SeekableStreamEndSequenceNumbers<>(STREAM, endPartitions),
              true,
              minimumMessageTime,
              maximumMessageTime,
              ioConfig.getInputFormat()
      )
      {
      };
    }

    @Override
    protected List<SeekableStreamIndexTask<String, String, ByteEntity>> createIndexTasks(
            int replicas,
            String baseSequenceName,
            ObjectMapper sortingMapper,
            TreeMap<Integer, Map<String, String>> sequenceOffsets,
            SeekableStreamIndexTaskIOConfig taskIoConfig,
            SeekableStreamIndexTaskTuningConfig taskTuningConfig,
            RowIngestionMetersFactory rowIngestionMetersFactory
    )
    {
      return null;
    }

    @Override
    protected int getTaskGroupIdForPartition(String partition)
    {
      return 0;
    }

    @Override
    protected boolean checkSourceMetadataMatch(DataSourceMetadata metadata)
    {
      return true;
    }

    @Override
    protected boolean doesTaskTypeMatchSupervisor(Task task)
    {
      return true;
    }

    @Override
    protected SeekableStreamDataSourceMetadata<String, String> createDataSourceMetaDataForReset(
            String stream,
            Map<String, String> map
    )
    {
      return null;
    }

    @Override
    protected OrderedSequenceNumber<String> makeSequenceNumber(String seq, boolean isExclusive)
    {
      return new OrderedSequenceNumber<String>(seq, isExclusive)
      {
        @Override
        public int compareTo(OrderedSequenceNumber<String> o)
        {
          return new BigInteger(this.get()).compareTo(new BigInteger(o.get()));
        }
      };
    }

    @Override
    protected Map<String, Long> getRecordLagPerPartition(Map<String, String> currentOffsets)
    {
      return null;
    }

    @Override
    protected Map<String, Long> getTimeLagPerPartition(Map<String, String> currentOffsets)
    {
      return null;
    }

    @Override
    protected RecordSupplier<String, String, ByteEntity> setupRecordSupplier()
    {
      return recordSupplier;
    }

    @Override
    protected SeekableStreamSupervisorReportPayload<String, String> createReportPayload(
            int numPartitions,
            boolean includeOffsets
    )
    {
      return new SeekableStreamSupervisorReportPayload<String, String>(
              DATASOURCE,
              STREAM,
              1,
              1,
              1L,
              null,
              null,
              null,
              null,
              null,
              null,
              false,
              true,
              null,
              null,
              null
      )
      {
      };
    }

    @Override
    protected String getNotSetMarker()
    {
      return "NOT_SET";
    }

    @Override
    protected String getEndOfPartitionMarker()
    {
      return "EOF";
    }

    @Override
    protected boolean isEndOfShard(String seqNum)
    {
      return false;
    }

    @Override
    protected boolean isShardExpirationMarker(String seqNum)
    {
      return false;
    }

    @Override
    protected boolean useExclusiveStartSequenceNumberForNonFirstSequence()
    {
      return false;
    }
  }

  private class TestSeekableStreamSupervisor extends BaseTestSeekableStreamSupervisor
  {
    @Override
    protected void scheduleReporting(ScheduledExecutorService reportingExec)
    {
        // do nothing
    }

    @Override
    public void collectLag(ArrayList<Long> lags)
    {
    }
  }


  private class TesstSeekableStreamSupervisorSpec extends SeekableStreamSupervisorSpec
  {
    private SeekableStreamSupervisor supervisor;
    private String id;

    public TesstSeekableStreamSupervisorSpec(SeekableStreamSupervisorIngestionSpec ingestionSchema,
                                             @Nullable Map<String, Object> context,
                                             Boolean suspended,
                                             TaskStorage taskStorage,
                                             TaskMaster taskMaster,
                                             IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
                                             SeekableStreamIndexTaskClientFactory indexTaskClientFactory,
                                             ObjectMapper mapper,
                                             ServiceEmitter emitter,
                                             DruidMonitorSchedulerConfig monitorSchedulerConfig,
                                             RowIngestionMetersFactory rowIngestionMetersFactory,
                                             SupervisorStateManagerConfig supervisorStateManagerConfig,
                                             SeekableStreamSupervisor supervisor,
                                             String id)
    {
      super(
              ingestionSchema,
              context,
              suspended,
              taskStorage,
              taskMaster,
              indexerMetadataStorageCoordinator,
              indexTaskClientFactory,
              mapper,
              emitter,
              monitorSchedulerConfig,
              rowIngestionMetersFactory,
              supervisorStateManagerConfig);

      this.supervisor = supervisor;
      this.id = id;
    }

    @Override
    public List<String> getDataSources()
    {
      return new ArrayList<>();
    }

    @Override
    public String getId()
    {
      return id;
    }

    @Override
    public Supervisor createSupervisor()
    {
      return supervisor;
    }

    @Override
    public String getType()
    {
      return null;
    }

    @Override
    public String getSource()
    {
      return null;
    }

    @Override
    protected SeekableStreamSupervisorSpec toggleSuspend(boolean suspend)
    {
      return null;
    }
  }

  private static SeekableStreamSupervisorTuningConfig getTuningConfig()
  {
    return new SeekableStreamSupervisorTuningConfig()
    {
      @Override
      public Integer getWorkerThreads()
      {
        return 1;
      }

      @Override
      public Integer getChatThreads()
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
        return new Period("PT1M").toStandardDuration();
      }

      @Override
      public Duration getShutdownTimeout()
      {
        return new Period("PT1S").toStandardDuration();
      }

      @Override
      public Duration getRepartitionTransitionDuration()
      {
        return new Period("PT2M").toStandardDuration();
      }

      @Override
      public Duration getOffsetFetchPeriod()
      {
        return new Period("PT5M").toStandardDuration();
      }

      @Override
      public SeekableStreamIndexTaskTuningConfig convertToTaskTuningConfig()
      {
        return new SeekableStreamIndexTaskTuningConfig(
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
        )
        {
          @Override
          public SeekableStreamIndexTaskTuningConfig withBasePersistDirectory(File dir)
          {
            return null;
          }

          @Override
          public String toString()
          {
            return null;
          }
        };
      }
    };
  }

  @Test
  public void testAutoScalerCreated()
  {
    HashMap<String, Object> dynamicAllocationTasksProperties = new HashMap<>();
    dynamicAllocationTasksProperties.put("enableDynamicAllocationTasks", true);
    dynamicAllocationTasksProperties.put("metricsCollectionIntervalMillis", 500);
    dynamicAllocationTasksProperties.put("metricsCollectionRangeMillis", 500);
    dynamicAllocationTasksProperties.put("scaleOutThreshold", 5000000);
    dynamicAllocationTasksProperties.put("triggerScaleOutThresholdFrequency", 0.3);
    dynamicAllocationTasksProperties.put("scaleInThreshold", 1000000);
    dynamicAllocationTasksProperties.put("triggerScaleInThresholdFrequency", 0.8);
    dynamicAllocationTasksProperties.put("dynamicCheckStartDelayMillis", 0);
    dynamicAllocationTasksProperties.put("dynamicCheckPeriod", 100);
    dynamicAllocationTasksProperties.put("taskCountMax", 8);
    dynamicAllocationTasksProperties.put("taskCountMin", 1);
    dynamicAllocationTasksProperties.put("scaleInStep", 1);
    dynamicAllocationTasksProperties.put("scaleOutStep", 2);
    dynamicAllocationTasksProperties.put("minTriggerDynamicFrequencyMillis", 1200000);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(seekableStreamSupervisorIOConfig.getDynamicAllocationTasksProperties()).andReturn(dynamicAllocationTasksProperties).anyTimes();
    EasyMock.replay(seekableStreamSupervisorIOConfig);

    EasyMock.expect(supervisor4.getSupervisorTaskInfos()).andReturn(new HashMap()).anyTimes();
    EasyMock.replay(supervisor4);

    TesstSeekableStreamSupervisorSpec spec = new TesstSeekableStreamSupervisorSpec(ingestionSchema,
            null,
            false,
            taskStorage,
            taskMaster,
            indexerMetadataStorageCoordinator,
            indexTaskClientFactory,
            mapper,
            emitter,
            monitorSchedulerConfig,
            rowIngestionMetersFactory,
            supervisorStateManagerConfig,
            supervisor4,
            "id1");
    SupervisorTaskAutoscaler autoscaler = spec.createAutoscaler(supervisor4);
    Assert.assertTrue(autoscaler instanceof DefaultAutoScaler);

    dynamicAllocationTasksProperties.put("enableDynamicAllocationTasks", false);
    SupervisorTaskAutoscaler autoscaler2 = spec.createAutoscaler(supervisor4);
    Assert.assertTrue(autoscaler2 instanceof DummyAutoScaler);

    dynamicAllocationTasksProperties.remove("enableDynamicAllocationTasks");
    SupervisorTaskAutoscaler autoscaler3 = spec.createAutoscaler(supervisor4);
    Assert.assertTrue(autoscaler3 instanceof DummyAutoScaler);

  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleOut() throws InterruptedException
  {

    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(1, true)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();
    DefaultAutoScaler autoScaler = new DefaultAutoScaler(supervisor, DATASOURCE, getScaleOutProperties(), spec);
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();
    int taskCountBeforeScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountBeforeScaleOut);
    Thread.sleep(1 * 1000);
    int taskCountAfterScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(2, taskCountAfterScaleOut);

    autoScaler.reset();
    autoScaler.stop();

  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleIn() throws InterruptedException
  {

    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(getIOConfig(2, false)).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();
    DefaultAutoScaler autoScaler = new DefaultAutoScaler(supervisor, DATASOURCE, getScaleInProperties(), spec);
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();
    int taskCountBeforeScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(2, taskCountBeforeScaleOut);
    Thread.sleep(1 * 1000);
    int taskCountAfterScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountAfterScaleOut);

    autoScaler.reset();
    autoScaler.stop();
  }

  @Test
  public void testSeekableStreamSupervisorSpecWithScaleDisable() throws InterruptedException
  {

    SeekableStreamSupervisorIOConfig seekableStreamSupervisorIOConfig = new SeekableStreamSupervisorIOConfig(
            "stream",
            new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false),
            1,
            1,
            new Period("PT1H"),
            new Period("P1D"),
            new Period("PT30S"),
            false,
            new Period("PT30M"),
            null,
            null, null, null
    ) {};
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();

    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(ingestionSchema.getIOConfig()).andReturn(this.seekableStreamSupervisorIOConfig).anyTimes();
    EasyMock.expect(ingestionSchema.getDataSchema()).andReturn(dataSchema).anyTimes();
    EasyMock.expect(ingestionSchema.getTuningConfig()).andReturn(seekableStreamSupervisorTuningConfig).anyTimes();
    EasyMock.replay(ingestionSchema);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor();
    DummyAutoScaler autoScaler = new DummyAutoScaler(supervisor, DATASOURCE);
    supervisor.start();
    autoScaler.start();
    supervisor.runInternal();
    int taskCountBeforeScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountBeforeScaleOut);
    Thread.sleep(1 * 1000);
    int taskCountAfterScaleOut = supervisor.getIoConfig().getTaskCount();
    Assert.assertEquals(1, taskCountAfterScaleOut);

    autoScaler.reset();
    autoScaler.stop();
  }

  private static DataSchema getDataSchema()
  {
    List<DimensionSchema> dimensions = new ArrayList<>();
    dimensions.add(StringDimensionSchema.create("dim1"));
    dimensions.add(StringDimensionSchema.create("dim2"));

    return new DataSchema(
            DATASOURCE,
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(
                    dimensions,
                    null,
                    null
            ),
            new AggregatorFactory[]{new CountAggregatorFactory("rows")},
            new UniformGranularitySpec(
                    Granularities.HOUR,
                    Granularities.NONE,
                    ImmutableList.of()
            ),
            null
    );
  }

  private static SeekableStreamSupervisorIOConfig getIOConfig(int taskCount, boolean scaleOut)
  {
    if (scaleOut) {
      return new SeekableStreamSupervisorIOConfig(
              "stream",
              new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false),
              1,
              taskCount,
              new Period("PT1H"),
              new Period("P1D"),
              new Period("PT30S"),
              false,
              new Period("PT30M"),
              null,
              null, getScaleOutProperties(), null
      ) {};
    } else {
      return new SeekableStreamSupervisorIOConfig(
              "stream",
              new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false),
              1,
              taskCount,
              new Period("PT1H"),
              new Period("P1D"),
              new Period("PT30S"),
              false,
              new Period("PT30M"),
              null,
              null, getScaleInProperties(), null
        ) {};
    }
  }

  private static Map<String, Object> getScaleOutProperties()
  {
    HashMap<String, Object> dynamicAllocationTasksProperties = new HashMap<>();
    dynamicAllocationTasksProperties.put("enableDynamicAllocationTasks", true);
    dynamicAllocationTasksProperties.put("metricsCollectionIntervalMillis", 500);
    dynamicAllocationTasksProperties.put("metricsCollectionRangeMillis", 500);
    dynamicAllocationTasksProperties.put("scaleOutThreshold", 0);
    dynamicAllocationTasksProperties.put("triggerScaleOutThresholdFrequency", 0.0);
    dynamicAllocationTasksProperties.put("scaleInThreshold", 1000000);
    dynamicAllocationTasksProperties.put("triggerScaleInThresholdFrequency", 0.8);
    dynamicAllocationTasksProperties.put("dynamicCheckStartDelayMillis", 0);
    dynamicAllocationTasksProperties.put("dynamicCheckPeriod", 100);
    dynamicAllocationTasksProperties.put("taskCountMax", 2);
    dynamicAllocationTasksProperties.put("taskCountMin", 1);
    dynamicAllocationTasksProperties.put("scaleInStep", 1);
    dynamicAllocationTasksProperties.put("scaleOutStep", 2);
    dynamicAllocationTasksProperties.put("minTriggerDynamicFrequencyMillis", 1200000);
    return dynamicAllocationTasksProperties;
  }

  private static Map<String, Object> getScaleInProperties()
  {
    HashMap<String, Object> dynamicAllocationTasksProperties = new HashMap<>();
    dynamicAllocationTasksProperties.put("enableDynamicAllocationTasks", true);
    dynamicAllocationTasksProperties.put("metricsCollectionIntervalMillis", 500);
    dynamicAllocationTasksProperties.put("metricsCollectionRangeMillis", 500);
    dynamicAllocationTasksProperties.put("scaleOutThreshold", 8000000);
    dynamicAllocationTasksProperties.put("triggerScaleOutThresholdFrequency", 0.3);
    dynamicAllocationTasksProperties.put("scaleInThreshold", 0);
    dynamicAllocationTasksProperties.put("triggerScaleInThresholdFrequency", 0.0);
    dynamicAllocationTasksProperties.put("dynamicCheckStartDelayMillis", 0);
    dynamicAllocationTasksProperties.put("dynamicCheckPeriod", 100);
    dynamicAllocationTasksProperties.put("taskCountMax", 2);
    dynamicAllocationTasksProperties.put("taskCountMin", 1);
    dynamicAllocationTasksProperties.put("scaleInStep", 1);
    dynamicAllocationTasksProperties.put("scaleOutStep", 2);
    dynamicAllocationTasksProperties.put("minTriggerDynamicFrequencyMillis", 1200000);
    return dynamicAllocationTasksProperties;
  }

}

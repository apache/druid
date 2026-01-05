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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClientFactory;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;

public class SeekableStreamSupervisorScaleDuringTaskRolloverTest extends EasyMockSupport
{
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private static final String STREAM = "stream";
  private static final String DATASOURCE = "testDS";
  private static final String SUPERVISOR = "supervisor";
  private static final int DEFAULT_TASK_COUNT = 10;

  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private ServiceEmitter emitter;
  private RowIngestionMetersFactory rowIngestionMetersFactory;
  private SeekableStreamIndexTaskClientFactory taskClientFactory;
  private SeekableStreamSupervisorSpec spec;
  private SupervisorStateManagerConfig supervisorConfig;

  @Before
  public void setUp()
  {
    taskStorage = EasyMock.mock(TaskStorage.class);
    taskMaster = EasyMock.mock(TaskMaster.class);
    indexerMetadataStorageCoordinator = EasyMock.mock(IndexerMetadataStorageCoordinator.class);
    emitter = new StubServiceEmitter();
    rowIngestionMetersFactory = EasyMock.mock(RowIngestionMetersFactory.class);
    taskClientFactory = EasyMock.mock(SeekableStreamIndexTaskClientFactory.class);
    spec = EasyMock.mock(SeekableStreamSupervisorSpec.class);
    supervisorConfig = new SupervisorStateManagerConfig();

    // Common taskMaster setup - used by all tests
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);
  }

  @Test
  public void test_maybeScaleDuringTaskRollover_noAutoScaler_doesNotScale()
  {
    // Given
    setupSpecExpectations(getIOConfigWithoutAutoScaler(5));
    EasyMock.expect(spec.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(spec);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(10);
    supervisor.start();

    int beforeTaskCount = supervisor.getIoConfig().getTaskCount();

    // When
    supervisor.maybeScaleDuringTaskRollover();

    // Then
    Assert.assertEquals(
        "Task count should not change when taskAutoScaler is null",
        beforeTaskCount,
        (int) supervisor.getIoConfig().getTaskCount()
    );
  }

  @Test
  public void test_maybeScaleDuringTaskRollover_rolloverCountNonPositive_doesNotScale()
  {
    // Given
    setupSpecExpectations(getIOConfigWithCostBasedAutoScaler());
    EasyMock.expect(spec.createAutoscaler(EasyMock.anyObject()))
            .andReturn(createMockAutoScaler(-1))
            .anyTimes();
    EasyMock.replay(spec);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(100);
    supervisor.start();
    supervisor.createAutoscaler(spec);

    int beforeTaskCount = supervisor.getIoConfig().getTaskCount();

    // When
    supervisor.maybeScaleDuringTaskRollover();

    // Then
    Assert.assertEquals(
        "Task count should not change when rolloverTaskCount <= 0",
        beforeTaskCount,
        (int) supervisor.getIoConfig().getTaskCount()
    );
  }

  @Test
  public void test_maybeScaleDuringTaskRollover_rolloverCountPositive_performsScaling()
  {
    // Given
    final int targetTaskCount = 5;

    setupSpecExpectations(getIOConfigWithCostBasedAutoScaler());
    EasyMock.expect(spec.createAutoscaler(EasyMock.anyObject()))
            .andReturn(createMockAutoScaler(targetTaskCount))
            .anyTimes();
    EasyMock.replay(spec);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(100);
    supervisor.start();
    supervisor.createAutoscaler(spec);

    Assert.assertEquals(1, (int) supervisor.getIoConfig().getTaskCount());

    // When
    supervisor.maybeScaleDuringTaskRollover();

    // Then
    Assert.assertEquals(
        "Task count should be updated to " + targetTaskCount + " when rolloverTaskCount > 0",
        targetTaskCount,
        (int) supervisor.getIoConfig().getTaskCount()
    );
  }

  @Test
  public void test_maybeScaleDuringTaskRollover_rolloverCountZero_doesNotScale()
  {
    // Given
    setupSpecExpectations(getIOConfigWithCostBasedAutoScaler());
    EasyMock.expect(spec.createAutoscaler(EasyMock.anyObject()))
            .andReturn(createMockAutoScaler(0))
            .anyTimes();
    EasyMock.replay(spec);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(100);
    supervisor.start();
    supervisor.createAutoscaler(spec);

    int beforeTaskCount = supervisor.getIoConfig().getTaskCount();

    // When
    supervisor.maybeScaleDuringTaskRollover();

    // Then
    Assert.assertEquals(
        "Task count should not change when rolloverTaskCount is 0",
        beforeTaskCount,
        (int) supervisor.getIoConfig().getTaskCount()
    );
  }

  // Helper methods for test setup

  /**
   * Sets up common spec expectations. Call EasyMock.replay(spec) after this and any additional expectations.
   */
  private void setupSpecExpectations(SeekableStreamSupervisorIOConfig ioConfig)
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();
    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema()).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
  }

  /**
   * Creates a mock autoscaler that returns the specified rollover count.
   */
  private static SupervisorTaskAutoScaler createMockAutoScaler(int rolloverCount)
  {
    return new SupervisorTaskAutoScaler()
    {
      @Override
      public void start()
      {
      }

      @Override
      public void stop()
      {
      }

      @Override
      public void reset()
      {
      }

      @Override
      public int computeTaskCountForRollover()
      {
        return rolloverCount;
      }
    };
  }

  // Helper methods for config creation

  private static CostBasedAutoScalerConfig getCostBasedAutoScalerConfig()
  {
    return CostBasedAutoScalerConfig.builder()
                                    .taskCountMax(100)
                                    .taskCountMin(1)
                                    .enableTaskAutoScaler(true)
                                    .lagWeight(0.3)
                                    .idleWeight(0.7)
                                    .scaleActionPeriodMillis(100)
                                    .build();
  }

  private SeekableStreamSupervisorIOConfig getIOConfigWithCostBasedAutoScaler()
  {
    return createIOConfig(DEFAULT_TASK_COUNT, getCostBasedAutoScalerConfig());
  }

  private SeekableStreamSupervisorIOConfig getIOConfigWithoutAutoScaler(int taskCount)
  {
    return createIOConfig(taskCount, null);
  }

  private SeekableStreamSupervisorIOConfig createIOConfig(int taskCount, CostBasedAutoScalerConfig autoScalerConfig)
  {
    return new SeekableStreamSupervisorIOConfig(
        STREAM,
        new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false, false, false),
        1,
        taskCount,
        new Period("PT1H"),
        new Period("P1D"),
        new Period("PT30S"),
        false,
        new Period("PT30M"),
        null,
        null,
        autoScalerConfig,
        LagAggregator.DEFAULT,
        null,
        null,
        null
    )
    {
    };
  }

  private static DataSchema getDataSchema()
  {
    List<DimensionSchema> dimensions = new ArrayList<>();
    dimensions.add(StringDimensionSchema.create("dim1"));
    dimensions.add(StringDimensionSchema.create("dim2"));

    return DataSchema.builder()
                     .withDataSource(DATASOURCE)
                     .withTimestamp(new TimestampSpec("timestamp", "iso", null))
                     .withDimensions(dimensions)
                     .withAggregators(new CountAggregatorFactory("rows"))
                     .withGranularity(
                         new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.NONE,
                             ImmutableList.of()
                         )
                     )
                     .build();
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
        return new SeekableStreamIndexTaskTuningConfig(null, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null, null, null,
            null, null, null
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

  // Inner test classes

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
      return new SeekableStreamIndexTaskIOConfig<>(
          groupId,
          baseSequenceName,
          new SeekableStreamStartSequenceNumbers<>(STREAM, startPartitions, exclusiveStartSequenceNumberPartitions),
          new SeekableStreamEndSequenceNumbers<>(STREAM, endPartitions),
          true,
          minimumMessageTime,
          maximumMessageTime,
          ioConfig.getInputFormat(),
          ioConfig.getTaskDuration().getStandardMinutes()
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
    protected boolean doesTaskMatchSupervisor(Task task)
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
      return new OrderedSequenceNumber<>(seq, isExclusive)
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
      return new SeekableStreamSupervisorReportPayload<>(SUPERVISOR, DATASOURCE, STREAM, 1, 1, 1L,
          null, null, null, null, null, null,
          false, true, null, null, null
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
    private final int partitionNumbers;

    public TestSeekableStreamSupervisor(int partitionNumbers)
    {
      this.partitionNumbers = partitionNumbers;
    }

    @Override
    protected void scheduleReporting(ScheduledExecutorService reportingExec)
    {
    }

    @Override
    public LagStats computeLagStats()
    {
      return new LagStats(0, 0, 0);
    }

    @Override
    public int getPartitionCount()
    {
      return partitionNumbers;
    }
  }
}

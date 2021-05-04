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

package org.apache.druid.indexing.pulsar.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.pulsar.PulsarRecordEntity;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.pulsar.PulsarDataSourceMetadata;
import org.apache.druid.indexing.pulsar.PulsarIndexTaskClientFactory;
import org.apache.druid.indexing.pulsar.PulsarIndexTaskIOConfig;
import org.apache.druid.indexing.pulsar.PulsarIndexTaskTuningConfig;
import org.apache.druid.indexing.pulsar.PulsarSequenceNumber;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.pulsar.PulsarIndexTask;
import org.apache.druid.indexing.pulsar.PulsarRecordSupplier;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorReportPayload;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

//TODO:check all lag logic

public class PulsarSupervisor extends SeekableStreamSupervisor<Integer, String, PulsarRecordEntity>
{
  public static final TypeReference<TreeMap<Integer, Map<Integer, String>>> CHECKPOINTS_TYPE_REF =
      new TypeReference<TreeMap<Integer, Map<Integer, String>>>()
      {
      };

  private static final EmittingLogger log = new EmittingLogger(PulsarSupervisor.class);
  private static final String NOT_SET = "-1:-1:-1";
  private static final String END_OF_PARTITION = "9223372036854775807:9223372036854775807:-1";

  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private volatile Map<Integer, String> latestSequenceFromStream;


  private final PulsarSupervisorSpec spec;

  public PulsarSupervisor(
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final PulsarIndexTaskClientFactory taskClientFactory,
      final ObjectMapper mapper,
      final PulsarSupervisorSpec spec,
      final RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    super(
        StringUtils.format("PulsarSupervisor-%s", spec.getDataSchema().getDataSource()),
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        mapper,
        spec,
        rowIngestionMetersFactory,
        false
    );

    this.spec = spec;
    this.emitter = spec.getEmitter();
    this.monitorSchedulerConfig = spec.getMonitorSchedulerConfig();
  }


  @Override
  protected RecordSupplier<Integer, String, PulsarRecordEntity> setupRecordSupplier()
  {
    return new PulsarRecordSupplier(spec.getIoConfig().getConsumerProperties(), sortingMapper);
  }

  @Override
  protected int getTaskGroupIdForPartition(Integer partitionId)
  {
    return partitionId % spec.getIoConfig().getTaskCount();
  }

  @Override
  protected boolean checkSourceMetadataMatch(DataSourceMetadata metadata)
  {
    return metadata instanceof PulsarDataSourceMetadata;
  }

  @Override
  protected boolean doesTaskTypeMatchSupervisor(Task task)
  {
    return task instanceof PulsarIndexTask;
  }

  @Override
  protected SeekableStreamSupervisorReportPayload<Integer, String> createReportPayload(
      int numPartitions,
      boolean includeOffsets
  )
  {
    PulsarSupervisorIOConfig ioConfig = spec.getIoConfig();
    Map<Integer, Long> partitionLag = getRecordLagPerPartition(getHighestCurrentOffsets());
    return new PulsarSupervisorReportPayload(
        spec.getDataSchema().getDataSource(),
        ioConfig.getTopic(),
        numPartitions,
        ioConfig.getReplicas(),
        ioConfig.getTaskDuration().getMillis() / 1000,
        includeOffsets ? latestSequenceFromStream : null,
        includeOffsets ? partitionLag : null,
        includeOffsets ? partitionLag.values().stream().mapToLong(x -> Math.max(x, 0)).sum() : null,
        includeOffsets ? sequenceLastUpdated : null,
        spec.isSuspended(),
        stateManager.isHealthy(),
        stateManager.getSupervisorState().getBasicState(),
        stateManager.getSupervisorState(),
        stateManager.getExceptionEvents()
    );
  }


  @Override
  protected SeekableStreamIndexTaskIOConfig createTaskIoConfig(
      int groupId,
      Map<Integer, String> startPartitions,
      Map<Integer, String> endPartitions,
      String baseSequenceName,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      Set<Integer> exclusiveStartSequenceNumberPartitions,
      SeekableStreamSupervisorIOConfig ioConfig
  )
  {
    PulsarSupervisorIOConfig pulsarIoConfig = (PulsarSupervisorIOConfig) ioConfig;
    return new PulsarIndexTaskIOConfig(
        groupId,
        baseSequenceName,
        new SeekableStreamStartSequenceNumbers<>(pulsarIoConfig.getTopic(), startPartitions, Collections.emptySet()),
        new SeekableStreamEndSequenceNumbers<>(pulsarIoConfig.getTopic(), endPartitions),
        pulsarIoConfig.getConsumerProperties(),
        pulsarIoConfig.getPollTimeout(),
        true,
        minimumMessageTime,
        maximumMessageTime,
        ioConfig.getInputFormat()
    );
  }

  @Override
  protected List<SeekableStreamIndexTask<Integer, String, PulsarRecordEntity>> createIndexTasks(
      int replicas,
      String baseSequenceName,
      ObjectMapper sortingMapper,
      TreeMap<Integer, Map<Integer, String>> sequenceOffsets,
      SeekableStreamIndexTaskIOConfig taskIoConfig,
      SeekableStreamIndexTaskTuningConfig taskTuningConfig,
      RowIngestionMetersFactory rowIngestionMetersFactory
  ) throws JsonProcessingException
  {
    final String checkpoints = sortingMapper.writerFor(CHECKPOINTS_TYPE_REF).writeValueAsString(sequenceOffsets);
    final Map<String, Object> context = createBaseTaskContexts();
    context.put(CHECKPOINTS_CTX_KEY, checkpoints);

    List<SeekableStreamIndexTask<Integer, String, PulsarRecordEntity>> taskList = new ArrayList<>();
    for (int i = 0; i < replicas; i++) {
      String taskId = IdUtils.getRandomIdWithPrefix(baseSequenceName);
      taskList.add(new PulsarIndexTask(
          taskId,
          new TaskResource(baseSequenceName, 1),
          spec.getDataSchema(),
          (PulsarIndexTaskTuningConfig) taskTuningConfig,
          (PulsarIndexTaskIOConfig) taskIoConfig,
          context,
          sortingMapper
      ));
    }
    return taskList;
  }

  @Override
  protected Map<Integer, Long> getPartitionRecordLag()
  {
    return null;
  }

  @Nullable
  @Override
  protected Map<Integer, Long> getPartitionTimeLag()
  {
    // time lag not currently support with pulsar
    return null;
  }

  @Override
  protected Map<Integer, Long> getRecordLagPerPartition(Map<Integer, String> currentOffsets)
  {
    return ImmutableMap.of();
  }


  @Override
  protected Map<Integer, Long> getTimeLagPerPartition(Map<Integer, String> currentOffsets)
  {
    return null;
  }

  @Override
  protected PulsarDataSourceMetadata createDataSourceMetaDataForReset(String topic, Map<Integer, String> map)
  {
    return new PulsarDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, map));
  }

  @Override
  protected OrderedSequenceNumber<String> makeSequenceNumber(String seq, boolean isExclusive)
  {
    return PulsarSequenceNumber.of(seq);
  }

  @Override
  protected String getNotSetMarker()
  {
    return NOT_SET;
  }

  @Override
  protected String getEndOfPartitionMarker()
  {
    return END_OF_PARTITION;
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

  @Override
  public LagStats computeLagStats()
  {
    throw new UnsupportedOperationException("Compute Lag Stats is not supported in KinesisSupervisor yet.");
  }

  @Override
  protected void updatePartitionLagFromStream()
  {
    getRecordSupplierLock().lock();
    try {
      Set<Integer> partitionIds;
      try {
        partitionIds = recordSupplier.getPartitionIds(getIoConfig().getStream());
      }
      catch (Exception e) {
        log.warn("Could not fetch partitions for topic/stream [%s]", getIoConfig().getStream());
        throw new StreamException(e);
      }

      Set<StreamPartition<Integer>> partitions = partitionIds
          .stream()
          .map(e -> new StreamPartition<>(getIoConfig().getStream(), e))
          .collect(Collectors.toSet());

      recordSupplier.seekToLatest(partitions);

      // this method isn't actually computing the lag, just fetching the latests offsets from the stream. This is
      // because we currently only have record lag for pulsar, which can be lazily computed by subtracting the highest
      // task offsets from the latest offsets from the stream when it is needed
      latestSequenceFromStream =
          partitions.stream().collect(Collectors.toMap(StreamPartition::getPartitionId, recordSupplier::getPosition));
    } catch (InterruptedException e) {
      throw new StreamException(e);
    } finally {
      getRecordSupplierLock().unlock();
    }
  }

  @Override
  protected String baseTaskName()
  {
    return "index_pulsar";
  }

  @Override
  @VisibleForTesting
  public PulsarSupervisorIOConfig getIoConfig()
  {
    return spec.getIoConfig();
  }

  @VisibleForTesting
  public PulsarSupervisorTuningConfig getTuningConfig()
  {
    return spec.getTuningConfig();
  }
}

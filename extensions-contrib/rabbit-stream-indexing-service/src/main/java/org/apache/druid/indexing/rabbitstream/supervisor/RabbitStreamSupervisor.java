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

package org.apache.druid.indexing.rabbitstream.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.rabbitstream.RabbitSequenceNumber;
import org.apache.druid.indexing.rabbitstream.RabbitStreamDataSourceMetadata;
import org.apache.druid.indexing.rabbitstream.RabbitStreamIndexTask;
import org.apache.druid.indexing.rabbitstream.RabbitStreamIndexTaskClientFactory;
import org.apache.druid.indexing.rabbitstream.RabbitStreamIndexTaskIOConfig;
import org.apache.druid.indexing.rabbitstream.RabbitStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.rabbitstream.RabbitStreamRecordSupplier;
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
import org.apache.druid.java.util.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Supervisor responsible for managing the RabbitStreamIndexTasks for a single
 * dataSource. At a high level, the class accepts a
 * {@link RabbitStreamSupervisorSpec} which includes the rabbit super stream and
 * configuration as well as an ingestion spec which will be used to generate the
 * indexing tasks. The run loop periodically refreshes its view of the super stream's
 * partitions and the list of running indexing tasks and ensures that
 * all partitions are being read from and that there are enough tasks to satisfy
 * the desired number of replicas. As tasks complete, new tasks are queued to
 * process the next range of rabbit stream offsets.
 */
public class RabbitStreamSupervisor extends SeekableStreamSupervisor<String, Long, ByteEntity>
{
  public static final TypeReference<TreeMap<Integer, Map<String, Long>>> CHECKPOINTS_TYPE_REF = new TypeReference<TreeMap<Integer, Map<String, Long>>>() {
  };

  private static final EmittingLogger log = new EmittingLogger(RabbitStreamSupervisor.class);
  private static final Long NOT_SET = -1L;
  private static final Long END_OF_PARTITION = Long.MAX_VALUE;

  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private volatile Map<String, Long> latestSequenceFromStream;

  private final RabbitStreamSupervisorSpec spec;

  public RabbitStreamSupervisor(
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final RabbitStreamIndexTaskClientFactory taskClientFactory,
      final ObjectMapper mapper,
      final RabbitStreamSupervisorSpec spec,
      final RowIngestionMetersFactory rowIngestionMetersFactory)
  {
    super(
        StringUtils.format("RabbitSupervisor-%s", spec.getDataSchema().getDataSource()),
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        mapper,
        spec,
        rowIngestionMetersFactory,
        false);

    this.spec = spec;
    this.emitter = spec.getEmitter();
    this.monitorSchedulerConfig = spec.getMonitorSchedulerConfig();
  }

  @Override
  protected RecordSupplier<String, Long, ByteEntity> setupRecordSupplier()
  {
    RabbitStreamIndexTaskTuningConfig taskTuningConfig = spec.getTuningConfig();

    return new RabbitStreamRecordSupplier(
      spec.getIoConfig().getConsumerProperties(),
      sortingMapper,
      spec.getIoConfig().getUri(),
      taskTuningConfig.getRecordBufferSizeOrDefault(Runtime.getRuntime().maxMemory()),
      taskTuningConfig.getRecordBufferOfferTimeout(),
      taskTuningConfig.getMaxRecordsPerPollOrDefault()      
      );
  }

  @Override
  protected int getTaskGroupIdForPartition(String partitionId)
  {
    return partitionId.hashCode() % spec.getIoConfig().getTaskCount();
  }

  @Override
  protected boolean checkSourceMetadataMatch(DataSourceMetadata metadata)
  {
    return metadata instanceof RabbitStreamDataSourceMetadata;
  }

  @Override
  protected boolean doesTaskTypeMatchSupervisor(Task task)
  {
    return task instanceof RabbitStreamIndexTask;
  }

  @Override
  protected SeekableStreamSupervisorReportPayload<String, Long> createReportPayload(
      int numPartitions,
      boolean includeOffsets)
  {
    RabbitStreamSupervisorIOConfig ioConfig = spec.getIoConfig();
    Map<String, Long> partitionLag = getRecordLagPerPartitionInLatestSequences(getHighestCurrentOffsets());
    return new RabbitStreamSupervisorReportPayload(
        spec.getDataSchema().getDataSource(),
        ioConfig.getStream(),
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
        stateManager.getExceptionEvents());
  }

  @Override
  protected SeekableStreamIndexTaskIOConfig createTaskIoConfig(
      int groupId,
      Map<String, Long> startPartitions,
      Map<String, Long> endPartitions,
      String baseSequenceName,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      Set<String> exclusiveStartSequenceNumberPartitions,
      SeekableStreamSupervisorIOConfig ioConfig)
  {
    RabbitStreamSupervisorIOConfig rabbitConfig = (RabbitStreamSupervisorIOConfig) ioConfig;
    return new RabbitStreamIndexTaskIOConfig(
        groupId,
        baseSequenceName,
        new SeekableStreamStartSequenceNumbers<>(ioConfig.getStream(), startPartitions, Collections.emptySet()),
        new SeekableStreamEndSequenceNumbers<>(ioConfig.getStream(), endPartitions),
        rabbitConfig.getConsumerProperties(),
        rabbitConfig.getPollTimeout(),
        true,
        minimumMessageTime,
        maximumMessageTime,
        ioConfig.getInputFormat(),
        rabbitConfig.getUri());
  }

  @Override
  protected List<SeekableStreamIndexTask<String, Long, ByteEntity>> createIndexTasks(
      int replicas,
      String baseSequenceName,
      ObjectMapper sortingMapper,
      TreeMap<Integer, Map<String, Long>> sequenceOffsets,
      SeekableStreamIndexTaskIOConfig taskIoConfig,
      SeekableStreamIndexTaskTuningConfig taskTuningConfig,
      RowIngestionMetersFactory rowIngestionMetersFactory) throws JsonProcessingException
  {
    final String checkpoints = sortingMapper.writerFor(CHECKPOINTS_TYPE_REF).writeValueAsString(sequenceOffsets);
    final Map<String, Object> context = createBaseTaskContexts();
    context.put(CHECKPOINTS_CTX_KEY, checkpoints);

    List<SeekableStreamIndexTask<String, Long, ByteEntity>> taskList = new ArrayList<>();
    for (int i = 0; i < replicas; i++) {
      String taskId = IdUtils.getRandomIdWithPrefix(baseSequenceName);
      taskList.add(new RabbitStreamIndexTask(
          taskId,
          new TaskResource(baseSequenceName, 1),
          spec.getDataSchema(),
          (RabbitStreamIndexTaskTuningConfig) taskTuningConfig,
          (RabbitStreamIndexTaskIOConfig) taskIoConfig,
          context,
          sortingMapper));
    }
    return taskList;
  }

  @Override
  protected Map<String, Long> getPartitionRecordLag()
  {
    Map<String, Long> highestCurrentOffsets = getHighestCurrentOffsets();

    if (latestSequenceFromStream == null) {
      return null;
    }

    if (!latestSequenceFromStream.keySet().equals(highestCurrentOffsets.keySet())) {
      log.warn(
          "Lag metric: rabbit partitions %s do not match task partitions %s",
          latestSequenceFromStream.keySet(),
          highestCurrentOffsets.keySet());
    }

    return getRecordLagPerPartitionInLatestSequences(highestCurrentOffsets);
  }

  @Nullable
  @Override
  protected Map<String, Long> getPartitionTimeLag()
  {
    // time lag not currently support with rabbit
    return null;
  }

  // suppress use of CollectionUtils.mapValues() since the valueMapper function
  // is
  // dependent on map key here
  @SuppressWarnings("SSBasedInspection")
  // Used while calculating cummulative lag for entire stream
  private Map<String, Long> getRecordLagPerPartitionInLatestSequences(Map<String, Long> currentOffsets)
  {
    if (latestSequenceFromStream == null) {
      return Collections.emptyMap();
    }

    return latestSequenceFromStream
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Entry::getKey,
                e -> e.getValue() != null
                    ? e.getValue() + 1 - Optional.ofNullable(currentOffsets.get(e.getKey())).orElse(0L)
                    : 0));
  }

  @Override
  // suppress use of CollectionUtils.mapValues() since the valueMapper function
  // is
  // dependent on map key here
  @SuppressWarnings("SSBasedInspection")
  // Used while generating Supervisor lag reports per task
  protected Map<String, Long> getRecordLagPerPartition(Map<String, Long> currentOffsets)
  {
    if (latestSequenceFromStream == null || currentOffsets == null) {
      return Collections.emptyMap();
    }

    return currentOffsets
        .entrySet()
        .stream()
        .filter(e -> latestSequenceFromStream.get(e.getKey()) != null)
        .collect(
            Collectors.toMap(
                Entry::getKey,
                e -> e.getValue() != null
                    ? latestSequenceFromStream.get(e.getKey()) + 1 - e.getValue()
                    : 0));
  }

  @Override
  protected Map<String, Long> getTimeLagPerPartition(Map<String, Long> currentOffsets)
  {
    return null;
  }

  @Override
  protected RabbitStreamDataSourceMetadata createDataSourceMetaDataForReset(String topic, Map<String, Long> map)
  {
    return new RabbitStreamDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, map));
  }

  @Override
  protected OrderedSequenceNumber<Long> makeSequenceNumber(Long seq, boolean isExclusive)
  {
    return RabbitSequenceNumber.of(seq);
  }

  @Override
  protected Long getNotSetMarker()
  {
    return NOT_SET;
  }

  @Override
  protected Long getEndOfPartitionMarker()
  {
    return END_OF_PARTITION;
  }

  @Override
  protected boolean isEndOfShard(Long seqNum)
  {
    return false;
  }

  @Override
  protected boolean isShardExpirationMarker(Long seqNum)
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
    Map<String, Long> partitionRecordLag = getPartitionRecordLag();
    if (partitionRecordLag == null) {
      return new LagStats(0, 0, 0);
    }

    return computeLags(partitionRecordLag);
  }

  @Override
  protected void updatePartitionLagFromStream()
  {
    getRecordSupplierLock().lock();

    Set<String> partitionIds;
    try {
      partitionIds = recordSupplier.getPartitionIds(getIoConfig().getStream());
    }
    catch (Exception e) {
      log.warn("Could not fetch partitions for topic/stream [%s]", getIoConfig().getStream());
      getRecordSupplierLock().unlock();
      throw new StreamException(e);
    }

    Set<StreamPartition<String>> partitions = partitionIds
        .stream()
        .map(e -> new StreamPartition<>(getIoConfig().getStream(), e))
        .collect(Collectors.toSet());

    latestSequenceFromStream = partitions.stream()
        .collect(Collectors.toMap(StreamPartition::getPartitionId, recordSupplier::getLatestSequenceNumber));

    getRecordSupplierLock().unlock();

  }

  @Override
  protected Map<String, Long> getLatestSequencesFromStream()
  {
    return latestSequenceFromStream != null ? latestSequenceFromStream : new HashMap<>();
  }

  @Override
  protected String baseTaskName()
  {
    return "index_rabbit";
  }

  @Override
  @VisibleForTesting
  public RabbitStreamSupervisorIOConfig getIoConfig()
  {
    return spec.getIoConfig();
  }

  @VisibleForTesting
  public RabbitStreamSupervisorTuningConfig getTuningConfig()
  {
    return spec.getTuningConfig();
  }
}

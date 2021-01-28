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

package org.apache.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.kafka.KafkaDataSourceMetadata;
import org.apache.druid.indexing.kafka.KafkaIndexTask;
import org.apache.druid.indexing.kafka.KafkaIndexTaskClientFactory;
import org.apache.druid.indexing.kafka.KafkaIndexTaskIOConfig;
import org.apache.druid.indexing.kafka.KafkaIndexTaskTuningConfig;
import org.apache.druid.indexing.kafka.KafkaRecordSupplier;
import org.apache.druid.indexing.kafka.KafkaSequenceNumber;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
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
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Supervisor responsible for managing the KafkaIndexTasks for a single dataSource. At a high level, the class accepts a
 * {@link KafkaSupervisorSpec} which includes the Kafka topic and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the Kafka topic's partitions
 * and the list of running indexing tasks and ensures that all partitions are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * Kafka offsets.
 */
public class KafkaSupervisor extends SeekableStreamSupervisor<Integer, Long, KafkaRecordEntity>
{
  public static final TypeReference<TreeMap<Integer, Map<Integer, Long>>> CHECKPOINTS_TYPE_REF =
      new TypeReference<TreeMap<Integer, Map<Integer, Long>>>()
      {
      };

  private static final EmittingLogger log = new EmittingLogger(KafkaSupervisor.class);
  private static final Long NOT_SET = -1L;
  private static final Long END_OF_PARTITION = Long.MAX_VALUE;

  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private volatile Map<Integer, Long> latestSequenceFromStream;


  private final KafkaSupervisorSpec spec;

  public KafkaSupervisor(
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final KafkaIndexTaskClientFactory taskClientFactory,
      final ObjectMapper mapper,
      final KafkaSupervisorSpec spec,
      final RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    super(
        StringUtils.format("KafkaSupervisor-%s", spec.getDataSchema().getDataSource()),
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
  protected RecordSupplier<Integer, Long, KafkaRecordEntity> setupRecordSupplier()
  {
    return new KafkaRecordSupplier(spec.getIoConfig().getConsumerProperties(), sortingMapper);
  }

  @Override
  protected int getTaskGroupIdForPartition(Integer partitionId)
  {
    return partitionId % spec.getIoConfig().getTaskCount();
  }

  @Override
  protected boolean checkSourceMetadataMatch(DataSourceMetadata metadata)
  {
    return metadata instanceof KafkaDataSourceMetadata;
  }

  @Override
  protected boolean doesTaskTypeMatchSupervisor(Task task)
  {
    return task instanceof KafkaIndexTask;
  }

  @Override
  protected SeekableStreamSupervisorReportPayload<Integer, Long> createReportPayload(
      int numPartitions,
      boolean includeOffsets
  )
  {
    KafkaSupervisorIOConfig ioConfig = spec.getIoConfig();
    Map<Integer, Long> partitionLag = getRecordLagPerPartition(getHighestCurrentOffsets());
    return new KafkaSupervisorReportPayload(
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
      Map<Integer, Long> startPartitions,
      Map<Integer, Long> endPartitions,
      String baseSequenceName,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      Set<Integer> exclusiveStartSequenceNumberPartitions,
      SeekableStreamSupervisorIOConfig ioConfig
  )
  {
    KafkaSupervisorIOConfig kafkaIoConfig = (KafkaSupervisorIOConfig) ioConfig;
    return new KafkaIndexTaskIOConfig(
        groupId,
        baseSequenceName,
        new SeekableStreamStartSequenceNumbers<>(kafkaIoConfig.getTopic(), startPartitions, Collections.emptySet()),
        new SeekableStreamEndSequenceNumbers<>(kafkaIoConfig.getTopic(), endPartitions),
        kafkaIoConfig.getConsumerProperties(),
        kafkaIoConfig.getPollTimeout(),
        true,
        minimumMessageTime,
        maximumMessageTime,
        ioConfig.getInputFormat()
    );
  }

  @Override
  protected List<SeekableStreamIndexTask<Integer, Long, KafkaRecordEntity>> createIndexTasks(
      int replicas,
      String baseSequenceName,
      ObjectMapper sortingMapper,
      TreeMap<Integer, Map<Integer, Long>> sequenceOffsets,
      SeekableStreamIndexTaskIOConfig taskIoConfig,
      SeekableStreamIndexTaskTuningConfig taskTuningConfig,
      RowIngestionMetersFactory rowIngestionMetersFactory
  ) throws JsonProcessingException
  {
    final String checkpoints = sortingMapper.writerFor(CHECKPOINTS_TYPE_REF).writeValueAsString(sequenceOffsets);
    final Map<String, Object> context = createBaseTaskContexts();
    context.put(CHECKPOINTS_CTX_KEY, checkpoints);
    // Kafka index task always uses incremental handoff since 0.16.0.
    // The below is for the compatibility when you want to downgrade your cluster to something earlier than 0.16.0.
    // Kafka index task will pick up LegacyKafkaIndexTaskRunner without the below configuration.
    context.put("IS_INCREMENTAL_HANDOFF_SUPPORTED", true);

    List<SeekableStreamIndexTask<Integer, Long, KafkaRecordEntity>> taskList = new ArrayList<>();
    for (int i = 0; i < replicas; i++) {
      String taskId = IdUtils.getRandomIdWithPrefix(baseSequenceName);
      taskList.add(new KafkaIndexTask(
          taskId,
          new TaskResource(baseSequenceName, 1),
          spec.getDataSchema(),
          (KafkaIndexTaskTuningConfig) taskTuningConfig,
          (KafkaIndexTaskIOConfig) taskIoConfig,
          context,
          sortingMapper
      ));
    }
    return taskList;
  }

  @Override
  protected Map<Integer, Long> getPartitionRecordLag()
  {
    Map<Integer, Long> highestCurrentOffsets = getHighestCurrentOffsets();

    if (latestSequenceFromStream == null) {
      return null;
    }

    if (!latestSequenceFromStream.keySet().equals(highestCurrentOffsets.keySet())) {
      log.warn(
          "Lag metric: Kafka partitions %s do not match task partitions %s",
          latestSequenceFromStream.keySet(),
          highestCurrentOffsets.keySet()
      );
    }

    return getRecordLagPerPartition(highestCurrentOffsets);
  }

  @Nullable
  @Override
  protected Map<Integer, Long> getPartitionTimeLag()
  {
    // time lag not currently support with kafka
    return null;
  }

  @Override
  // suppress use of CollectionUtils.mapValues() since the valueMapper function is dependent on map key here
  @SuppressWarnings("SSBasedInspection")
  protected Map<Integer, Long> getRecordLagPerPartition(Map<Integer, Long> currentOffsets)
  {
    return currentOffsets
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Entry::getKey,
                e -> latestSequenceFromStream != null
                     && latestSequenceFromStream.get(e.getKey()) != null
                     && e.getValue() != null
                     ? latestSequenceFromStream.get(e.getKey()) - e.getValue()
                     : Integer.MIN_VALUE
            )
        );
  }

  @Override
  protected Map<Integer, Long> getTimeLagPerPartition(Map<Integer, Long> currentOffsets)
  {
    return null;
  }

  @Override
  protected KafkaDataSourceMetadata createDataSourceMetaDataForReset(String topic, Map<Integer, Long> map)
  {
    return new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, map));
  }

  @Override
  protected OrderedSequenceNumber<Long> makeSequenceNumber(Long seq, boolean isExclusive)
  {
    return KafkaSequenceNumber.of(seq);
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
      // because we currently only have record lag for kafka, which can be lazily computed by subtracting the highest
      // task offsets from the latest offsets from the stream when it is needed
      latestSequenceFromStream =
          partitions.stream().collect(Collectors.toMap(StreamPartition::getPartitionId, recordSupplier::getPosition));
    }
    catch (InterruptedException e) {
      throw new StreamException(e);
    }
    finally {
      getRecordSupplierLock().unlock();
    }
  }

  @Override
  protected String baseTaskName()
  {
    return "index_kafka";
  }

  @Override
  @VisibleForTesting
  public KafkaSupervisorIOConfig getIoConfig()
  {
    return spec.getIoConfig();
  }

  @VisibleForTesting
  public KafkaSupervisorTuningConfig getTuningConfig()
  {
    return spec.getTuningConfig();
  }
}

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

package org.apache.druid.indexing.kinesis.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.kinesis.KinesisDataSourceMetadata;
import org.apache.druid.indexing.kinesis.KinesisIndexTask;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskClientFactory;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskIOConfig;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskTuningConfig;
import org.apache.druid.indexing.kinesis.KinesisRecordSupplier;
import org.apache.druid.indexing.kinesis.KinesisSequenceNumber;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorReportPayload;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Supervisor responsible for managing the KinesisIndexTask for a single dataSource. At a high level, the class accepts a
 * {@link KinesisSupervisorSpec} which includes the Kinesis stream and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the Kinesis stream's partitions
 * and the list of running indexing tasks and ensures that all partitions are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * Kinesis sequences.
 */
public class KinesisSupervisor extends SeekableStreamSupervisor<String, String, ByteEntity>
{
  private static final EmittingLogger log = new EmittingLogger(KinesisSupervisor.class);

  public static final TypeReference<TreeMap<Integer, Map<String, String>>> CHECKPOINTS_TYPE_REF =
      new TypeReference<TreeMap<Integer, Map<String, String>>>()
      {
      };

  public static final String OFFSET_NOT_SET = "-1";
  private final KinesisSupervisorSpec spec;
  private final AWSCredentialsConfig awsCredentialsConfig;
  private volatile Map<String, Long> currentPartitionTimeLag;

  public KinesisSupervisor(
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final KinesisIndexTaskClientFactory taskClientFactory,
      final ObjectMapper mapper,
      final KinesisSupervisorSpec spec,
      final RowIngestionMetersFactory rowIngestionMetersFactory,
      final AWSCredentialsConfig awsCredentialsConfig
  )
  {
    super(
        StringUtils.format("KinesisSupervisor-%s", spec.getDataSchema().getDataSource()),
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        mapper,
        spec,
        rowIngestionMetersFactory,
        true
    );

    this.spec = spec;
    this.awsCredentialsConfig = awsCredentialsConfig;
    this.currentPartitionTimeLag = null;
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
      SeekableStreamSupervisorIOConfig ioConfigg
  )
  {
    KinesisSupervisorIOConfig ioConfig = (KinesisSupervisorIOConfig) ioConfigg;
    return new KinesisIndexTaskIOConfig(
        groupId,
        baseSequenceName,
        new SeekableStreamStartSequenceNumbers<>(
            ioConfig.getStream(),
            startPartitions,
            exclusiveStartSequenceNumberPartitions
        ),
        new SeekableStreamEndSequenceNumbers<>(ioConfig.getStream(), endPartitions),
        true,
        minimumMessageTime,
        maximumMessageTime,
        ioConfig.getInputFormat(),
        ioConfig.getEndpoint(),
        ioConfig.getFetchDelayMillis(),
        ioConfig.getAwsAssumedRoleArn(),
        ioConfig.getAwsExternalId()
    );
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
  ) throws JsonProcessingException
  {
    final String checkpoints = sortingMapper.writerFor(CHECKPOINTS_TYPE_REF).writeValueAsString(sequenceOffsets);
    final Map<String, Object> context = createBaseTaskContexts();
    context.put(CHECKPOINTS_CTX_KEY, checkpoints);

    List<SeekableStreamIndexTask<String, String, ByteEntity>> taskList = new ArrayList<>();
    for (int i = 0; i < replicas; i++) {
      String taskId = IdUtils.getRandomIdWithPrefix(baseSequenceName);
      taskList.add(new KinesisIndexTask(
          taskId,
          new TaskResource(baseSequenceName, 1),
          spec.getDataSchema(),
          (KinesisIndexTaskTuningConfig) taskTuningConfig,
          (KinesisIndexTaskIOConfig) taskIoConfig,
          context,
          spec.getSpec().getTuningConfig().isUseListShards(),
          awsCredentialsConfig
      ));
    }
    return taskList;
  }


  @Override
  protected RecordSupplier<String, String, ByteEntity> setupRecordSupplier() throws RuntimeException
  {
    KinesisSupervisorIOConfig ioConfig = spec.getIoConfig();
    KinesisIndexTaskTuningConfig taskTuningConfig = spec.getTuningConfig();

    return new KinesisRecordSupplier(
        KinesisRecordSupplier.getAmazonKinesisClient(
            ioConfig.getEndpoint(),
            awsCredentialsConfig,
            ioConfig.getAwsAssumedRoleArn(),
            ioConfig.getAwsExternalId()
        ),
        ioConfig.getFetchDelayMillis(),
        0, // skip starting background fetch, it is not used
        taskTuningConfig.getRecordBufferSizeBytesOrDefault(Runtime.getRuntime().maxMemory()),
        taskTuningConfig.getRecordBufferOfferTimeout(),
        taskTuningConfig.getRecordBufferFullWait(),
        taskTuningConfig.getMaxBytesPerPollOrDefault(),
        ioConfig.isUseEarliestSequenceNumber(),
        spec.getSpec().getTuningConfig().isUseListShards()
    );
  }

  /**
   * We hash the shard ID string, and then use the first four bytes of the hash as an int % task count
   */
  @Override
  protected int getTaskGroupIdForPartition(String partitionId)
  {
    return getTaskGroupIdForPartitionWithProvidedList(partitionId, partitionIds);
  }

  private int getTaskGroupIdForPartitionWithProvidedList(String partitionId, List<String> availablePartitions)
  {
    int index = availablePartitions.indexOf(partitionId);
    if (index < 0) {
      return index;
    }
    return availablePartitions.indexOf(partitionId) % spec.getIoConfig().getTaskCount();
  }

  @Override
  protected Map<Integer, Set<String>> recomputePartitionGroupsForExpiration(Set<String> availablePartitions)
  {
    List<String> availablePartitionsList = new ArrayList<>(availablePartitions);

    Map<Integer, Set<String>> newPartitionGroups = new HashMap<>();

    for (String availablePartition : availablePartitions) {
      int newTaskGroupId = getTaskGroupIdForPartitionWithProvidedList(availablePartition, availablePartitionsList);
      Set<String> newGroup = newPartitionGroups.computeIfAbsent(
          newTaskGroupId,
          k -> new HashSet<>()
      );
      newGroup.add(availablePartition);
    }

    return newPartitionGroups;
  }

  @Override
  protected boolean checkSourceMetadataMatch(DataSourceMetadata metadata)
  {
    return metadata instanceof KinesisDataSourceMetadata;
  }

  @Override
  protected boolean doesTaskTypeMatchSupervisor(Task task)
  {
    return task instanceof KinesisIndexTask;
  }

  @Override
  protected SeekableStreamSupervisorReportPayload<String, String> createReportPayload(
      int numPartitions,
      boolean includeOffsets
  )
  {
    KinesisSupervisorIOConfig ioConfig = spec.getIoConfig();
    Map<String, Long> partitionLag = getTimeLagPerPartition(getHighestCurrentOffsets());
    return new KinesisSupervisorReportPayload(
        spec.getDataSchema().getDataSource(),
        ioConfig.getStream(),
        numPartitions,
        ioConfig.getReplicas(),
        ioConfig.getTaskDuration().getMillis() / 1000,
        spec.isSuspended(),
        stateManager.isHealthy(),
        stateManager.getSupervisorState().getBasicState(),
        stateManager.getSupervisorState(),
        stateManager.getExceptionEvents(),
        includeOffsets ? partitionLag : null,
        includeOffsets ? partitionLag.values().stream().mapToLong(x -> Math.max(x, 0)).sum() : null
    );
  }

  // not yet supported, will be implemented in the future maybe? need a way to get record count between current
  // sequence and latest sequence
  @Override
  protected Map<String, Long> getRecordLagPerPartition(Map<String, String> currentOffsets)
  {
    return ImmutableMap.of();
  }

  @Override
  protected Map<String, Long> getTimeLagPerPartition(Map<String, String> currentOffsets)
  {
    return currentOffsets
        .entrySet()
        .stream()
        .filter(e -> e.getValue() != null &&
                     currentPartitionTimeLag != null &&
                     currentPartitionTimeLag.get(e.getKey()) != null
        )
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e -> currentPartitionTimeLag.get(e.getKey())
            )
        );
  }

  @Override
  protected SeekableStreamDataSourceMetadata<String, String> createDataSourceMetaDataForReset(
      String stream,
      Map<String, String> map
  )
  {
    return new KinesisDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(stream, map));
  }

  @Override
  protected OrderedSequenceNumber<String> makeSequenceNumber(String seq, boolean isExclusive)
  {
    return KinesisSequenceNumber.of(seq, isExclusive);
  }

  @Override
  protected void updatePartitionLagFromStream()
  {
    KinesisRecordSupplier supplier = (KinesisRecordSupplier) recordSupplier;
    // this recordSupplier method is thread safe, so does not need to acquire the recordSupplierLock
    currentPartitionTimeLag = supplier.getPartitionsTimeLag(getIoConfig().getStream(), getHighestCurrentOffsets());
  }

  @Override
  protected Map<String, Long> getPartitionRecordLag()
  {
    return null;
  }

  @Override
  protected Map<String, Long> getPartitionTimeLag()
  {
    return currentPartitionTimeLag;
  }

  @Override
  protected String baseTaskName()
  {
    return "index_kinesis";
  }

  @Override
  protected String getNotSetMarker()
  {
    return OFFSET_NOT_SET;
  }

  @Override
  protected String getEndOfPartitionMarker()
  {
    return KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER;
  }

  @Override
  protected boolean isEndOfShard(String seqNum)
  {
    return KinesisSequenceNumber.END_OF_SHARD_MARKER.equals(seqNum);
  }

  @Override
  protected boolean isShardExpirationMarker(String seqNum)
  {
    return KinesisSequenceNumber.EXPIRED_MARKER.equals(seqNum);
  }

  @Override
  protected boolean useExclusiveStartSequenceNumberForNonFirstSequence()
  {
    return true;
  }

  // Unlike the Kafka Indexing Service,
  // Kinesis reports lag metrics measured in time difference in milliseconds between the current sequence number and latest sequence number,
  // rather than message count.
  @Override
  public LagStats computeLagStats()
  {
    Map<String, Long> partitionTimeLags = getPartitionTimeLag();

    if (partitionTimeLags == null) {
      return new LagStats(0, 0, 0);
    }

    return computeLags(partitionTimeLags);
  }

  @Override
  protected Map<String, OrderedSequenceNumber<String>> filterExpiredPartitionsFromStartingOffsets(
      Map<String, OrderedSequenceNumber<String>> startingOffsets
  )
  {
    Map<String, OrderedSequenceNumber<String>> filteredOffsets = new HashMap<>();
    for (Map.Entry<String, OrderedSequenceNumber<String>> entry : startingOffsets.entrySet()) {
      if (!entry.getValue().get().equals(KinesisSequenceNumber.END_OF_SHARD_MARKER)) {
        filteredOffsets.put(entry.getKey(), entry.getValue());
      } else {
        log.debug("Excluding shard[%s] because it has reached EOS.", entry.getKey());
      }
    }
    return filteredOffsets;
  }

  @Override
  protected boolean supportsPartitionExpiration()
  {
    return true;
  }

  @Override
  protected SeekableStreamDataSourceMetadata<String, String> createDataSourceMetadataWithExpiredPartitions(
      SeekableStreamDataSourceMetadata<String, String> currentMetadata, Set<String> expiredPartitionIds
  )
  {
    log.info("Marking expired shards in metadata: " + expiredPartitionIds);

    return createDataSourceMetadataWithClosedOrExpiredPartitions(
        currentMetadata,
        expiredPartitionIds,
        KinesisSequenceNumber.EXPIRED_MARKER
    );
  }

  private SeekableStreamDataSourceMetadata<String, String> createDataSourceMetadataWithClosedOrExpiredPartitions(
      SeekableStreamDataSourceMetadata<String, String> currentMetadata,
      Set<String> terminatedPartitionIds,
      String terminationMarker
  )
  {
    final KinesisDataSourceMetadata dataSourceMetadata = (KinesisDataSourceMetadata) currentMetadata;

    SeekableStreamSequenceNumbers<String, String> old = dataSourceMetadata.getSeekableStreamSequenceNumbers();

    Map<String, String> oldPartitionSequenceNumberMap = old.getPartitionSequenceNumberMap();
    Map<String, String> newPartitionSequenceNumberMap = new HashMap<>();
    for (Map.Entry<String, String> entry : oldPartitionSequenceNumberMap.entrySet()) {
      if (!terminatedPartitionIds.contains(entry.getKey())) {
        newPartitionSequenceNumberMap.put(entry.getKey(), entry.getValue());
      } else {
        newPartitionSequenceNumberMap.put(entry.getKey(), terminationMarker);
      }
    }

    SeekableStreamSequenceNumbers<String, String> newSequences;
    if (old instanceof SeekableStreamStartSequenceNumbers) {
      Set<String> oldExclusiveStartPartitions;
      Set<String> newExclusiveStartPartitions;

      newExclusiveStartPartitions = new HashSet<>();
      oldExclusiveStartPartitions = ((SeekableStreamStartSequenceNumbers<String, String>) old).getExclusivePartitions();
      for (String partitionId : oldExclusiveStartPartitions) {
        if (!terminatedPartitionIds.contains(partitionId)) {
          newExclusiveStartPartitions.add(partitionId);
        }
      }

      newSequences = new SeekableStreamStartSequenceNumbers<>(
          old.getStream(),
          null,
          newPartitionSequenceNumberMap,
          null,
          newExclusiveStartPartitions
      );
    } else {
      newSequences = new SeekableStreamEndSequenceNumbers<>(
          old.getStream(),
          null,
          newPartitionSequenceNumberMap,
          null
      );
    }

    return new KinesisDataSourceMetadata(newSequences);
  }
}

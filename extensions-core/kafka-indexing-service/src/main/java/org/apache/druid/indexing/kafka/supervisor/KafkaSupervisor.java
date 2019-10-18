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
import com.google.common.base.Joiner;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
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
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorReportPayload;
import org.apache.druid.indexing.seekablestream.utils.RandomIdUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Supervisor responsible for managing the KafkaIndexTasks for a single dataSource. At a high level, the class accepts a
 * {@link KafkaSupervisorSpec} which includes the Kafka topic and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the Kafka topic's partitions
 * and the list of running indexing tasks and ensures that all partitions are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * Kafka offsets.
 */
public class KafkaSupervisor extends SeekableStreamSupervisor<Integer, Long>
{
  public static final TypeReference<TreeMap<Integer, Map<Integer, Long>>> CHECKPOINTS_TYPE_REF =
      new TypeReference<TreeMap<Integer, Map<Integer, Long>>>()
      {
      };

  private static final EmittingLogger log = new EmittingLogger(KafkaSupervisor.class);
  private static final long MINIMUM_GET_OFFSET_PERIOD_MILLIS = 5000;
  private static final long INITIAL_GET_OFFSET_DELAY_MILLIS = 15000;
  private static final long INITIAL_EMIT_LAG_METRIC_DELAY_MILLIS = 25000;
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
  protected RecordSupplier<Integer, Long> setupRecordSupplier()
  {
    return new KafkaRecordSupplier(spec.getIoConfig().getConsumerProperties(), sortingMapper);
  }

  @Override
  protected void scheduleReporting(ScheduledExecutorService reportingExec)
  {
    KafkaSupervisorIOConfig ioConfig = spec.getIoConfig();
    KafkaSupervisorTuningConfig tuningConfig = spec.getTuningConfig();
    reportingExec.scheduleAtFixedRate(
        updateCurrentAndLatestOffsets(),
        ioConfig.getStartDelay().getMillis() + INITIAL_GET_OFFSET_DELAY_MILLIS, // wait for tasks to start up
        Math.max(
            tuningConfig.getOffsetFetchPeriod().getMillis(), MINIMUM_GET_OFFSET_PERIOD_MILLIS
        ),
        TimeUnit.MILLISECONDS
    );

    reportingExec.scheduleAtFixedRate(
        emitLag(),
        ioConfig.getStartDelay().getMillis() + INITIAL_EMIT_LAG_METRIC_DELAY_MILLIS, // wait for tasks to start up
        monitorSchedulerConfig.getEmitterPeriod().getMillis(),
        TimeUnit.MILLISECONDS
    );
  }


  @Override
  protected int getTaskGroupIdForPartition(Integer partition)
  {
    return partition % spec.getIoConfig().getTaskCount();
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
    Map<Integer, Long> partitionLag = getLagPerPartition(getHighestCurrentOffsets());
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
        maximumMessageTime
    );
  }

  @Override
  protected List<SeekableStreamIndexTask<Integer, Long>> createIndexTasks(
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

    List<SeekableStreamIndexTask<Integer, Long>> taskList = new ArrayList<>();
    for (int i = 0; i < replicas; i++) {
      String taskId = Joiner.on("_").join(baseSequenceName, RandomIdUtils.getRandomId());
      taskList.add(new KafkaIndexTask(
          taskId,
          new TaskResource(baseSequenceName, 1),
          spec.getDataSchema(),
          (KafkaIndexTaskTuningConfig) taskTuningConfig,
          (KafkaIndexTaskIOConfig) taskIoConfig,
          context,
          null,
          null,
          rowIngestionMetersFactory,
          sortingMapper,
          null
      ));
    }
    return taskList;
  }


  @Override
  // suppress use of CollectionUtils.mapValues() since the valueMapper function is dependent on map key here
  @SuppressWarnings("SSBasedInspection")
  protected Map<Integer, Long> getLagPerPartition(Map<Integer, Long> currentOffsets)
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
  protected KafkaDataSourceMetadata createDataSourceMetaDataForReset(String topic, Map<Integer, Long> map)
  {
    return new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, map));
  }

  @Override
  protected OrderedSequenceNumber<Long> makeSequenceNumber(Long seq, boolean isExclusive)
  {
    return KafkaSequenceNumber.of(seq);
  }

  private Runnable emitLag()
  {
    return () -> {
      try {
        Map<Integer, Long> highestCurrentOffsets = getHighestCurrentOffsets();
        String dataSource = spec.getDataSchema().getDataSource();

        if (latestSequenceFromStream == null) {
          throw new ISE("Latest offsets from Kafka have not been fetched");
        }

        if (!latestSequenceFromStream.keySet().equals(highestCurrentOffsets.keySet())) {
          log.warn(
              "Lag metric: Kafka partitions %s do not match task partitions %s",
              latestSequenceFromStream.keySet(),
              highestCurrentOffsets.keySet()
          );
        }

        Map<Integer, Long> partitionLags = getLagPerPartition(highestCurrentOffsets);
        long maxLag = 0, totalLag = 0, avgLag;
        for (long lag : partitionLags.values()) {
          if (lag > maxLag) {
            maxLag = lag;
          }
          totalLag += lag;
        }
        avgLag = partitionLags.size() == 0 ? 0 : totalLag / partitionLags.size();

        emitter.emit(
            ServiceMetricEvent.builder().setDimension("dataSource", dataSource).build("ingest/kafka/lag", totalLag)
        );
        emitter.emit(
            ServiceMetricEvent.builder().setDimension("dataSource", dataSource).build("ingest/kafka/maxLag", maxLag)
        );
        emitter.emit(
            ServiceMetricEvent.builder().setDimension("dataSource", dataSource).build("ingest/kafka/avgLag", avgLag)
        );
      }
      catch (Exception e) {
        log.warn(e, "Unable to compute Kafka lag");
      }
    };
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
  protected void updateLatestSequenceFromStream(
      RecordSupplier<Integer, Long> recordSupplier,
      Set<StreamPartition<Integer>> partitions
  )
  {
    latestSequenceFromStream = partitions.stream()
                                         .collect(Collectors.toMap(
                                             StreamPartition::getPartitionId,
                                             recordSupplier::getPosition
                                         ));
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

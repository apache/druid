package org.apache.druid.indexing.pulsar.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.pulsar.PulsarDataSourceMetadata;
import org.apache.druid.indexing.pulsar.PulsarIndexTask;
import org.apache.druid.indexing.pulsar.PulsarIndexTaskClientFactory;
import org.apache.druid.indexing.pulsar.PulsarIndexTaskIOConfig;
import org.apache.druid.indexing.pulsar.PulsarIndexTaskTuningConfig;
import org.apache.druid.indexing.pulsar.PulsarRecordSupplier;
import org.apache.druid.indexing.pulsar.PulsarSequenceNumber;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
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
import org.apache.druid.segment.indexing.TuningConfig;
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

/**
 * Supervisor responsible for managing the PulsarIndexTasks for a single dataSource. At a high level, the class accepts a
 * {@link PulsarSupervisorSpec} which includes the Pulsar topic and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the Pulsar topic's partitions
 * and the list of running indexing tasks and ensures that all partitions are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * Pulsar offsets.
 */
public class PulsarSupervisor extends SeekableStreamSupervisor<Integer, Long, ByteEntity>
{

  public static final TypeReference<TreeMap<Integer, Map<Integer, String>>> CHECKPOINTS_TYPE_REF =
      new TypeReference<TreeMap<Integer, Map<Integer, String>>>()
      {
      };

  private static final EmittingLogger log = new EmittingLogger(PulsarSupervisor.class);
  private static final Long NOT_SET = -1L;
  private static final Long END_OF_PARTITION = Long.MAX_VALUE;
  private static final String TASK_NAME = "index_pulsar";

  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private final PulsarSupervisorSpec spec;
  private volatile Map<Integer, Long> latestSequenceFromStream;

  public PulsarSupervisor(
      TaskStorage taskStorage,
      TaskMaster taskMaster,
      IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      PulsarIndexTaskClientFactory taskClientFactory,
      ObjectMapper mapper,
      PulsarSupervisorSpec spec,
      RowIngestionMetersFactory rowIngestionMetersFactory)
  {
    super(StringUtils.format("PulsarSupervisor-%s", spec.getDataSchema().getDataSource()),
        taskStorage, taskMaster, indexerMetadataStorageCoordinator, taskClientFactory, mapper, spec,
        rowIngestionMetersFactory, false);
    this.spec = spec;
    this.emitter = spec.getEmitter();
    this.monitorSchedulerConfig = spec.getMonitorSchedulerConfig();
  }

  @Override
  protected String baseTaskName()
  {
    return TASK_NAME;
  }

  @Override
  protected void updatePartitionLagFromStream()
  {
    getRecordSupplierLock().lock();
    try {
      Set<Integer> partitionIds;
      try {
        partitionIds = recordSupplier.getPartitionIds(getIoConfig().getStream());
      } catch (Exception e) {
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

  @Nullable
  @Override
  protected Map<Integer, Long> getPartitionRecordLag()
  {
    Map<Integer, Long> highestCurrentOffsets = getHighestCurrentOffsets();

    if (latestSequenceFromStream == null) {
      return null;
    }

    if (!latestSequenceFromStream.keySet().equals(highestCurrentOffsets.keySet())) {
      log.warn(
          "Lag metric: Pulsar partitions %s do not match task partitions %s",
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
    // TODO: how to measure lag in pulsar
    return null;
  }

  @Override
  protected SeekableStreamIndexTaskIOConfig createTaskIoConfig(int groupId, Map<Integer, Long> startPartitions,
                                                               Map<Integer, Long> endPartitions,
                                                               String baseSequenceName, DateTime minimumMessageTime,
                                                               DateTime maximumMessageTime,
                                                               Set<Integer> exclusiveStartSequenceNumberPartitions,
                                                               SeekableStreamSupervisorIOConfig ioConfig)
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
  protected List<SeekableStreamIndexTask<Integer, Long, ByteEntity>> createIndexTasks(int replicas,
                                                                                      String baseSequenceName,
                                                                                      ObjectMapper sortingMapper,
                                                                                      TreeMap<Integer, Map<Integer, Long>> sequenceOffsets,
                                                                                      SeekableStreamIndexTaskIOConfig taskIoConfig,
                                                                                      SeekableStreamIndexTaskTuningConfig taskTuningConfig,
                                                                                      RowIngestionMetersFactory rowIngestionMetersFactory)
      throws JsonProcessingException
  {
    final String checkpoints = sortingMapper.writerFor(CHECKPOINTS_TYPE_REF).writeValueAsString(sequenceOffsets);
    final Map<String, Object> context = createBaseTaskContexts();
    context.put(CHECKPOINTS_CTX_KEY, checkpoints);
    // Pulsar index task always uses incremental handoff since 0.16.0.
    // The below is for the compatibility when you want to downgrade your cluster to something earlier than 0.16.0.
    // Pulsar index task will pick up LegacyPulsarIndexTaskRunner without the below configuration.
    context.put("IS_INCREMENTAL_HANDOFF_SUPPORTED", true);

    List<SeekableStreamIndexTask<Integer, Long, ByteEntity>> taskList = new ArrayList<>();
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
  protected int getTaskGroupIdForPartition(Integer partition)
  {
    return partition % spec.getIoConfig().getTaskCount();
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
  protected SeekableStreamDataSourceMetadata<Integer, Long> createDataSourceMetaDataForReset(String topic,
                                                                                             Map<Integer, Long> map)
  {
    return new PulsarDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, map));
  }

  @Override
  protected OrderedSequenceNumber<Long> makeSequenceNumber(Long seq, boolean isExclusive)
  {
    return PulsarSequenceNumber.of(seq);
  }

  @Override
  protected Map<Integer, Long> getRecordLagPerPartition(Map<Integer, Long> currentOffsets)
  {
    return ImmutableMap.of();
  }

  @Override
  protected Map<Integer, Long> getTimeLagPerPartition(Map<Integer, Long> currentOffsets)
  {
    return ImmutableMap.of();
  }

  @Override
  protected RecordSupplier<Integer, Long, ByteEntity> setupRecordSupplier()
  {
    String serviceUrl = (String) ((PulsarSupervisorIOConfig) getIoConfig()).getServiceUrl();
    return new PulsarRecordSupplier(serviceUrl,
        StringUtils.format("PulsarSupervisor-%s", spec.getDataSchema().getDataSource()),
        TuningConfig.DEFAULT_MAX_ROWS_IN_MEMORY);
  }

  @Override
  protected SeekableStreamSupervisorReportPayload<Integer, Long> createReportPayload(int numPartitions,
                                                                                     boolean includeOffsets)
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
    Map<Integer, Long> partitionRecordLag = getPartitionRecordLag();
    if (partitionRecordLag == null) {
      return new LagStats(0, 0, 0);
    }

    return computeLags(partitionRecordLag);
  }
}

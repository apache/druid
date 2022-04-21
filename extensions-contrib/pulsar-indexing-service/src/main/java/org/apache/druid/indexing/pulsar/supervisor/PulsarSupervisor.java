package org.apache.druid.indexing.pulsar.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClient;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClientFactory;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorReportPayload;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.joda.time.DateTime;

/**
 * Supervisor responsible for managing the PulsarIndexTasks for a single dataSource. At a high level, the class accepts a
 * {@link PulsarSupervisorSpec} which includes the Pulsar topic and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the Pulsar topic's partitions
 * and the list of running indexing tasks and ensures that all partitions are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * Pulsar offsets.
 */
public class PulsarSupervisor extends SeekableStreamSupervisor<Integer, Long, ByteEntity> {
  public PulsarSupervisor(
                          TaskStorage taskStorage,
                          TaskMaster taskMaster,
                          IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
                          SeekableStreamIndexTaskClientFactory<? extends SeekableStreamIndexTaskClient<Integer, Long>> taskClientFactory,
                          ObjectMapper mapper,
                          SeekableStreamSupervisorSpec spec,
                          RowIngestionMetersFactory rowIngestionMetersFactory) {
    super(StringUtils.format("PulsarSupervisor-%s", spec.getDataSchema().getDataSource()),
      taskStorage, taskMaster, indexerMetadataStorageCoordinator, taskClientFactory, mapper, spec,
      rowIngestionMetersFactory, false);
  }

  @Override
  protected String baseTaskName() {
    return null;
  }

  @Override
  protected void updatePartitionLagFromStream() {

  }

  @Nullable
  @Override
  protected Map<Integer, Long> getPartitionRecordLag() {
    return null;
  }

  @Nullable
  @Override
  protected Map<Integer, Long> getPartitionTimeLag() {
    return null;
  }

  @Override
  protected SeekableStreamIndexTaskIOConfig createTaskIoConfig(int groupId, Map<Integer, Long> startPartitions,
                                                               Map<Integer, Long> endPartitions,
                                                               String baseSequenceName, DateTime minimumMessageTime,
                                                               DateTime maximumMessageTime,
                                                               Set<Integer> exclusiveStartSequenceNumberPartitions,
                                                               SeekableStreamSupervisorIOConfig ioConfig) {
    return null;
  }

  @Override
  protected List<SeekableStreamIndexTask<Integer, Long, ByteEntity>> createIndexTasks(int replicas,
                                                                                      String baseSequenceName,
                                                                                      ObjectMapper sortingMapper,
                                                                                      TreeMap<Integer, Map<Integer, Long>> sequenceOffsets,
                                                                                      SeekableStreamIndexTaskIOConfig taskIoConfig,
                                                                                      SeekableStreamIndexTaskTuningConfig taskTuningConfig,
                                                                                      RowIngestionMetersFactory rowIngestionMetersFactory)
    throws JsonProcessingException {
    return null;
  }

  @Override
  protected int getTaskGroupIdForPartition(Integer partition) {
    return 0;
  }

  @Override
  protected boolean checkSourceMetadataMatch(DataSourceMetadata metadata) {
    return false;
  }

  @Override
  protected boolean doesTaskTypeMatchSupervisor(Task task) {
    return false;
  }

  @Override
  protected SeekableStreamDataSourceMetadata<Integer, Long> createDataSourceMetaDataForReset(String stream,
                                                                                             Map<Integer, Long> map) {
    return null;
  }

  @Override
  protected OrderedSequenceNumber<Long> makeSequenceNumber(Long seq, boolean isExclusive) {
    return null;
  }

  @Override
  protected Map<Integer, Long> getRecordLagPerPartition(Map<Integer, Long> currentOffsets) {
    return null;
  }

  @Override
  protected Map<Integer, Long> getTimeLagPerPartition(Map<Integer, Long> currentOffsets) {
    return null;
  }

  @Override
  protected RecordSupplier<Integer, Long, ByteEntity> setupRecordSupplier() {
    return null;
  }

  @Override
  protected SeekableStreamSupervisorReportPayload<Integer, Long> createReportPayload(int numPartitions,
                                                                                     boolean includeOffsets) {
    return null;
  }

  @Override
  protected Long getNotSetMarker() {
    return null;
  }

  @Override
  protected Long getEndOfPartitionMarker() {
    return null;
  }

  @Override
  protected boolean isEndOfShard(Long seqNum) {
    return false;
  }

  @Override
  protected boolean isShardExpirationMarker(Long seqNum) {
    return false;
  }

  @Override
  protected boolean useExclusiveStartSequenceNumberForNonFirstSequence() {
    return false;
  }

  @Override
  public LagStats computeLagStats() {
    return null;
  }
}

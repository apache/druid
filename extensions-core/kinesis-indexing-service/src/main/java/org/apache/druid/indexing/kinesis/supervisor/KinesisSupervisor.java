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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.kinesis.KinesisDataSourceMetadata;
import org.apache.druid.indexing.kinesis.KinesisIOConfig;
import org.apache.druid.indexing.kinesis.KinesisIndexTask;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskClientFactory;
import org.apache.druid.indexing.kinesis.KinesisRecordSupplier;
import org.apache.druid.indexing.kinesis.KinesisSequenceNumber;
import org.apache.druid.indexing.kinesis.KinesisTuningConfig;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamPartitions;
import org.apache.druid.indexing.seekablestream.SeekableStreamTuningConfig;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.SequenceNumber;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorReportPayload;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * Supervisor responsible for managing the KinesisIndexTask for a single dataSource. At a high level, the class accepts a
 * {@link KinesisSupervisorSpec} which includes the Kafka topic and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the Kafka topic's partitions
 * and the list of running indexing tasks and ensures that all partitions are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * Kinesis sequences.
 * <p>
 * the Kinesis supervisor does not yet support incremental handoff and emitLag
 */
public class KinesisSupervisor extends SeekableStreamSupervisor<String, String>
{
  private static final String NOT_SET = "-1";
  private final KinesisSupervisorSpec spec;

  public KinesisSupervisor(
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final KinesisIndexTaskClientFactory taskClientFactory,
      final ObjectMapper mapper,
      final KinesisSupervisorSpec spec,
      final RowIngestionMetersFactory rowIngestionMetersFactory
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
        NOT_SET,
        SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER,
        true
    );

    this.spec = spec;
  }


  @Override
  public void checkpoint(
      @Nullable Integer taskGroupId,
      @Deprecated String baseSequenceName,
      DataSourceMetadata previousCheckPoint,
      DataSourceMetadata currentCheckPoint
  )
  {
    // not supported right now
    throw new UnsupportedOperationException("kinesis supervisor does not yet support checkpoints");
  }

  @Override
  protected SeekableStreamIOConfig createIoConfig(
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
    return new KinesisIOConfig(
        baseSequenceName,
        new SeekableStreamPartitions<>(ioConfig.getStream(), startPartitions),
        new SeekableStreamPartitions<>(ioConfig.getStream(), endPartitions),
        true,
        true, // should pause after reading otherwise the task may complete early which will confuse the supervisor
        minimumMessageTime,
        maximumMessageTime,
        ioConfig.getEndpoint(),
        ioConfig.getRecordsPerFetch(),
        ioConfig.getFetchDelayMillis(),
        ioConfig.getAwsAccessKeyId(),
        ioConfig.getAwsSecretAccessKey(),
        exclusiveStartSequenceNumberPartitions,
        ioConfig.getAwsAssumedRoleArn(),
        ioConfig.getAwsExternalId(),
        ioConfig.isDeaggregate()
    );
  }

  @Override
  protected List<SeekableStreamIndexTask<String, String>> createIndexTasks(
      int replicas,
      String baseSequenceName,
      ObjectMapper sortingMapper,
      TreeMap<Integer, Map<String, String>> sequenceOffsets,
      SeekableStreamIOConfig taskIoConfig,
      SeekableStreamTuningConfig taskTuningConfig,
      RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    List<SeekableStreamIndexTask<String, String>> taskList = new ArrayList<>();
    for (int i = 0; i < replicas; i++) {
      String taskId = Joiner.on("_").join(baseSequenceName, getRandomId());
      taskList.add(new KinesisIndexTask(
          taskId,
          new TaskResource(baseSequenceName, 1),
          spec.getDataSchema(),
          (KinesisTuningConfig) taskTuningConfig,
          (KinesisIOConfig) taskIoConfig,
          spec.getContext(),
          null,
          null,
          rowIngestionMetersFactory
      ));
    }
    return taskList;
  }

  @Override
  protected RecordSupplier<String, String> setupRecordSupplier()
  {
    KinesisSupervisorIOConfig ioConfig = spec.getIoConfig();
    KinesisTuningConfig taskTuningConfig = spec.getTuningConfig();
    return new KinesisRecordSupplier(
        ioConfig.getEndpoint(),
        ioConfig.getAwsAccessKeyId(),
        ioConfig.getAwsSecretAccessKey(),
        ioConfig.getRecordsPerFetch(),
        ioConfig.getFetchDelayMillis(),
        1,
        ioConfig.getAwsAssumedRoleArn(),
        ioConfig.getAwsExternalId(),
        ioConfig.isDeaggregate(),
        taskTuningConfig.getRecordBufferSize(),
        taskTuningConfig.getRecordBufferOfferTimeout(),
        taskTuningConfig.getRecordBufferFullWait(),
        taskTuningConfig.getFetchSequenceNumberTimeout()
    );
  }


  @Override
  protected void scheduleReporting(ScheduledExecutorService reportingExec)
  {
    // Implement this for Kinesis which uses approximate time from latest instead of offset lag
/*
        reportingExec.scheduleAtFixedRate(
            computeAndEmitLag(taskClient),
            ioConfig.getStartDelay().getMillis() + 10000, // wait for tasks to start up
            Math.max(monitorSchedulerConfig.getEmitterPeriod().getMillis(), 60 * 1000),
            TimeUnit.MILLISECONDS
        );
*/
  }

  @Override
  protected int getTaskGroupIdForPartition(String partitionId)
  {
    if (!partitionIds.contains(partitionId)) {
      partitionIds.add(partitionId);
    }

    return partitionIds.indexOf(partitionId) % spec.getIoConfig().getTaskCount();
  }

  @Override
  protected boolean checkSourceMetadataMatch(DataSourceMetadata metadata)
  {
    return metadata instanceof KinesisDataSourceMetadata;
  }

  @Override
  protected boolean checkTaskInstance(Task task)
  {
    return task instanceof KinesisIndexTask;
  }

  @Override
  protected SeekableStreamSupervisorReportPayload<String, String> createReportPayload(
      int numPartitions, boolean includeOffsets
  )
  {
    KinesisSupervisorIOConfig ioConfig = spec.getIoConfig();
    return new KinesisSupervisorReportPayload(
        spec.getDataSchema().getDataSource(),
        ioConfig.getStream(),
        numPartitions,
        ioConfig.getReplicas(),
        ioConfig.getTaskDuration().getMillis() / 1000,
        spec.isSuspended()
    );
  }

  // not yet supported, will be implemented in the future
  @Override
  protected Map<String, String> getLagPerPartition(Map<String, String> currentOffsets)
  {
    return ImmutableMap.of();
  }

  @Override
  protected SeekableStreamDataSourceMetadata<String, String> createDataSourceMetaData(
      String stream, Map<String, String> map
  )
  {
    return new KinesisDataSourceMetadata(
        new SeekableStreamPartitions<>(stream, map)
    );
  }

  @Override
  protected SequenceNumber<String> makeSequenceNumber(
      String seq, boolean useExclusive, boolean isExclusive
  )
  {
    return KinesisSequenceNumber.of(seq, useExclusive, isExclusive);
  }

  // the following are for unit testing purposes only
  @Override
  @VisibleForTesting
  protected void runInternal()
      throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException
  {
    super.runInternal();
  }

  @Override
  @VisibleForTesting
  protected Runnable updateCurrentAndLatestOffsets()
  {
    return super.updateCurrentAndLatestOffsets();
  }

  @Override
  @VisibleForTesting
  protected void gracefulShutdownInternal() throws ExecutionException, InterruptedException, TimeoutException
  {
    super.gracefulShutdownInternal();
  }

  @Override
  @VisibleForTesting
  protected void resetInternal(DataSourceMetadata dataSourceMetadata)
  {
    super.resetInternal(dataSourceMetadata);
  }

  @Override
  @VisibleForTesting
  protected void moveTaskGroupToPendingCompletion(int taskGroupId)
  {
    super.moveTaskGroupToPendingCompletion(taskGroupId);
  }

  @Override
  @VisibleForTesting
  protected int getNoticesQueueSize()
  {
    return super.getNoticesQueueSize();
  }

  @Override
  protected boolean checkSequenceAvailability(@NotNull String partition, @NotNull String sequenceFromMetadata)
      throws TimeoutException
  {
    String earliestSequence = super.getOffsetFromStreamForPartition(partition, true);
    return earliestSequence != null
           && KinesisSequenceNumber.of(earliestSequence).compareTo(KinesisSequenceNumber.of(sequenceFromMetadata)) <= 0;
  }


// Implement this for Kinesis which uses approximate time from latest instead of offset lag
/*
  private Runnable computeAndEmitLag(final KinesisIndexTaskClient taskClient)
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        try {
          final Map<String, List<PartitionInfo>> topics = lagComputingConsumer.listTopics();
          final List<PartitionInfo> partitionInfoList = topics.get(ioConfig.getStream());
          lagComputingConsumer.assign(
              Lists.transform(partitionInfoList, new Function<PartitionInfo, TopicPartition>()
              {
                @Override
                public TopicPartition apply(PartitionInfo input)
                {
                  return new TopicPartition(ioConfig.getStream(), input.partition());
                }
              })
          );
          final Map<Integer, Long> offsetsResponse = new ConcurrentHashMap<>();
          final List<ListenableFuture<Void>> futures = Lists.newArrayList();
          for (TaskGroup taskGroup : taskGroups.values()) {
            for (String taskId : taskGroup.taskIds()) {
              futures.add(Futures.transform(
                  taskClient.getCurrentOffsetsAsync(taskId, false),
                  new Function<Map<Integer, Long>, Void>()
                  {
                    @Override
                    public Void apply(Map<Integer, Long> taskResponse)
                    {
                      if (taskResponse != null) {
                        for (final Map.Entry<Integer, Long> partitionOffsets : taskResponse.entrySet()) {
                          offsetsResponse.compute(partitionOffsets.getKey(), new BiFunction<Integer, Long, Long>()
                          {
                            @Override
                            public Long apply(Integer key, Long existingOffsetInMap)
                            {
                              // If existing value is null use the offset returned by task
                              // otherwise use the max (makes sure max offset is taken from replicas)
                              return existingOffsetInMap == null
                                     ? partitionOffsets.getValue()
                                     : Math.max(partitionOffsets.getValue(), existingOffsetInMap);
                            }
                          });
                        }
                      }
                      return null;
                    }
                  }
                          )
              );
            }
          }
          // not using futureTimeoutInSeconds as its min value is 120 seconds
          // and minimum emission period for this metric is 60 seconds
          Futures.successfulAsList(futures).get(30, TimeUnit.SECONDS);

          // for each partition, seek to end to get the highest offset
          // check the offsetsResponse map for the latest consumed offset
          // if partition info not present in offsetsResponse then use lastCurrentOffsets map
          // if not present there as well, fail the compute

          long lag = 0;
          for (PartitionInfo partitionInfo : partitionInfoList) {
            long diff;
            final TopicPartition topicPartition = new TopicPartition(ioConfig.getStream(), partitionInfo.partition());
            lagComputingConsumer.seekToEnd(ImmutableList.of(topicPartition));
            if (offsetsResponse.get(topicPartition.partition()) != null) {
              diff = lagComputingConsumer.position(topicPartition) - offsetsResponse.get(topicPartition.partition());
              lastCurrentOffsets.put(topicPartition.partition(), offsetsResponse.get(topicPartition.partition()));
            } else if (lastCurrentOffsets.get(topicPartition.partition()) != null) {
              diff = lagComputingConsumer.position(topicPartition) - lastCurrentOffsets.get(topicPartition.partition());
            } else {
              throw new ISE("Could not find latest consumed offset for partition [%d]", topicPartition.partition());
            }
            lag += diff;
            log.debug(
                "Topic - [%s] Partition - [%d] : Partition lag [%,d], Total lag so far [%,d]",
                topicPartition.topic(),
                topicPartition.partition(),
                diff,
                lag
            );
          }
          emitter.emit(
              ServiceMetricEvent.builder().setDimension("dataSource", dataSource).build("ingest/kinesis/lag", lag)
          );
        }
        catch (InterruptedException e) {
          // do nothing, probably we are shutting down
        }
        catch (Exception e) {
          log.warn(e, "Unable to compute Kinesis lag");
        }
      }
    };
  }
  */
}

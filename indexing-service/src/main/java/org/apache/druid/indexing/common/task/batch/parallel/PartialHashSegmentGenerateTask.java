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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SurrogateTaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.SegmentAllocatorForBatch;
import org.apache.druid.indexing.common.task.SegmentAllocators;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.batch.parallel.iterator.DefaultIndexTaskInputRowIteratorBuilder;
import org.apache.druid.indexing.common.task.batch.partition.HashPartitionAnalysis;
import org.apache.druid.indexing.worker.shuffle.ShuffleDataSegmentPusher;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.apache.druid.timeline.partition.PartialShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The worker task of {@link PartialHashSegmentGenerateParallelIndexTaskRunner}. This task partitions input data by
 * hashing the segment granularity and partition dimensions in {@link HashedPartitionsSpec}. Partitioned segments are
 * stored in local storage using {@link ShuffleDataSegmentPusher}.
 */
public class PartialHashSegmentGenerateTask extends PartialSegmentGenerateTask<GeneratedPartitionsMetadataReport>
{
  public static final String TYPE = "partial_index_generate";
  private static final String PROP_SPEC = "spec";

  private final int numAttempts;
  private final ParallelIndexIngestionSpec ingestionSchema;
  private final String supervisorTaskId;
  @Nullable
  private final Map<Interval, Integer> intervalToNumShardsOverride;

  @JsonCreator
  public PartialHashSegmentGenerateTask(
      // id shouldn't be null except when this task is created by ParallelIndexSupervisorTask
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("groupId") final String groupId,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("supervisorTaskId") final String supervisorTaskId,
      @JsonProperty("numAttempts") final int numAttempts, // zero-based counting
      @JsonProperty(PROP_SPEC) final ParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") final Map<String, Object> context,
      @JsonProperty("intervalToNumShardsOverride") @Nullable final Map<Interval, Integer> intervalToNumShardsOverride
  )
  {
    super(
        getOrMakeId(id, TYPE, ingestionSchema.getDataSchema().getDataSource()),
        groupId,
        taskResource,
        supervisorTaskId,
        ingestionSchema,
        context,
        new DefaultIndexTaskInputRowIteratorBuilder()
    );

    this.numAttempts = numAttempts;
    this.ingestionSchema = ingestionSchema;
    this.supervisorTaskId = supervisorTaskId;
    this.intervalToNumShardsOverride = intervalToNumShardsOverride;
  }

  @JsonProperty
  public int getNumAttempts()
  {
    return numAttempts;
  }

  @JsonProperty(PROP_SPEC)
  public ParallelIndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @JsonProperty
  public String getSupervisorTaskId()
  {
    return supervisorTaskId;
  }

  @Nullable
  @JsonProperty
  public Map<Interval, Integer> getIntervalToNumShardsOverride()
  {
    return intervalToNumShardsOverride;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return tryTimeChunkLock(
        new SurrogateTaskActionClient(supervisorTaskId, taskActionClient),
        getIngestionSchema().getDataSchema().getGranularitySpec().inputIntervals()
    );
  }

  @Override
  SegmentAllocatorForBatch createSegmentAllocator(TaskToolbox toolbox, ParallelIndexSupervisorTaskClient taskClient)
      throws IOException
  {
    final GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();
    final ParallelIndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();
    final HashedPartitionsSpec partitionsSpec = (HashedPartitionsSpec) tuningConfig.getGivenOrDefaultPartitionsSpec();
    return SegmentAllocators.forNonLinearPartitioning(
        toolbox,
        getDataSource(),
        getId(),
        granularitySpec,
        new SupervisorTaskAccess(supervisorTaskId, taskClient),
        createHashPartitionAnalysisFromPartitionsSpec(
            granularitySpec,
            partitionsSpec,
            intervalToNumShardsOverride
        )
    );
  }

  @Override
  GeneratedPartitionsMetadataReport createGeneratedPartitionsReport(TaskToolbox toolbox, List<DataSegment> segments)
  {
    List<GenericPartitionStat> partitionStats = segments.stream()
                                                        .map(segment -> createPartitionStat(toolbox, segment))
                                                        .collect(Collectors.toList());
    return new GeneratedPartitionsMetadataReport(getId(), partitionStats);
  }

  private GenericPartitionStat createPartitionStat(TaskToolbox toolbox, DataSegment segment)
  {
    return new GenericPartitionStat(
        toolbox.getTaskExecutorNode().getHost(),
        toolbox.getTaskExecutorNode().getPortToUse(),
        toolbox.getTaskExecutorNode().isEnableTlsPort(),
        segment.getInterval(),
        (BucketNumberedShardSpec) segment.getShardSpec(),
        null, // numRows is not supported yet
        null  // sizeBytes is not supported yet
    );
  }

  /**
   * Creates shard specs based on the given configurations. The return value is a map between intervals created
   * based on the segment granularity and the shard specs to be created.
   * Note that the shard specs to be created is a pair of {@link PartialShardSpec} and number of segments per interval
   * and filled only when {@link #isGuaranteedRollup} = true. Otherwise, the return value contains only the set of
   * intervals generated based on the segment granularity.
   */
  public static HashPartitionAnalysis createHashPartitionAnalysisFromPartitionsSpec(
      GranularitySpec granularitySpec,
      @Nonnull HashedPartitionsSpec partitionsSpec,
      @Nullable Map<Interval, Integer> intervalToNumShardsOverride
  )
  {
    final HashPartitionAnalysis partitionAnalysis = new HashPartitionAnalysis(partitionsSpec);

    if (intervalToNumShardsOverride != null) {
      // Some intervals populated from granularitySpec can be missing in intervalToNumShardsOverride
      // because intervalToNumShardsOverride contains only the intervals which exist in input data.
      // We only care about the intervals in intervalToNumShardsOverride here.
      intervalToNumShardsOverride.forEach(partitionAnalysis::updateBucket);
    } else {
      final Iterable<Interval> intervals = granularitySpec.sortedBucketIntervals();
      final int numBucketsPerInterval = partitionsSpec.getNumShards() == null
                                        ? 1
                                        : partitionsSpec.getNumShards();

      intervals.forEach(interval -> partitionAnalysis.updateBucket(interval, numBucketsPerInterval));
    }
    return partitionAnalysis;
  }
}

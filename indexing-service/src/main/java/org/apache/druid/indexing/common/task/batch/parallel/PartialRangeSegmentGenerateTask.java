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
import com.google.common.base.Preconditions;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SurrogateTaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.SegmentAllocatorForBatch;
import org.apache.druid.indexing.common.task.SegmentAllocators;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.batch.parallel.iterator.RangePartitionIndexTaskInputRowIteratorBuilder;
import org.apache.druid.indexing.common.task.batch.partition.RangePartitionAnalysis;
import org.apache.druid.indexing.worker.shuffle.ShuffleDataSegmentPusher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.PartitionBoundaries;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The worker task of {@link PartialRangeSegmentGenerateParallelIndexTaskRunner}. This task partitions input data by
 * ranges of the partition dimension specified in {@link DimensionRangePartitionsSpec}. Partitioned segments are stored
 * in local storage using {@link ShuffleDataSegmentPusher}.
 */
public class PartialRangeSegmentGenerateTask extends PartialSegmentGenerateTask<GeneratedPartitionsMetadataReport>
{
  public static final String TYPE = "partial_range_index_generate";
  private static final String PROP_SPEC = "spec";
  private static final boolean SKIP_NULL = true;

  private final String supervisorTaskId;
  private final String subtaskSpecId;
  private final int numAttempts;
  private final ParallelIndexIngestionSpec ingestionSchema;
  private final Map<Interval, PartitionBoundaries> intervalToPartitions;

  @JsonCreator
  public PartialRangeSegmentGenerateTask(
      // id shouldn't be null except when this task is created by ParallelIndexSupervisorTask
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("groupId") String groupId,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("supervisorTaskId") String supervisorTaskId,
      // subtaskSpecId can be null only for old task versions.
      @JsonProperty("subtaskSpecId") @Nullable final String subtaskSpecId,
      @JsonProperty("numAttempts") int numAttempts, // zero-based counting
      @JsonProperty(PROP_SPEC) ParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("intervalToPartitions") Map<Interval, PartitionBoundaries> intervalToPartitions
  )
  {
    super(
        getOrMakeId(id, TYPE, ingestionSchema.getDataSchema().getDataSource()),
        groupId,
        taskResource,
        supervisorTaskId,
        ingestionSchema,
        context,
        new RangePartitionIndexTaskInputRowIteratorBuilder(getPartitionDimensions(ingestionSchema), !SKIP_NULL)
    );

    this.subtaskSpecId = subtaskSpecId;
    this.numAttempts = numAttempts;
    this.ingestionSchema = ingestionSchema;
    this.supervisorTaskId = supervisorTaskId;
    this.intervalToPartitions = intervalToPartitions;
  }

  private static List<String> getPartitionDimensions(ParallelIndexIngestionSpec ingestionSpec)
  {
    PartitionsSpec partitionsSpec = ingestionSpec.getTuningConfig().getPartitionsSpec();
    Preconditions.checkArgument(
        partitionsSpec instanceof DimensionRangePartitionsSpec,
        "%s or %s partitionsSpec required",
        DimensionRangePartitionsSpec.NAME,
        SingleDimensionPartitionsSpec.NAME
    );

    DimensionRangePartitionsSpec multiDimPartitionsSpec = (DimensionRangePartitionsSpec) partitionsSpec;
    List<String> partitionDimensions = multiDimPartitionsSpec.getPartitionDimensions();
    Preconditions.checkNotNull(partitionDimensions, "partitionDimension required");

    return partitionDimensions;
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

  @JsonProperty
  @Override
  public String getSubtaskSpecId()
  {
    return subtaskSpecId;
  }

  @JsonProperty
  public Map<Interval, PartitionBoundaries> getIntervalToPartitions()
  {
    return intervalToPartitions;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws IOException
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
    final RangePartitionAnalysis partitionAnalysis = new RangePartitionAnalysis(
        (DimensionRangePartitionsSpec) ingestionSchema.getTuningConfig().getPartitionsSpec()
    );
    intervalToPartitions.forEach(partitionAnalysis::updateBucket);
    return SegmentAllocators.forNonLinearPartitioning(
        toolbox,
        getDataSource(),
        getSubtaskSpecId(),
        ingestionSchema.getDataSchema().getGranularitySpec(),
        new SupervisorTaskAccess(supervisorTaskId, taskClient),
        partitionAnalysis
    );
  }

  @Override
  GeneratedPartitionsMetadataReport createGeneratedPartitionsReport(TaskToolbox toolbox, List<DataSegment> segments, Map<String, TaskReport> taskReport)
  {
    List<PartitionStat> partitionStats = segments.stream()
                                                        .map(segment -> toolbox.getIntermediaryDataManager().generatePartitionStat(toolbox, segment))
                                                        .collect(Collectors.toList());
    return new GeneratedPartitionsMetadataReport(getId(), partitionStats, taskReport);
  }
}

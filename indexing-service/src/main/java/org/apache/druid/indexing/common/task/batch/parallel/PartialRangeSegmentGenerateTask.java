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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.SegmentAllocatorForBatch;
import org.apache.druid.indexing.common.task.SegmentAllocators;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.batch.parallel.iterator.RangePartitionIndexTaskInputRowIteratorBuilder;
import org.apache.druid.indexing.common.task.batch.partition.RangePartitionAnalysis;
import org.apache.druid.indexing.worker.ShuffleDataSegmentPusher;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionBoundaries;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The worker task of {@link PartialRangeSegmentGenerateParallelIndexTaskRunner}. This task partitions input data by
 * ranges of the partition dimension specified in {@link SingleDimensionPartitionsSpec}. Partitioned segments are stored
 * in local storage using {@link ShuffleDataSegmentPusher}.
 */
public class PartialRangeSegmentGenerateTask extends PartialSegmentGenerateTask<GeneratedPartitionsMetadataReport>
{
  public static final String TYPE = "partial_range_index_generate";
  private static final String PROP_SPEC = "spec";
  private static final boolean SKIP_NULL = true;

  private final String supervisorTaskId;
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
      @JsonProperty("numAttempts") int numAttempts, // zero-based counting
      @JsonProperty(PROP_SPEC) ParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("intervalToPartitions") Map<Interval, PartitionBoundaries> intervalToPartitions,
      @JacksonInject IndexingServiceClient indexingServiceClient,
      @JacksonInject IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory,
      @JacksonInject AppenderatorsManager appenderatorsManager
  )
  {
    super(
        getOrMakeId(id, TYPE, ingestionSchema.getDataSchema().getDataSource()),
        groupId,
        taskResource,
        supervisorTaskId,
        ingestionSchema,
        context,
        indexingServiceClient,
        taskClientFactory,
        appenderatorsManager,
        new RangePartitionIndexTaskInputRowIteratorBuilder(getPartitionDimension(ingestionSchema), !SKIP_NULL)
    );

    this.numAttempts = numAttempts;
    this.ingestionSchema = ingestionSchema;
    this.supervisorTaskId = supervisorTaskId;
    this.intervalToPartitions = intervalToPartitions;
  }

  private static String getPartitionDimension(ParallelIndexIngestionSpec ingestionSpec)
  {
    PartitionsSpec partitionsSpec = ingestionSpec.getTuningConfig().getPartitionsSpec();
    Preconditions.checkArgument(
        partitionsSpec instanceof SingleDimensionPartitionsSpec,
        "%s partitionsSpec required",
        SingleDimensionPartitionsSpec.NAME
    );

    SingleDimensionPartitionsSpec singleDimPartitionsSpec = (SingleDimensionPartitionsSpec) partitionsSpec;
    String partitionDimension = singleDimPartitionsSpec.getPartitionDimension();
    Preconditions.checkNotNull(partitionDimension, "partitionDimension required");

    return partitionDimension;
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
  public boolean isReady(TaskActionClient taskActionClient)
  {
    return true;
  }

  @Override
  SegmentAllocatorForBatch createSegmentAllocator(TaskToolbox toolbox, ParallelIndexSupervisorTaskClient taskClient)
      throws IOException
  {
    final RangePartitionAnalysis partitionAnalysis = new RangePartitionAnalysis(
        (SingleDimensionPartitionsSpec) ingestionSchema.getTuningConfig().getPartitionsSpec()
    );
    intervalToPartitions.forEach(partitionAnalysis::updateBucket);
    return SegmentAllocators.forNonLinearPartitioning(
        toolbox,
        getDataSource(),
        getId(),
        ingestionSchema.getDataSchema().getGranularitySpec(),
        new SupervisorTaskAccess(supervisorTaskId, taskClient),
        partitionAnalysis
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
}

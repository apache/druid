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
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.HashPartitionCachingLocalSegmentAllocator;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.IndexTaskSegmentAllocator;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.batch.parallel.iterator.DefaultIndexTaskInputRowIteratorBuilder;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The worker task of {@link PartialHashSegmentGenerateParallelIndexTaskRunner}. This task partitions input data by
 * hashing the segment granularity and partition dimensions in {@link HashedPartitionsSpec}. Partitioned segments are
 * stored in local storage using {@link org.apache.druid.indexing.worker.ShuffleDataSegmentPusher}.
 */
public class PartialHashSegmentGenerateTask extends PartialSegmentGenerateTask<GeneratedHashPartitionsReport>
{
  public static final String TYPE = "partial_index_generate";
  private static final String PROP_SPEC = "spec";

  private final int numAttempts;
  private final ParallelIndexIngestionSpec ingestionSchema;
  private final String supervisorTaskId;

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
        new DefaultIndexTaskInputRowIteratorBuilder()
    );

    this.numAttempts = numAttempts;
    this.ingestionSchema = ingestionSchema;
    this.supervisorTaskId = supervisorTaskId;
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

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return tryTimeChunkLock(
        taskActionClient,
        getIngestionSchema().getDataSchema().getGranularitySpec().inputIntervals()
    );
  }

  @Override
  IndexTaskSegmentAllocator createSegmentAllocator(TaskToolbox toolbox) throws IOException
  {
    return new HashPartitionCachingLocalSegmentAllocator(
        toolbox,
        getId(),
        getDataSource(),
        createShardSpecs()
    );
  }

  @Override
  GeneratedHashPartitionsReport createGeneratedPartitionsReport(TaskToolbox toolbox, List<DataSegment> segments)
  {
    List<HashPartitionStat> partitionStats = segments.stream()
                                                     .map(segment -> createPartitionStat(toolbox, segment))
                                                     .collect(Collectors.toList());
    return new GeneratedHashPartitionsReport(getId(), partitionStats);
  }

  private HashPartitionStat createPartitionStat(TaskToolbox toolbox, DataSegment segment)
  {
    return new HashPartitionStat(
        toolbox.getTaskExecutorNode().getHost(),
        toolbox.getTaskExecutorNode().getPortToUse(),
        toolbox.getTaskExecutorNode().isEnableTlsPort(),
        segment.getInterval(),
        segment.getShardSpec().getPartitionNum(),
        null, // numRows is not supported yet
        null  // sizeBytes is not supported yet
    );
  }

  private Map<Interval, Pair<ShardSpecFactory, Integer>> createShardSpecs()
  {
    GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();
    final ParallelIndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();
    final HashedPartitionsSpec partitionsSpec = (HashedPartitionsSpec) tuningConfig.getGivenOrDefaultPartitionsSpec();

    return createShardSpecWithoutInputScan(
        granularitySpec,
        ingestionSchema.getIOConfig(),
        tuningConfig,
        partitionsSpec
    );
  }
}

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
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * The worker task of {@link PartialHashSegmentMergeParallelIndexTaskRunner}. This task reads partitioned segments
 * created by {@link PartialHashSegmentGenerateTask}s, merges them, and pushes to deep storage. The pushed segments are
 * reported to {@link PartialHashSegmentMergeParallelIndexTaskRunner}.
 */

public class PartialHashSegmentMergeTask
    extends PartialSegmentMergeTask<HashBasedNumberedShardSpec, HashPartitionLocation>
{
  public static final String TYPE = "partial_index_merge";

  private final HashedPartitionsSpec partitionsSpec;
  private final PartialHashSegmentMergeIngestionSpec ingestionSchema;

  @JsonCreator
  public PartialHashSegmentMergeTask(
      // id shouldn't be null except when this task is created by ParallelIndexSupervisorTask
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("groupId") final String groupId,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("supervisorTaskId") final String supervisorTaskId,
      @JsonProperty("numAttempts") final int numAttempts, // zero-based counting
      @JsonProperty("spec") final PartialHashSegmentMergeIngestionSpec ingestionSchema,
      @JsonProperty("context") final Map<String, Object> context,
      @JacksonInject IndexingServiceClient indexingServiceClient,
      @JacksonInject IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory,
      @JacksonInject ShuffleClient shuffleClient
  )
  {
    super(
        getOrMakeId(id, TYPE, ingestionSchema.getDataSchema().getDataSource()),
        groupId,
        taskResource,
        supervisorTaskId,
        ingestionSchema.getDataSchema(),
        ingestionSchema.getIOConfig(),
        ingestionSchema.getTuningConfig(),
        numAttempts,
        context,
        indexingServiceClient,
        taskClientFactory,
        shuffleClient
    );

    this.ingestionSchema = ingestionSchema;

    PartitionsSpec inputPartitionsSpec = ingestionSchema.getTuningConfig().getGivenOrDefaultPartitionsSpec();
    Preconditions.checkArgument(inputPartitionsSpec instanceof HashedPartitionsSpec, "hashed partitionsSpec required");
    partitionsSpec = (HashedPartitionsSpec) inputPartitionsSpec;
    Preconditions.checkNotNull(partitionsSpec.getNumShards(), "hashed partitionsSpec numShards required");
  }

  @JsonProperty("spec")
  private PartialHashSegmentMergeIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  HashBasedNumberedShardSpec createShardSpec(TaskToolbox toolbox, Interval interval, int partitionId)
  {
    return new HashBasedNumberedShardSpec(
        partitionId,
        Preconditions.checkNotNull(partitionsSpec.getNumShards(), "numShards"),
        partitionsSpec.getPartitionDimensions(),
        toolbox.getJsonMapper()
    );
  }
}

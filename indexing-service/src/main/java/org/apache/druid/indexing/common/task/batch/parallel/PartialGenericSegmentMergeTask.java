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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.timeline.partition.BuildingShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * {@link ParallelIndexTaskRunner} for the phase to merge generic partitioned segments in multi-phase parallel indexing.
 */
public class PartialGenericSegmentMergeTask extends PartialSegmentMergeTask<BuildingShardSpec, GenericPartitionLocation>
{
  public static final String TYPE = "partial_index_generic_merge";

  private final PartialGenericSegmentMergeIngestionSpec ingestionSchema;
  private final Table<Interval, Integer, BuildingShardSpec<?>> intervalAndIntegerToShardSpec;

  @JsonCreator
  public PartialGenericSegmentMergeTask(
      // id shouldn't be null except when this task is created by ParallelIndexSupervisorTask
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("groupId") final String groupId,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("supervisorTaskId") final String supervisorTaskId,
      @JsonProperty("numAttempts") final int numAttempts, // zero-based counting
      @JsonProperty("spec") final PartialGenericSegmentMergeIngestionSpec ingestionSchema,
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
    this.intervalAndIntegerToShardSpec = createIntervalAndIntegerToShardSpec(
        ingestionSchema.getIOConfig().getPartitionLocations()
    );
  }

  private static Table<Interval, Integer, BuildingShardSpec<?>> createIntervalAndIntegerToShardSpec(
      List<GenericPartitionLocation> partitionLocations
  )
  {
    final Table<Interval, Integer, BuildingShardSpec<?>> intervalAndIntegerToShardSpec = HashBasedTable.create();

    partitionLocations.forEach(
        p -> {
          final ShardSpec currShardSpec = intervalAndIntegerToShardSpec.get(p.getInterval(), p.getBucketId());
          if (currShardSpec == null) {
            intervalAndIntegerToShardSpec.put(p.getInterval(), p.getBucketId(), p.getShardSpec());
          } else {
            if (!p.getShardSpec().equals(currShardSpec)) {
              throw new ISE(
                  "interval %s, bucketId %s mismatched shard specs: %s and %s",
                  p.getInterval(),
                  p.getBucketId(),
                  currShardSpec,
                  p.getShardSpec()
              );
            }
          }
        }
    );

    return intervalAndIntegerToShardSpec;
  }

  @JsonProperty("spec")
  private PartialGenericSegmentMergeIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  BuildingShardSpec<?> createShardSpec(TaskToolbox toolbox, Interval interval, int partitionId)
  {
    return Preconditions.checkNotNull(
        intervalAndIntegerToShardSpec.get(interval, partitionId),
        "no shard spec exists for interval %s, partitionId %s: %s",
        interval,
        partitionId,
        intervalAndIntegerToShardSpec.rowMap()
    );
  }
}

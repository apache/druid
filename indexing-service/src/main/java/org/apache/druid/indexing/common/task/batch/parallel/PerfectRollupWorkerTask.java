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

import com.google.common.base.Preconditions;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Base class for parallel indexing perfect rollup worker tasks.
 */
abstract class PerfectRollupWorkerTask extends AbstractBatchIndexTask
{
  private final GranularitySpec granularitySpec;
  private final DataSchema dataSchema;
  private final ParallelIndexTuningConfig tuningConfig;

  PerfectRollupWorkerTask(
      String id,
      @Nullable String groupId,
      @Nullable TaskResource taskResource,
      DataSchema dataSchema,
      ParallelIndexTuningConfig tuningConfig,
      @Nullable Map<String, Object> context
  )
  {
    super(id, groupId, taskResource, dataSchema.getDataSource(), context);

    Preconditions.checkArgument(
        tuningConfig.isForceGuaranteedRollup(),
        "forceGuaranteedRollup must be set"
    );

    checkPartitionsSpec(tuningConfig.getGivenOrDefaultPartitionsSpec());

    granularitySpec = dataSchema.getGranularitySpec();
    Preconditions.checkArgument(
        !granularitySpec.inputIntervals().isEmpty(),
        "Missing intervals in granularitySpec"
    );

    this.dataSchema = dataSchema;
    this.tuningConfig = tuningConfig;
  }

  private static void checkPartitionsSpec(PartitionsSpec partitionsSpec)
  {
    if (!partitionsSpec.isForceGuaranteedRollupCompatible()) {
      String incompatibiltyMsg = partitionsSpec.getForceGuaranteedRollupIncompatiblityReason();
      String msg = "forceGuaranteedRollup is incompatible with partitionsSpec: " + incompatibiltyMsg;
      throw new IllegalArgumentException(msg);
    }
  }

  @Override
  public final boolean requireLockExistingSegments()
  {
    return true;
  }

  @Override
  public final List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
  {
    throw new UnsupportedOperationException("This task locks by timeChunk instead of segment");
  }

  @Override
  public final boolean isPerfectRollup()
  {
    return true;
  }

  @Nullable
  @Override
  public final Granularity getSegmentGranularity()
  {
    if (granularitySpec instanceof ArbitraryGranularitySpec) {
      return null;
    } else {
      return granularitySpec.getSegmentGranularity();
    }
  }

  DataSchema getDataSchema()
  {
    return dataSchema;
  }

  ParallelIndexTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }
}

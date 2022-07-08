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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.FirehoseFactoryToInputSourceAdaptor;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.indexing.common.Counters;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.TaskMonitor.SubTaskCompleteEvent;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.BuildingNumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of {@link ParallelIndexTaskRunner} to support best-effort roll-up. This runner can submit and
 * monitor multiple {@link SinglePhaseSubTask}s.
 * <p>
 * As its name indicates, distributed indexing is done in a single phase, i.e., without shuffling intermediate data. As
 * a result, this task can't be used for perfect rollup.
 */
public class SinglePhaseParallelIndexTaskRunner extends ParallelIndexPhaseRunner<SinglePhaseSubTask, PushedSegmentsReport>
{
  /**
   * A flag to determine what protocol to use for segment allocation. The Overlod sets this context explicitly
   * for all tasks to use the lineage-based protocol in 0.22 or later.
   */
  public static final String CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY = "useLineageBasedSegmentAllocation";

  /**
   * A legacy default for {@link #CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY} when the Overlord is running on
   * 0.21 or earlier.
   *
   * The new lineage-based segment allocation protocol must be used as the legacy protocol has a critical bug.
   * However, we tell subtasks to use the legacy protocol by default unless it is explicitly set in the taskContext.
   * This is to guarantee that every subtask uses the same protocol during the replacing rolling upgrade so that
   * batch tasks that are already running can continue. Once the upgrade is done, the Overlod will set this context
   * explicitly for all tasks to use the new protocol.
   *
   * @see SinglePhaseParallelIndexTaskRunner#allocateNewSegment(String, DateTime)
   * @see org.apache.druid.indexing.overlord.TaskQueue#add(Task)
   * @see #DEFAULT_USE_LINEAGE_BASED_SEGMENT_ALLOCATION
   */
  @Deprecated
  static final boolean LEGACY_DEFAULT_USE_LINEAGE_BASED_SEGMENT_ALLOCATION = false;

  /**
   * A new default for {@link #CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY} when the Overlord is running on 0.22
   * or later. The new lineage-based segment allocation protocol must be used to ensure data correctness.
   *
   * @see SinglePhaseParallelIndexTaskRunner#allocateNewSegment(String, DateTime, String, String)
   */
  public static final boolean DEFAULT_USE_LINEAGE_BASED_SEGMENT_ALLOCATION = true;

  private static final String PHASE_NAME = "segment generation";

  // interval -> next partitionId
  private final ConcurrentHashMap<Interval, AtomicInteger> partitionNumCountersPerInterval = new ConcurrentHashMap<>();

  // sequenceName -> list of segmentIds
  private final ConcurrentHashMap<String, List<String>> sequenceToSegmentIds = new ConcurrentHashMap<>();

  private final ParallelIndexIngestionSpec ingestionSchema;
  private final SplittableInputSource<?> baseInputSource;

  SinglePhaseParallelIndexTaskRunner(
      TaskToolbox toolbox,
      String taskId,
      String groupId,
      String baseSubtaskSpecName,
      ParallelIndexIngestionSpec ingestionSchema,
      Map<String, Object> context
  )
  {
    super(
        toolbox,
        taskId,
        groupId,
        baseSubtaskSpecName,
        ingestionSchema.getTuningConfig(),
        context
    );
    this.ingestionSchema = ingestionSchema;
    this.baseInputSource = (SplittableInputSource) ingestionSchema.getIOConfig().getNonNullInputSource(
        ingestionSchema.getDataSchema().getParser()
    );
  }

  @VisibleForTesting
  SinglePhaseParallelIndexTaskRunner(
      TaskToolbox toolbox,
      String taskId,
      String groupId,
      ParallelIndexIngestionSpec ingestionSchema,
      Map<String, Object> context
  )
  {
    this(toolbox, taskId, groupId, taskId, ingestionSchema, context);
  }

  @Override
  public String getName()
  {
    return PHASE_NAME;
  }

  @VisibleForTesting
  ParallelIndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @VisibleForTesting
  @Override
  Iterator<SubTaskSpec<SinglePhaseSubTask>> subTaskSpecIterator() throws IOException
  {
    return baseInputSource.createSplits(
        ingestionSchema.getIOConfig().getInputFormat(),
        getTuningConfig().getSplitHintSpec()
    ).map(this::newTaskSpec).iterator();
  }

  @Override
  int estimateTotalNumSubTasks() throws IOException
  {
    return baseInputSource.estimateNumSplits(
        ingestionSchema.getIOConfig().getInputFormat(),
        getTuningConfig().getSplitHintSpec()
    );
  }

  @VisibleForTesting
  SubTaskSpec<SinglePhaseSubTask> newTaskSpec(InputSplit split)
  {
    final FirehoseFactory firehoseFactory;
    final InputSource inputSource;
    if (baseInputSource instanceof FirehoseFactoryToInputSourceAdaptor) {
      firehoseFactory = ((FirehoseFactoryToInputSourceAdaptor) baseInputSource).getFirehoseFactory().withSplit(split);
      inputSource = null;
    } else {
      firehoseFactory = null;
      inputSource = baseInputSource.withSplit(split);
    }
    final Map<String, Object> subtaskContext = new HashMap<>(getContext());
    return new SinglePhaseSubTaskSpec(
        getBaseSubtaskSpecName() + "_" + getAndIncrementNextSpecId(),
        getGroupId(),
        getTaskId(),
        new ParallelIndexIngestionSpec(
            ingestionSchema.getDataSchema(),
            new ParallelIndexIOConfig(
                firehoseFactory,
                inputSource,
                ingestionSchema.getIOConfig().getInputFormat(),
                ingestionSchema.getIOConfig().isAppendToExisting(),
                ingestionSchema.getIOConfig().isDropExisting()
            ),
            ingestionSchema.getTuningConfig()
        ),
        subtaskContext,
        split
    );
  }

  /**
   * This method has a bug that transient task failures or network errors can create
   * non-contiguous partitionIds in time chunks. When this happens, the segments this method creates
   * will never become queryable (see {@link org.apache.druid.timeline.partition.PartitionHolder#isComplete()}.
   * As a result, this method is deprecated in favor of {@link #allocateNewSegment(String, DateTime, String, String)}.
   * However, we keep this method to support rolling upgrade without downtime of batch ingestion
   * where you can have mixed versions of middleManagers/indexers.
   */
  @Deprecated
  public SegmentIdWithShardSpec allocateNewSegment(String dataSource, DateTime timestamp) throws IOException
  {
    NonnullPair<Interval, String> intervalAndVersion = findIntervalAndVersion(timestamp);

    final int partitionNum = Counters.getAndIncrementInt(partitionNumCountersPerInterval, intervalAndVersion.lhs);
    return new SegmentIdWithShardSpec(
        dataSource,
        intervalAndVersion.lhs,
        intervalAndVersion.rhs,
        new BuildingNumberedShardSpec(partitionNum)
    );
  }

  /**
   * Allocate a new segment for the given timestamp locally. This method is called when dynamic partitioning is used
   * and {@link org.apache.druid.indexing.common.LockGranularity} is {@code TIME_CHUNK}.
   *
   * The allocation algorithm is similar to the Overlord-based segment allocation. It keeps the segment allocation
   * history per sequenceName. If the prevSegmentId is found in the segment allocation history, this method
   * returns the next segmentId right after the prevSegmentId in the history. Since the sequenceName is unique
   * per {@link SubTaskSpec} (it is the ID of subtaskSpec), this algorithm guarantees that the same set of segmentIds
   * are created in the same order for the same subtaskSpec.
   *
   * @see org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator#allocatePendingSegmentWithSegmentLineageCheck
   */
  public SegmentIdWithShardSpec allocateNewSegment(
      String dataSource,
      DateTime timestamp,
      String sequenceName,
      @Nullable String prevSegmentId
  ) throws IOException
  {
    NonnullPair<Interval, String> intervalAndVersion = findIntervalAndVersion(timestamp);

    MutableObject<SegmentIdWithShardSpec> segmentIdHolder = new MutableObject<>();
    sequenceToSegmentIds.compute(sequenceName, (k, v) -> {
      final int prevSegmentIdIndex;
      final List<String> segmentIds;
      if (prevSegmentId == null) {
        prevSegmentIdIndex = -1;
        segmentIds = v == null ? new ArrayList<>() : v;
      } else {
        segmentIds = v;
        if (segmentIds == null) {
          throw new ISE("Can't find previous segmentIds for sequence[%s]", sequenceName);
        }
        prevSegmentIdIndex = segmentIds.indexOf(prevSegmentId);
        if (prevSegmentIdIndex == -1) {
          throw new ISE("Can't find previously allocated segmentId[%s] for sequence[%s]", prevSegmentId, sequenceName);
        }
      }
      final int nextSegmentIdIndex = prevSegmentIdIndex + 1;
      final SegmentIdWithShardSpec newSegmentId;
      if (nextSegmentIdIndex < segmentIds.size()) {
        SegmentId segmentId = SegmentId.tryParse(dataSource, segmentIds.get(nextSegmentIdIndex));
        if (segmentId == null) {
          throw new ISE("Illegal segmentId format [%s]", segmentIds.get(nextSegmentIdIndex));
        }
        newSegmentId = new SegmentIdWithShardSpec(
            segmentId.getDataSource(),
            segmentId.getInterval(),
            segmentId.getVersion(),
            new BuildingNumberedShardSpec(segmentId.getPartitionNum())
        );
      } else {
        final int partitionNum = Counters.getAndIncrementInt(partitionNumCountersPerInterval, intervalAndVersion.lhs);
        newSegmentId = new SegmentIdWithShardSpec(
            dataSource,
            intervalAndVersion.lhs,
            intervalAndVersion.rhs,
            new BuildingNumberedShardSpec(partitionNum)
        );
        segmentIds.add(newSegmentId.toString());
      }
      segmentIdHolder.setValue(newSegmentId);
      return segmentIds;
    });
    return segmentIdHolder.getValue();
  }

  NonnullPair<Interval, String> findIntervalAndVersion(DateTime timestamp) throws IOException
  {
    return AbstractBatchIndexTask.findIntervalAndVersion(getToolbox(), ingestionSchema, timestamp);
  }

  @Override
  public Runnable getSubtaskCompletionCallback(SubTaskCompleteEvent<?> event)
  {
    return () -> {
      if (event.getLastState().isSuccess()) {
        sequenceToSegmentIds.remove(event.getSpec().getId());
      }
    };
  }
}

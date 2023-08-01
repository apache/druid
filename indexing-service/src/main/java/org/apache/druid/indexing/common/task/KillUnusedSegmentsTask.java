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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.druid.client.indexing.ClientKillUnusedSegmentsTaskQuery;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.MarkSegmentsAsUnusedAction;
import org.apache.druid.indexing.common.actions.RetrieveUnusedSegmentsAction;
import org.apache.druid.indexing.common.actions.SegmentNukeAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskLocks;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * The client representation of this task is {@link ClientKillUnusedSegmentsTaskQuery}.
 * JSON serialization fields of this class must correspond to those of {@link
 * ClientKillUnusedSegmentsTaskQuery}, except for "id" and "context" fields.
 */
public class KillUnusedSegmentsTask extends AbstractFixedIntervalTask
{
  private static final Logger LOG = new Logger(KillUnusedSegmentsTask.class);

  /**
   * Default nuke batch size. This is a small enough size that we still get value from batching, while
   * yielding as quickly as possible. In one real cluster environment backed with mysql, ~2000rows/sec,
   * with batch size of 100, means a batch should only less than a second for the task lock, and depending
   * on the segment store latency, unoptimised S3 cleanups typically take 5-10 seconds per 100. Over time
   * we expect the S3 cleanup to get quicker, so this should be < 1 second, which means we'll be yielding
   * the task lockbox every 1-2 seconds.
   */
  private static final int DEFAULT_SEGMENT_NUKE_BATCH_SIZE = 100;

  private final boolean markAsUnused;

  /**
   * Split processing to try and keep each nuke operation relatively short, in the case that either
   * the database or the storage layer is particularly slow.
   */
  private final int batchSize;

  // counter included primarily for testing
  private long numBatchesProcessed = 0;

  @JsonCreator
  public KillUnusedSegmentsTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("markAsUnused") Boolean markAsUnused,
      @JsonProperty("batchSize") Integer batchSize
  )
  {
    super(
        getOrMakeId(id, "kill", dataSource, interval),
        dataSource,
        interval,
        context
    );
    this.markAsUnused = markAsUnused != null && markAsUnused;
    this.batchSize = (batchSize != null) ? batchSize : DEFAULT_SEGMENT_NUKE_BATCH_SIZE;
    Preconditions.checkArgument(this.batchSize > 0, "batchSize should be greater than zero");
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isMarkAsUnused()
  {
    return markAsUnused;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getBatchSize()
  {
    return batchSize;
  }

  @Override
  public String getType()
  {
    return "kill";
  }

  @Nonnull
  @JsonIgnore
  @Override
  public Set<ResourceAction> getInputSourceResources()
  {
    return ImmutableSet.of();
  }

  @JsonIgnore
  @VisibleForTesting
  long getNumBatchesProcessed()
  {
    return numBatchesProcessed;
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
    final NavigableMap<DateTime, List<TaskLock>> taskLockMap = getTaskLockMap(toolbox.getTaskActionClient());

    if (markAsUnused) {
      int numMarked = toolbox.getTaskActionClient().submit(
          new MarkSegmentsAsUnusedAction(getDataSource(), getInterval())
      );
      LOG.info("Marked %d segments as unused.", numMarked);
    }

    // List unused segments
    final List<DataSegment> allUnusedSegments = toolbox
        .getTaskActionClient()
        .submit(new RetrieveUnusedSegmentsAction(getDataSource(), getInterval()));

    final List<List<DataSegment>> unusedSegmentBatches = Lists.partition(allUnusedSegments, batchSize);

    // The individual activities here on the toolbox have possibility to run for a longer period of time,
    // since they involve calls to metadata storage and archival object storage. And, the tasks take hold of the
    // task lockbox to run. By splitting the segment list into smaller batches, we have an opportunity to yield the
    // lock to other activity that might need to happen using the overlord tasklockbox.

    LOG.info("Running kill task[%s] for dataSource[%s] and interval[%s]. Killing total [%,d] unused segments in [%d] batches(batchSize = [%d]).",
            getId(), getDataSource(), getInterval(), allUnusedSegments.size(), unusedSegmentBatches.size(), batchSize);

    for (final List<DataSegment> unusedSegments : unusedSegmentBatches) {
      if (!TaskLocks.isLockCoversSegments(taskLockMap, unusedSegments)) {
        throw new ISE(
                "Locks[%s] for task[%s] can't cover segments[%s]",
                taskLockMap.values().stream().flatMap(List::stream).collect(Collectors.toList()),
                getId(),
                unusedSegments
        );
      }

      // Kill segments
      // Order is important here: we want the nuke action to clean up the metadata records _before_ the
      // segments are removed from storage, this helps maintain that we will always have a storage segment if
      // the metadata segment is present. If the segment nuke throws an exception, then the segment cleanup is
      // abandoned.

      toolbox.getTaskActionClient().submit(new SegmentNukeAction(new HashSet<>(unusedSegments)));
      toolbox.getDataSegmentKiller().kill(unusedSegments);
      numBatchesProcessed++;

      if (numBatchesProcessed % 10 == 0) {
        LOG.info("Processed [%d/%d] batches for kill task[%s].",
                numBatchesProcessed, unusedSegmentBatches.size(), getId());
      }
    }

    LOG.info("Finished kill task[%s] for dataSource[%s] and interval[%s]. Deleted total [%,d] unused segments in [%d] batches.",
            getId(), getDataSource(), getInterval(), allUnusedSegments.size(), unusedSegmentBatches.size());

    return TaskStatus.success(getId());
  }

  private NavigableMap<DateTime, List<TaskLock>> getTaskLockMap(TaskActionClient client) throws IOException
  {
    final NavigableMap<DateTime, List<TaskLock>> taskLockMap = new TreeMap<>();
    getTaskLocks(client).forEach(
        taskLock -> taskLockMap.computeIfAbsent(taskLock.getInterval().getStart(), k -> new ArrayList<>()).add(taskLock)
    );
    return taskLockMap;
  }
}

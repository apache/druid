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

  // We split this to try and keep each nuke operation relatively short, in the case that either
  // the database or the storage layer is particularly slow.
  private static final int SEGMENT_NUKE_BATCH_SIZE = 10_000;

  private final boolean markAsUnused;

  @JsonCreator
  public KillUnusedSegmentsTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("markAsUnused") Boolean markAsUnused
  )
  {
    super(
        getOrMakeId(id, "kill", dataSource, interval),
        dataSource,
        interval,
        context
    );
    this.markAsUnused = markAsUnused != null && markAsUnused;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isMarkAsUnused()
  {
    return markAsUnused;
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

    final List<List<DataSegment>> unusedSegmentBatches = Lists.partition(allUnusedSegments, SEGMENT_NUKE_BATCH_SIZE);

    // The individual activities here on the toolbox have possibility to run for a longer period of time,
    // since they involve calls to metadata storage and archival object storage. And, the tasks take hold of the
    // task lockbox to run. By splitting the segment list into smaller batches, we have an opportunity to yield the
    // lock to other activity that might need to happen using the overlord tasklockbox.

    for (final List<DataSegment> unusedSegments : unusedSegmentBatches) {
      if (!TaskLocks.isLockCoversSegments(taskLockMap, unusedSegments)) {
        throw new ISE(
                "Locks[%s] for task[%s] can't cover segments[%s]",
                taskLockMap.values().stream().flatMap(List::stream).collect(Collectors.toList()),
                getId(),
                unusedSegments
        );
      }

      // Kill segments:
      // Order is important here: we want the nuke action to clean up the metadata records _before_ the
      // segments are removed from storage, this helps maintain that we will always have a storage segment if
      // the metadata segment is present. If the segment nuke throws an exception, then the segment cleanup is
      // abandoned.

      toolbox.getTaskActionClient().submit(new SegmentNukeAction(new HashSet<>(unusedSegments)));
      toolbox.getDataSegmentKiller().kill(unusedSegments);
    }

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

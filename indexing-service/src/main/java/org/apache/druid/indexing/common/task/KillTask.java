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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentListUnusedAction;
import org.apache.druid.indexing.common.actions.SegmentNukeAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionPreconditions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 */
public class KillTask extends AbstractFixedIntervalTask
{
  private static final Logger log = new Logger(KillTask.class);

  @JsonCreator
  public KillTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        getOrMakeId(id, "kill", dataSource, interval),
        dataSource,
        interval,
        context
    );
  }

  @Override
  public String getType()
  {
    return "kill";
  }

  @Override
  public boolean requireLockInputSegments()
  {
    return true;
  }

  @Override
  public List<DataSegment> findInputSegments(TaskActionClient taskActionClient, List<Interval> intervals)
      throws IOException
  {
    final List<DataSegment> allSegments = new ArrayList<>();
    for (Interval interval : intervals) {
      allSegments.addAll(taskActionClient.submit(new SegmentListUnusedAction(getDataSource(), interval)));
    }
    return allSegments;
  }

  @Override
  public boolean changeSegmentGranularity(List<Interval> intervalOfExistingSegments)
  {
    return false;
  }

  @Nullable
  @Override
  public Granularity getSegmentGranularity(Interval interval)
  {
    return null;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    final NavigableMap<DateTime, List<TaskLock>> taskLockMap = getTaskLockMap(toolbox.getTaskActionClient());
    // List unused segments
    final List<DataSegment> unusedSegments = toolbox
        .getTaskActionClient()
        .submit(new SegmentListUnusedAction(getDataSource(), getInterval()));

 //    log.info("segments to kill: %s", unusedSegments);
 //    log.info("taskLockMap: %s", taskLockMap);

    if (!TaskActionPreconditions.isLockCoversSegments(taskLockMap, unusedSegments)) {
      throw new ISE(
          "Locks[%s] for task[%s] can't cover segments[%s]",
          taskLockMap.values().stream().flatMap(List::stream).collect(Collectors.toList()),
          getId(),
          unusedSegments
      );
    }

    // Kill segments
    for (DataSegment segment : unusedSegments) {
      toolbox.getDataSegmentKiller().kill(segment);
      toolbox.getTaskActionClient().submit(new SegmentNukeAction(ImmutableSet.of(segment)));
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

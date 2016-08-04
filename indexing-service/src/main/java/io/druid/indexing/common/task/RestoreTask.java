/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SetLockCriticalStateAction;
import io.druid.indexing.common.actions.SegmentListUnusedAction;
import io.druid.indexing.common.actions.SegmentMetadataUpdateAction;
import io.druid.indexing.common.actions.TaskLockCriticalState;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class RestoreTask extends AbstractFixedIntervalTask
{
  private static final Logger log = new Logger(RestoreTask.class);

  public RestoreTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        makeId(id, "restore", dataSource, interval),
        dataSource,
        interval,
        context
    );
  }

  @Override
  public String getType()
  {
    return "restore";
  }

  @Override
    public TaskStatus run(TaskToolbox toolbox) throws Exception
    {
      final TaskLock myLock;
      // Confirm we have a lock and it has not been revoked by a higher priority task
      try {
        myLock = Iterables.getOnlyElement(getTaskLocks(toolbox));
      } catch (NoSuchElementException e) {
        throw new ISE("No valid lock found, dying now !! Is there a higher priority task running that may have revoked this lock ?");
      }

      if (!myLock.getDataSource().equals(getDataSource())) {
        throw new ISE("WTF?! Lock dataSource[%s] != task dataSource[%s]", myLock.getDataSource(), getDataSource());
      }

      if (!myLock.getInterval().equals(getInterval())) {
        throw new ISE("WTF?! Lock interval[%s] != task interval[%s]", myLock.getInterval(), getInterval());
      }

      // List unused segments
      final List<DataSegment> unusedSegments = toolbox
          .getTaskActionClient()
          .submit(new SegmentListUnusedAction(myLock.getDataSource(), myLock.getInterval()));

      // Verify none of these segments have versions > lock version
      for (final DataSegment unusedSegment : unusedSegments) {
        if (unusedSegment.getVersion().compareTo(myLock.getVersion()) > 0) {
          throw new ISE(
              "WTF?! Unused segment[%s] has version[%s] > task version[%s]",
              unusedSegment.getIdentifier(),
              unusedSegment.getVersion(),
              myLock.getVersion()
          );
        }

        log.info("OK to restore segment: %s", unusedSegment.getIdentifier());
      }

      int counter = 0;
      for (DataSegment segment : unusedSegments) {
        if (counter % getBatchSize() == 0) {
          // SetLockCriticalStateAction is idempotent
          if (!toolbox.getTaskActionClient().submit(new SetLockCriticalStateAction(getInterval(), TaskLockCriticalState.DOWNGRADE))) {
            throw new ISE(
                "Lock downgrade failed for interval [%s] !! Successfully restored [%s] segments out of [%s] before failing",
                getInterval(),
                counter,
                unusedSegments.size()
            );
          }

          // Try to upgrade the lock again - we will be successful if no other higher priority task needs to lock
          if (!toolbox.getTaskActionClient().submit(new SetLockCriticalStateAction(getInterval(), TaskLockCriticalState.UPGRADE))) {
            throw new ISE(
                "Lock upgrade failed for interval [%s] !! Successfully archived [%s] segments out of [%s] before failing",
                getInterval(),
                counter,
                unusedSegments.size()
            );
          }
        }
        DataSegment restoredSegment = toolbox.getDataSegmentArchiver().restore(segment);
        toolbox.getTaskActionClient().submit(new SegmentMetadataUpdateAction(ImmutableSet.of(restoredSegment)));
        counter++;
      }
      return TaskStatus.success(getId());
    }
}

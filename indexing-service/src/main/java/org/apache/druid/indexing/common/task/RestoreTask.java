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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.RetrieveUnusedSegmentsAction;
import org.apache.druid.indexing.common.actions.SegmentMetadataUpdateAction;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        getOrMakeId(id, "restore", dataSource, interval),
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
    final TaskLock myLock = getAndCheckLock(toolbox);

    // List unused segments
    final List<DataSegment> unusedSegments = toolbox
        .getTaskActionClient()
        .submit(new RetrieveUnusedSegmentsAction(myLock.getDataSource(), myLock.getInterval(), null, null));

    // Verify none of these segments have versions > lock version
    for (final DataSegment unusedSegment : unusedSegments) {
      if (unusedSegment.getVersion().compareTo(myLock.getVersion()) > 0) {
        throw new ISE(
            "Unused segment[%s] has version[%s] > task version[%s]",
            unusedSegment.getId(),
            unusedSegment.getVersion(),
            myLock.getVersion()
        );
      }

      log.info("OK to restore segment: %s", unusedSegment.getId());
    }

    final List<DataSegment> restoredSegments = new ArrayList<>();

    // Move segments
    for (DataSegment segment : unusedSegments) {
      final DataSegment restored = toolbox.getDataSegmentArchiver().restore(segment);
      if (restored != null) {
        restoredSegments.add(restored);
      } else {
        log.info("Segment [%s] did not move, not updating metadata", segment.getId());
      }
    }

    if (restoredSegments.isEmpty()) {
      log.info("No segments restored");
    } else {
      // Update metadata for moved segments
      toolbox.getTaskActionClient().submit(
          new SegmentMetadataUpdateAction(
              ImmutableSet.copyOf(restoredSegments)
          )
      );
    }

    return TaskStatus.success(getId());
  }
}

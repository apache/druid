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

package org.apache.druid.server.coordination;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class BatchDataSegmentAnnouncer implements DataSegmentAnnouncer
{
  private static final Logger log = new Logger(BatchDataSegmentAnnouncer.class);

  private final Set<DataSegment> announcedSegments = ConcurrentHashMap.newKeySet();

  private final ChangeRequestHistory<DataSegmentChangeRequest> changes = new ChangeRequestHistory<>();

  private final ConcurrentMap<String, SegmentSchemas> taskSinkSchema = new ConcurrentHashMap<>();

  @LifecycleStop
  public void stop()
  {
    changes.stop();
  }

  @Override
  public void announceSegment(DataSegment segment)
  {
    if (!announcedSegments.add(segment)) {
      log.info("Skipping announcement of segment [%s]. Announcement exists already.", segment.getId());
      return;
    }
    changes.addChangeRequest(new SegmentChangeRequestLoad(segment));
  }

  @Override
  public void unannounceSegment(DataSegment segment)
  {
    if (!announcedSegments.remove(segment)) {
      log.warn("No announcement to remove for segment[%s]", segment.getId());
      return;
    }
    changes.addChangeRequest(new SegmentChangeRequestDrop(segment));
  }

  @Override
  public void announceSegments(Iterable<DataSegment> segments)
  {
    List<DataSegmentChangeRequest> changesBatch = new ArrayList<>();
    for (DataSegment segment : segments) {
      if (announcedSegments.add(segment)) {
        changesBatch.add(new SegmentChangeRequestLoad(segment));
      } else {
        log.info("Skipping announcement of segment [%s]. Announcement exists already.", segment.getId());
      }
    }
    if (!changesBatch.isEmpty()) {
      changes.addChangeRequests(changesBatch);
    }
  }

  @Override
  public void unannounceSegments(Iterable<DataSegment> segments)
  {
    for (DataSegment segment : segments) {
      unannounceSegment(segment);
    }
  }

  @Override
  public void announceSegmentSchemas(
      String taskId,
      SegmentSchemas segmentSchemas,
      @Nullable SegmentSchemas segmentSchemasChange
  )
  {
    log.info("Announcing sink schema for task [%s], absolute schema [%s], delta schema [%s].",
             taskId, segmentSchemas, segmentSchemasChange
    );

    taskSinkSchema.put(taskId, segmentSchemas);

    if (segmentSchemasChange != null) {
      changes.addChangeRequest(new SegmentSchemasChangeRequest(segmentSchemasChange));
    }
  }

  @Override
  public void removeSegmentSchemasForTask(String taskId)
  {
    log.info("Unannouncing task [%s].", taskId);
    taskSinkSchema.remove(taskId);
  }

  /**
   * Returns Future that lists the segment load/drop requests since given counter.
   */
  public ListenableFuture<ChangeRequestsSnapshot<DataSegmentChangeRequest>> getSegmentChangesSince(ChangeRequestHistory.Counter counter)
  {
    if (counter.getCounter() < 0) {
      Iterable<DataSegmentChangeRequest> segments = Iterables.transform(
          announcedSegments,
          SegmentChangeRequestLoad::new
      );

      Iterable<DataSegmentChangeRequest> sinkSchema = Iterables.transform(
          taskSinkSchema.values(),
          SegmentSchemasChangeRequest::new
      );
      Iterable<DataSegmentChangeRequest> changeRequestIterables = Iterables.concat(segments, sinkSchema);
      SettableFuture<ChangeRequestsSnapshot<DataSegmentChangeRequest>> future = SettableFuture.create();
      future.set(ChangeRequestsSnapshot.success(changes.getLastCounter(), Lists.newArrayList(changeRequestIterables)));
      return future;
    } else {
      return changes.getRequestsSince(counter);
    }
  }
}

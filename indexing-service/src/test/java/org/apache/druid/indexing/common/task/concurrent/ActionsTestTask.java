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

package org.apache.druid.indexing.common.task.concurrent;

import com.google.common.collect.Sets;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.actions.LockReleaseAction;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalReplaceAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test task that can only invoke task actions.
 */
public class ActionsTestTask extends CommandQueueTask
{
  private final TaskActionClient client;
  private final AtomicInteger sequenceId = new AtomicInteger(0);
  private final Map<SegmentId, String> announcedSegmentsToParentSegments = new HashMap<>();

  public ActionsTestTask(String datasource, String groupId, TaskActionClientFactory factory)
  {
    super(datasource, groupId);
    this.client = factory.create(this);
  }

  public TaskLock acquireReplaceLockOn(Interval interval)
  {
    return runAction(new TimeChunkLockTryAcquireAction(TaskLockType.REPLACE, interval));
  }

  public Void releaseLock(Interval interval)
  {
    return runAction(new LockReleaseAction(interval));
  }

  public TaskLock acquireAppendLockOn(Interval interval)
  {
    return runAction(new TimeChunkLockTryAcquireAction(TaskLockType.APPEND, interval));
  }

  public SegmentPublishResult commitReplaceSegments(DataSegment... segments)
  {
    return runAction(
        SegmentTransactionalReplaceAction.create(Sets.newHashSet(segments), null)
    );
  }

  public Map<SegmentId, String> getAnnouncedSegmentsToParentSegments()
  {
    return announcedSegmentsToParentSegments;
  }

  public SegmentPublishResult commitAppendSegments(DataSegment... segments)
  {
    SegmentPublishResult publishResult = runAction(
        SegmentTransactionalAppendAction.forSegments(Sets.newHashSet(segments), null)
    );
    for (DataSegment segment : publishResult.getSegments()) {
      announcedSegmentsToParentSegments.remove(segment.getId());
    }
    return publishResult;
  }

  public SegmentIdWithShardSpec allocateSegmentForTimestamp(DateTime timestamp, Granularity preferredSegmentGranularity)
  {
    SegmentIdWithShardSpec pendingSegment = runAction(
        new SegmentAllocateAction(
            getDataSource(),
            timestamp,
            Granularities.SECOND,
            preferredSegmentGranularity,
            getId() + "__" + sequenceId.getAndIncrement(),
            null,
            false,
            NumberedPartialShardSpec.instance(),
            LockGranularity.TIME_CHUNK,
            TaskLockType.APPEND
        )
    );
    announcedSegmentsToParentSegments.put(pendingSegment.asSegmentId(), pendingSegment.asSegmentId().toString());
    return pendingSegment;
  }

  private <T> T runAction(TaskAction<T> action)
  {
    return execute(() -> client.submit(action));
  }
}

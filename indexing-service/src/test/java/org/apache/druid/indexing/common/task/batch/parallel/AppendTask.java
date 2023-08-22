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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.actions.SurrogateTaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * A test Task which mimics an appending task by having similar interactions with the overlord.
 *
 * Begins running by acquiring an APPEND lock and immediately allocates pending segments.
 *
 * Task ends after publishing these pending segments and relevant metadata entries in a transaction
 */
public class AppendTask extends AbstractTask
{
  private final Interval interval;
  private final Granularity segmentGranularity;
  private final TaskLockType lockType;
  private final int priority;
  private final CountDownLatch readyLatch = new CountDownLatch(1);
  private final CountDownLatch runLatch = new CountDownLatch(1);
  private final CountDownLatch segmentAllocationComplete = new CountDownLatch(1);
  private final CountDownLatch runComplete = new CountDownLatch(1);
  private final CountDownLatch readyComplete = new CountDownLatch(1);

  private final Set<SegmentIdWithShardSpec> pendingSegments = new HashSet<>();

  private TaskToolbox toolbox;
  private final AtomicInteger sequenceId = new AtomicInteger(0);

  public AppendTask(
      String id,
      String dataSource,
      Interval interval,
      Granularity segmentGranularity,
      Integer priority
  )
  {
    super(
        id == null ? StringUtils.format("append_%s_%s", DateTimes.nowUtc(), UUID.randomUUID().toString()) : id,
        dataSource == null ? "none" : dataSource,
        null,
        IngestionMode.APPEND
    );
    this.interval = interval;
    this.segmentGranularity = segmentGranularity;
    this.lockType = TaskLockType.APPEND;
    this.priority = priority == null ? 50 : priority;
  }

  @Override
  public String getType()
  {
    return "replace";
  }

  @Nonnull
  @Override
  public Set<ResourceAction> getInputSourceResources()
  {
    return ImmutableSet.of();
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    readyLatch.await();
    return tryTimeChunkLockSingleInterval(
        new SurrogateTaskActionClient(getId(), taskActionClient),
        interval,
        lockType
    );
  }

  private boolean tryTimeChunkLockSingleInterval(TaskActionClient client, Interval interval, TaskLockType lockType)
      throws IOException
  {
    final TaskLock lock = client.submit(new TimeChunkLockTryAcquireAction(lockType, interval));
    if (lock == null) {
      return false;
    }
    if (lock.isRevoked()) {
      throw new ISE(StringUtils.format("Lock for interval [%s] was revoked.", interval));
    }
    return true;
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
    this.toolbox = toolbox;
    readyComplete.countDown();

    //final Set<SegmentIdWithShardSpec> pendingSegments = allocatePendingSegments(toolbox);

    segmentAllocationComplete.await();

    runLatch.await();

    if (publishSegments(toolbox, convertPendingSegments(pendingSegments))) {
      return TaskStatus.success(getId());
    }
    return TaskStatus.failure(getId(), "Failed to append segments");
  }

  @Override
  public void cleanUp(TaskToolbox toolbox, TaskStatus taskStatus) throws Exception
  {
    super.cleanUp(toolbox, taskStatus);
    runComplete.countDown();
  }

  public SegmentIdWithShardSpec allocateOrGetSegmentForTimestamp(String timestamp)
  {
    final DateTime time = DateTimes.of(timestamp);
    for (SegmentIdWithShardSpec pendingSegment : pendingSegments) {
      if (pendingSegment.getInterval().contains(time)) {
        return pendingSegment;
      }
    }
    return allocateNewSegmentForDate(time);
  }

  private SegmentIdWithShardSpec allocateNewSegmentForDate(DateTime time)
  {
    try {
      SegmentAllocateAction allocateAction = new SegmentAllocateAction(
          getDataSource(),
          time,
          Granularities.NONE,
          segmentGranularity,
          getId() + "_" + sequenceId.getAndIncrement(),
          null,
          false,
          NumberedPartialShardSpec.instance(),
          LockGranularity.TIME_CHUNK,
          lockType
      );
      final SegmentIdWithShardSpec id = toolbox.getTaskActionClient().submit(allocateAction);
      pendingSegments.add(id);
      return id;
    }
    catch (Exception e) {
      return null;
    }

  }

  private Set<DataSegment> convertPendingSegments(Set<SegmentIdWithShardSpec> pendingSegments)
  {
    final Set<DataSegment> segments = new HashSet<>();
    for (SegmentIdWithShardSpec pendingSegment : pendingSegments) {
      final SegmentId id = pendingSegment.asSegmentId();
      segments.add(
          new DataSegment(
              id,
              ImmutableMap.of(id.toString(), id.toString()),
              ImmutableList.of(),
              ImmutableList.of(),
              pendingSegment.getShardSpec(),
              null,
              0,
              0
          )
      );
    }
    return segments;
  }

  private boolean publishSegments(TaskToolbox toolbox, Set<DataSegment> newSegments)
      throws Exception
  {
    final TransactionalSegmentPublisher publisher = (segmentsToBeOverwritten, segmentsToPublish, commitMetadata) ->
        toolbox.getTaskActionClient().submit(
            SegmentTransactionalAppendAction.create(segmentsToPublish, null, null)
        );
    return publisher.publishSegments(
        Collections.emptySet(),
        newSegments,
        Function.identity(),
        null
    ).isSuccess();
  }

  @Override
  public int getPriority()
  {
    return priority;
  }

  public void markReady()
  {
    readyLatch.countDown();
  }

  public void beginPublish()
  {
    runLatch.countDown();
  }

  public void awaitReadyComplete() throws InterruptedException
  {
    readyComplete.await();
  }

  public void completeSegmentAllocation()
  {
    segmentAllocationComplete.countDown();
  }

  public void awaitRunComplete() throws InterruptedException
  {
    runComplete.await();
  }

}

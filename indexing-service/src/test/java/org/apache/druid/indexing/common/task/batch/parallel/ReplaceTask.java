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
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalReplaceAction;
import org.apache.druid.indexing.common.actions.SurrogateTaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

/**
 * A test Task which mimics a replacing task by having similar interactions with the overlord.
 *
 * Begins running by acquiring a REPLACE lock
 *
 * Task ends after publishing a set of core partitions and
 * creating metadata copies for all appended segments published when this lock was held
 */
public class ReplaceTask extends AbstractTask
{
  private final Interval interval;
  private final Granularity segmentGranularity;
  private final TaskLockType lockType;
  private final int priority;
  private final int numCorePartitions;
  private final CountDownLatch readyLatch = new CountDownLatch(1);
  private final CountDownLatch readyComplete = new CountDownLatch(1);
  private final CountDownLatch runLatch = new CountDownLatch(1);
  private final CountDownLatch runComplete = new CountDownLatch(1);
  private String version;

  public ReplaceTask(
      String id,
      String dataSource,
      Interval interval,
      Granularity segmentGranularity,
      Integer priority,
      Integer numCorePartitions
  )
  {
    super(
        id == null ? StringUtils.format("replace_%s_%s", DateTimes.nowUtc(), UUID.randomUUID().toString()) : id,
        dataSource == null ? "none" : dataSource,
        null,
        IngestionMode.REPLACE
    );
    this.interval = interval;
    this.segmentGranularity = segmentGranularity;
    this.lockType = TaskLockType.REPLACE;
    this.priority = priority == null ? 50 : priority;
    this.numCorePartitions = numCorePartitions == null ? 1 : numCorePartitions;
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
  public String setup(TaskToolbox toolbox) throws Exception
  {
    readyLatch.await();
    while (!isReady(toolbox.getTaskActionClient())) {
      Thread.sleep(100);
    }
    return null;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
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
    version = lock.getVersion();
    return true;
  }

  private Set<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient)
      throws IOException
  {
    return ImmutableSet.copyOf(
        taskActionClient.submit(
            new RetrieveUsedSegmentsAction(getDataSource(), null, ImmutableList.of(interval), Segments.ONLY_VISIBLE)
        )
    );
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
    final Set<DataSegment> oldSegments = findSegmentsToLock(toolbox.getTaskActionClient());

    readyComplete.countDown();

    runLatch.await();

    final Set<DataSegment> newSegments = createSegments();
    final SegmentPublishResult publishResult = publishSegments(toolbox, oldSegments, newSegments);
    if (publishResult.isSuccess()) {
      return TaskStatus.success(getId());
    } else {
      return TaskStatus.failure(getId(), publishResult.getErrorMsg());
    }
  }

  @Override
  public void cleanUp(TaskToolbox toolbox, TaskStatus taskStatus) throws Exception
  {
    super.cleanUp(toolbox, taskStatus);
    runComplete.countDown();
  }


  private Set<DataSegment> createSegments()
  {
    final Set<DataSegment> newSegments = new HashSet<>();
    for (int i = 0; i < numCorePartitions; i++) {
      for (Interval subInterval : segmentGranularity.getIterable(interval)) {
        final ShardSpec shardSpec = new NumberedShardSpec(i, numCorePartitions);
        final SegmentId segmentId = SegmentId.of(getDataSource(), subInterval, version, shardSpec);
        newSegments.add(
            new DataSegment(
                segmentId,
                ImmutableMap.of(segmentId.toString(), segmentId.toString()),
                ImmutableList.of(),
                ImmutableList.of(),
                shardSpec,
                null,
                0,
                0
            )
        );
      }
    }
    return newSegments;
  }

  private SegmentPublishResult publishSegments(TaskToolbox toolbox, Set<DataSegment> oldSegments, Set<DataSegment> newSegments)
      throws Exception
  {
    final TransactionalSegmentPublisher publisher = (segmentsToBeOverwritten, segmentsToPublish, commitMetadata) ->
        toolbox.getTaskActionClient().submit(
            SegmentTransactionalReplaceAction.create(segmentsToPublish)
        );
    return publisher.publishSegments(
        oldSegments,
        newSegments,
        Function.identity(),
        null
    );
  }

  @Override
  public int getPriority()
  {
    return priority;
  }

  public void awaitRunComplete() throws InterruptedException
  {
    runComplete.await();
  }

  public void awaitReadyComplete() throws InterruptedException
  {
    readyComplete.await();
  }

  public void markReady()
  {
    readyLatch.countDown();
  }

  public void beginPublish()
  {
    runLatch.countDown();
  }
}

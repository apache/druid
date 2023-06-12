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
import com.fasterxml.jackson.annotation.JsonProperty;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
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
  private final String lockType;
  private final int priority;
  private final int numCorePartitions;
  private final CountDownLatch readyLatch = new CountDownLatch(1);
  private final CountDownLatch readyComplete = new CountDownLatch(1);
  private final CountDownLatch runLatch = new CountDownLatch(1);
  private final CountDownLatch runComplete = new CountDownLatch(1);
  private String version;

  @JsonCreator
  public ReplaceTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        id == null ? StringUtils.format("replace_%s_%s", DateTimes.nowUtc(), UUID.randomUUID().toString()) : id,
        dataSource == null ? "none" : dataSource,
        context,
        IngestionMode.REPLACE
    );
    this.interval = interval;
    this.segmentGranularity = segmentGranularity;
    this.lockType = getContextValue(Tasks.TASK_LOCK_TYPE, "EXCLUSIVE");
    this.priority = getContextValue(Tasks.PRIORITY_KEY, 0);
    this.numCorePartitions = getContextValue("numCorePartitions", 0);
  }

  @Override
  public String getType()
  {
    return "replace";
  }

  @Nonnull
  @JsonIgnore
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
        TaskLockType.valueOf(lockType)
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
    if (publishSegments(toolbox, oldSegments, newSegments)) {
      return TaskStatus.success(getId());
    }
    return TaskStatus.failure(getId(), "Failed to replace segments");
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

  private boolean publishSegments(TaskToolbox toolbox, Set<DataSegment> oldSegments, Set<DataSegment> newSegments)
      throws Exception
  {
    final TransactionalSegmentPublisher publisher = (segmentsToBeOverwritten, segmentsToDrop, segmentsToPublish, commitMetadata) ->
        toolbox.getTaskActionClient().submit(
            SegmentTransactionalReplaceAction.overwriteAction(segmentsToBeOverwritten, segmentsToDrop, segmentsToPublish)
        );
    return publisher.publishSegments(
        oldSegments,
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

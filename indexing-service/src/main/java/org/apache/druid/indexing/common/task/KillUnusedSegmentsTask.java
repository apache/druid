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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.indexing.ClientKillUnusedSegmentsTaskQuery;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.report.KillTaskReport;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.RetrieveUnusedSegmentsAction;
import org.apache.druid.indexing.common.actions.RetrieveUpgradedFromSegmentIdsAction;
import org.apache.druid.indexing.common.actions.RetrieveUpgradedToSegmentIdsAction;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.SegmentNukeAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskLocks;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.actions.UpgradedToSegmentsResponse;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordination.BroadcastDatasourceLoadingSpec;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * <p/>
 * The client representation of this task is {@link ClientKillUnusedSegmentsTaskQuery}.
 * JSON serialization fields of this class must correspond to those of {@link
 * ClientKillUnusedSegmentsTaskQuery}, except for {@link #id} and {@link #context} fields.
 * <p/>
 * The Kill task fetches the set of used segments for the interval and computes the set of their load specs. <br/>
 * Until `limit` segments have been processed in total or all segments for the interval have been nuked:
 * <ol>
 * <li> Fetch at most `batchSize` unused segments from the metadata store. </li>
 * <li> Determine the mapping from these segments to their parents *before* nuking the segments. </li>
 * <li> Nuke the batch of unused segments from the metadata store. </li>
 * <li> Determine the mapping of the set of parents to all their children. </li>
 * <li> Check if unused or parent segments exist. </li>
 * <li> Find the unreferenced segments. </li>
 * <li> Filter the set of unreferenced segments using load specs from the set of used segments. </li>
 * <li> Kill the filtered set of segments from deep storage. </li>
 * </ol>
 */
public class KillUnusedSegmentsTask extends AbstractFixedIntervalTask
{
  public static final String TYPE = "kill";
  private static final Logger LOG = new Logger(KillUnusedSegmentsTask.class);

  /**
   * Default nuke batch size. This is a small enough size that we still get value from batching, while
   * yielding as quickly as possible. In one real cluster environment backed with mysql, ~2000rows/sec,
   * with batch size of 100, means a batch should only less than a second for the task lock, and depending
   * on the segment store latency, unoptimised S3 cleanups typically take 5-10 seconds per 100. Over time,
   * we expect the S3 cleanup to get quicker, so this should be < 1 second, which means we'll be yielding
   * the task lockbox every 1-2 seconds.
   */
  private static final int DEFAULT_SEGMENT_NUKE_BATCH_SIZE = 100;

  /**
   * The version of segments to delete in this {@link #getInterval()}.
   */
  @Nullable
  private final List<String> versions;

  /**
   * Split processing to try and keep each nuke operation relatively short, in the case that either
   * the database or the storage layer is particularly slow.
   */
  private final int batchSize;

  /**
   * Maximum number of segments that can be killed.
   */
  @Nullable
  private final Integer limit;

  /**
   * The maximum used status last updated time. Any segments with
   * {@code used_status_last_updated} no later than this time will be included in the kill task.
   */
  @Nullable
  private final DateTime maxUsedStatusLastUpdatedTime;

  @JsonCreator
  public KillUnusedSegmentsTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("versions") @Nullable List<String> versions,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("batchSize") Integer batchSize,
      @JsonProperty("limit") @Nullable Integer limit,
      @JsonProperty("maxUsedStatusLastUpdatedTime") @Nullable DateTime maxUsedStatusLastUpdatedTime
  )
  {
    super(
        getOrMakeId(id, "kill", dataSource, interval),
        dataSource,
        interval,
        context
    );
    this.batchSize = (batchSize != null) ? batchSize : DEFAULT_SEGMENT_NUKE_BATCH_SIZE;
    if (this.batchSize <= 0) {
      throw InvalidInput.exception("batchSize[%d] must be a positive integer.", batchSize);
    }
    if (limit != null && limit <= 0) {
      throw InvalidInput.exception("limit[%d] must be a positive integer.", limit);
    }
    this.versions = versions;
    this.limit = limit;
    this.maxUsedStatusLastUpdatedTime = maxUsedStatusLastUpdatedTime;
  }


  @Nullable
  @JsonProperty
  public List<String> getVersions()
  {
    return versions;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getBatchSize()
  {
    return batchSize;
  }

  @Nullable
  @JsonProperty
  public Integer getLimit()
  {
    return limit;
  }

  @Nullable
  @JsonProperty
  public DateTime getMaxUsedStatusLastUpdatedTime()
  {
    return maxUsedStatusLastUpdatedTime;
  }

  @Override
  public String getType()
  {
    return TYPE;
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
    // Track stats for reporting
    int numSegmentsKilled = 0;
    int numBatchesProcessed = 0;

    // List unused segments
    int nextBatchSize = computeNextBatchSize(numSegmentsKilled);
    @Nullable Integer numTotalBatches = getNumTotalBatches();
    List<DataSegment> unusedSegments;
    LOG.info(
        "Starting kill for datasource[%s] in interval[%s] and versions[%s] with batchSize[%d], up to limit[%d]"
        + " segments before maxUsedStatusLastUpdatedTime[%s] will be deleted%s",
        getDataSource(), getInterval(), getVersions(), batchSize, limit, maxUsedStatusLastUpdatedTime,
        numTotalBatches != null ? StringUtils.format(" in [%d] batches.", numTotalBatches) : "."
    );

    final TaskActionClient taskActionClient = toolbox.getTaskActionClient();
    RetrieveUsedSegmentsAction retrieveUsedSegmentsAction = new RetrieveUsedSegmentsAction(
            getDataSource(),
            ImmutableList.of(getInterval()),
            Segments.INCLUDING_OVERSHADOWED
    );
    // Fetch the load specs of all segments overlapping with the unused segment intervals
    final Set<Map<String, Object>> usedSegmentLoadSpecs = taskActionClient.submit(retrieveUsedSegmentsAction)
                                                                          .stream()
                                                                          .map(DataSegment::getLoadSpec)
                                                                          .collect(Collectors.toSet());

    do {
      if (nextBatchSize <= 0) {
        break;
      }

      unusedSegments = toolbox.getTaskActionClient().submit(
          new RetrieveUnusedSegmentsAction(getDataSource(), getInterval(), getVersions(), nextBatchSize, maxUsedStatusLastUpdatedTime)
      );

      // Fetch locks each time as a revokal could have occurred in between batches
      final NavigableMap<DateTime, List<TaskLock>> taskLockMap
              = getNonRevokedTaskLockMap(toolbox.getTaskActionClient());

      if (!TaskLocks.isLockCoversSegments(taskLockMap, unusedSegments)) {
        throw new ISE(
                "Locks[%s] for task[%s] can't cover segments[%s]",
                taskLockMap.values().stream().flatMap(List::stream).collect(Collectors.toList()),
                getId(),
                unusedSegments
        );
      }

      // Kill segments. Order is important here:
      // Retrieve the segment upgrade infos for the batch _before_ the segments are nuked
      // We then want the nuke action to clean up the metadata records _before_ the segments are removed from storage.
      // This helps maintain that we will always have a storage segment if the metadata segment is present.
      // Determine the subset of segments to be killed from deep storage based on loadspecs.
      // If the segment nuke throws an exception, then the segment cleanup is abandoned.

      // Determine upgraded segment ids before nuking
      final Set<String> segmentIds = unusedSegments.stream()
                                                   .map(DataSegment::getId)
                                                   .map(SegmentId::toString)
                                                   .collect(Collectors.toSet());
      final Map<String, String> upgradedFromSegmentIds = new HashMap<>();
      try {
        upgradedFromSegmentIds.putAll(
            taskActionClient.submit(
                new RetrieveUpgradedFromSegmentIdsAction(getDataSource(), segmentIds)
            ).getUpgradedFromSegmentIds()
        );
      }
      catch (Exception e) {
        LOG.warn(
            e,
            "Could not retrieve parent segment ids using task action[retrieveUpgradedFromSegmentIds]."
            + " Overlord may be on an older version."
        );
      }

      // Nuke Segments
      taskActionClient.submit(new SegmentNukeAction(new HashSet<>(unusedSegments)));

      // Determine segments to be killed
      final List<DataSegment> segmentsToBeKilled
          = getKillableSegments(unusedSegments, upgradedFromSegmentIds, usedSegmentLoadSpecs, taskActionClient);

      final Set<DataSegment> segmentsNotKilled = new HashSet<>(unusedSegments);
      segmentsToBeKilled.forEach(segmentsNotKilled::remove);
      LOG.infoSegments(
          segmentsNotKilled,
          "Skipping segment kill from deep storage as their load specs are referenced by other segments."
      );

      toolbox.getDataSegmentKiller().kill(segmentsToBeKilled);
      numBatchesProcessed++;
      numSegmentsKilled += segmentsToBeKilled.size();

      LOG.info("Processed [%d] batches for kill task[%s].", numBatchesProcessed, getId());

      nextBatchSize = computeNextBatchSize(numSegmentsKilled);
    } while (!unusedSegments.isEmpty() && (null == numTotalBatches || numBatchesProcessed < numTotalBatches));

    final String taskId = getId();
    LOG.info(
        "Finished kill task[%s] for dataSource[%s] and interval[%s]."
        + " Deleted total [%d] unused segments in [%d] batches.",
        taskId, getDataSource(), getInterval(), numSegmentsKilled, numBatchesProcessed
    );

    final KillTaskReport.Stats stats =
        new KillTaskReport.Stats(numSegmentsKilled, numBatchesProcessed);
    toolbox.getTaskReportFileWriter().write(
        taskId,
        TaskReport.buildTaskReports(new KillTaskReport(taskId, stats))
    );

    return TaskStatus.success(taskId);
  }

  @JsonIgnore
  @VisibleForTesting
  @Nullable
  Integer getNumTotalBatches()
  {
    return null != limit ? (int) Math.ceil((double) limit / batchSize) : null;
  }

  @JsonIgnore
  @VisibleForTesting
  int computeNextBatchSize(int numSegmentsKilled)
  {
    return null != limit ? Math.min(limit - numSegmentsKilled, batchSize) : batchSize;
  }

  private NavigableMap<DateTime, List<TaskLock>> getNonRevokedTaskLockMap(TaskActionClient client) throws IOException
  {
    final NavigableMap<DateTime, List<TaskLock>> taskLockMap = new TreeMap<>();
    getTaskLocks(client).forEach(
        taskLock -> {
          if (!taskLock.isRevoked()) {
            taskLockMap.computeIfAbsent(taskLock.getInterval().getStart(), k -> new ArrayList<>()).add(taskLock);
          }
        }
    );
    return taskLockMap;
  }

  /**
   * Determines subset of segments without referenced load specs that can be safely killed by
   * looking at the segment upgrades and used segment load specs
   * @param unusedSegments input segments
   * @param upgradedFromSegmentIds segment to parent mapping
   * @param usedSegmentLoadSpecs load specs of used segments
   * @param taskActionClient task action client
   * @return list of segments to kill from deep storage
   */
  private List<DataSegment> getKillableSegments(
      List<DataSegment> unusedSegments,
      Map<String, String> upgradedFromSegmentIds,
      Set<Map<String, Object>> usedSegmentLoadSpecs,
      TaskActionClient taskActionClient
  )
  {

    // Determine parentId for each unused segment
    final Map<String, Set<DataSegment>> parentIdToUnusedSegments = new HashMap<>();
    for (DataSegment segment : unusedSegments) {
      final String segmentId = segment.getId().toString();
      parentIdToUnusedSegments.computeIfAbsent(
          upgradedFromSegmentIds.getOrDefault(segmentId, segmentId),
          k -> new HashSet<>()
      ).add(segment);
    }

    // Check if the parent or any of its children exist in metadata store
    try {
      UpgradedToSegmentsResponse response = taskActionClient.submit(
          new RetrieveUpgradedToSegmentIdsAction(getDataSource(), parentIdToUnusedSegments.keySet())
      );
      if (response != null && response.getUpgradedToSegmentIds() != null) {
        response.getUpgradedToSegmentIds().forEach((parent, children) -> {
          if (!CollectionUtils.isNullOrEmpty(children)) {
            // Do not kill segment if its parent or any of its siblings still exist in metadata store
            parentIdToUnusedSegments.remove(parent);
          }
        });
      }
    }
    catch (Exception e) {
      LOG.warn(
          e,
          "Could not retrieve referenced ids using task action[retrieveUpgradedToSegmentIds]."
          + " Overlord may be on an older version."
      );
    }

    // Filter using the used segment load specs as segment upgrades predate the above task action
    return parentIdToUnusedSegments.values()
                                   .stream()
                                   .flatMap(Set::stream)
                                   .filter(segment -> !usedSegmentLoadSpecs.contains(segment.getLoadSpec()))
                                   .collect(Collectors.toList());
  }


  @Override
  public LookupLoadingSpec getLookupLoadingSpec()
  {
    return LookupLoadingSpec.NONE;
  }

  @Override
  public BroadcastDatasourceLoadingSpec getBroadcastDatasourceLoadingSpec()
  {
    return BroadcastDatasourceLoadingSpec.NONE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    final boolean useConcurrentLocks = Boolean.TRUE.equals(
        getContextValue(
            Tasks.USE_CONCURRENT_LOCKS,
            Tasks.DEFAULT_USE_CONCURRENT_LOCKS
        )
    );

    TaskLockType actualLockType = determineLockType(useConcurrentLocks);

    final TaskLock lock = taskActionClient.submit(
        new TimeChunkLockTryAcquireAction(
            actualLockType,
            getInterval()
        )
    );
    if (lock == null) {
      return false;
    }
    lock.assertNotRevoked();
    return true;
  }

  private TaskLockType determineLockType(boolean useConcurrentLocks)
  {
    TaskLockType actualLockType;
    if (useConcurrentLocks) {
      actualLockType = TaskLockType.REPLACE;
    } else {
      actualLockType = getContextValue(Tasks.TASK_LOCK_TYPE, TaskLockType.EXCLUSIVE);
    }
    return actualLockType;
  }

}

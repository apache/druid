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
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.timeline.DataSegment;
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
    List<DataSegmentPlus> unusedSegmentsPlus;
    logInfo(
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

      unusedSegmentsPlus = fetchNextBatchOfUnusedSegments(toolbox, nextBatchSize);
      if (unusedSegmentsPlus.isEmpty()) {
        // No more segments eligible for kill, do not proceed further
        break;
      }

      // Fetch locks each time as a revokal could have occurred in between batches
      final NavigableMap<DateTime, List<TaskLock>> taskLockMap
              = getNonRevokedTaskLockMap(toolbox.getTaskActionClient());

      final Set<DataSegment> unusedSegments = unusedSegmentsPlus.stream()
                                                                 .map(DataSegmentPlus::getDataSegment)
                                                                 .collect(Collectors.toSet());

      if (!TaskLocks.isLockCoversSegments(taskLockMap, unusedSegments)) {
        throw new ISE(
                "Locks[%s] for task[%s] can't cover segments[%s]",
                taskLockMap.values().stream().flatMap(List::stream).collect(Collectors.toList()),
                getId(),
                unusedSegments
        );
      }

      // Kill segments - order of steps 1, 2, 3, 4 must remain the same

      // 1. Determine parent segment ids of killable unused segments
      final Map<String, String> upgradedFromSegmentIds
          = fetchParentIdsForSegments(toolbox, unusedSegmentsPlus);

      // 2. Identify killable segments whose load specs are not shared with any other segment
      final List<DataSegment> segmentsToKillFromDeepStore = getKillableSegments(
          unusedSegments,
          upgradedFromSegmentIds,
          usedSegmentLoadSpecs,
          taskActionClient
      );

      // 2a. Track segments that cannot be removed from deep store yet
      final Set<DataSegment> segmentsNotKilled = new HashSet<>(unusedSegments);
      segmentsToKillFromDeepStore.forEach(segmentsNotKilled::remove);
      if (!segmentsNotKilled.isEmpty()) {
        LOG.warn(
            "Skipping kill of [%d] segments of datasource[%s] from deep storage"
            + " as their load specs are shared by other segments.",
            segmentsNotKilled.size(), getDataSource()
        );
      }
      if (segmentsToKillFromDeepStore.isEmpty()) {
        // Do not proceed further as we will always keep getting the same batch of segments
        break;
      }

      // 3. Nuke all eligible unused segments
      taskActionClient.submit(new SegmentNukeAction(unusedSegments));
      emitMetric(toolbox.getEmitter(), TaskMetrics.SEGMENTS_DELETED_FROM_METADATA_STORE, unusedSegments.size());

      // 4. Delete deep store files only for segments which do not share load specs with other segments
      toolbox.getDataSegmentKiller().kill(segmentsToKillFromDeepStore);
      emitMetric(toolbox.getEmitter(), TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, segmentsToKillFromDeepStore.size());

      numBatchesProcessed++;
      numSegmentsKilled += segmentsToKillFromDeepStore.size();

      logInfo("Processed [%d] batches for kill task[%s].", numBatchesProcessed, getId());

      nextBatchSize = computeNextBatchSize(numSegmentsKilled);
    } while (!unusedSegmentsPlus.isEmpty() && (null == numTotalBatches || numBatchesProcessed < numTotalBatches));

    final String taskId = getId();
    logInfo(
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
  @Nullable
  protected Integer getNumTotalBatches()
  {
    return null != limit ? (int) Math.ceil((double) limit / batchSize) : null;
  }

  @JsonIgnore
  @VisibleForTesting
  int computeNextBatchSize(int numSegmentsKilled)
  {
    return null != limit ? Math.min(limit - numSegmentsKilled, batchSize) : batchSize;
  }

  /**
   * Fetches the next batch of unused segments that are eligible for kill.
   */
  protected List<DataSegmentPlus> fetchNextBatchOfUnusedSegments(TaskToolbox toolbox, int nextBatchSize) throws IOException
  {
    return toolbox.getTaskActionClient().submit(
        new RetrieveUnusedSegmentsAction(
            getDataSource(),
            getInterval(),
            getVersions(),
            nextBatchSize,
            maxUsedStatusLastUpdatedTime
        )
    )
                  .stream()
                  .map(segment -> new DataSegmentPlus(segment, null, null, null, null, null, null, null))
                  .collect(Collectors.toList());
  }

  /**
   * Fetches the parent IDs (if any) for the given unused segments.
   *
   * @param unusedSegments Unused segments whose parent IDs need to be fetched
   * @return Map from segment ID to the segment ID from which
   * it was upgraded. If an input segment was not upgraded from any other segment,
   * it does not have an entry in the map.
   */
  protected Map<String, String> fetchParentIdsForSegments(
      TaskToolbox toolbox,
      List<DataSegmentPlus> unusedSegments
  )
  {
    try {
      final Set<String> segmentIds = unusedSegments.stream().map(
          s -> s.getDataSegment().getId().toString()
      ).collect(Collectors.toSet());

      return toolbox.getTaskActionClient().submit(
          new RetrieveUpgradedFromSegmentIdsAction(getDataSource(), segmentIds)
      ).getUpgradedFromSegmentIds();
    }
    catch (Exception e) {
      // Do not proceed with killing these segments as we cannot be sure if their
      // load spec is shared by any other segment or not. If load spec is shared,
      // segment files cannot be deleted from deep store. If load spec is not
      // shared, segments cannot be deleted from metadata store as that would
      // leave deep store files orphaned, and they would never be cleaned up.
      throw new ISE(
          e,
          "Could not retrieve parent segment ids using task action[retrieveUpgradedFromSegmentIds]."
          + " Stopping kill task to avoid data loss in case the segment files"
          + " are shared by other segments."
      );
    }
  }

  /**
   * Logs the given info message. Exposed here to allow embedded kill tasks to
   * suppress info logs.
   */
  protected void logInfo(String message, Object... args)
  {
    LOG.info(message, args);
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
      Set<DataSegment> unusedSegments,
      Map<String, String> upgradedFromSegmentIds,
      Set<Map<String, Object>> usedSegmentLoadSpecs,
      TaskActionClient taskActionClient
  )
  {
    // Unused segment IDs being killed
    final Set<String> segmentIdsBeingKilled = unusedSegments.stream()
                                                            .map(s -> s.getId().toString())
                                                            .collect(Collectors.toSet());

    // Determine parentId (or self, if no parent) for each unused segment
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
          if (!segmentIdsBeingKilled.containsAll(children)) {
            // Do not kill segment if its load spec is shared by another segment
            // which is not being killed.
            LOG.info(
                "Skipping kill of segments[%s] as its load spec is shared by segment IDs[%s].",
                parentIdToUnusedSegments.get(parent), children
            );
            parentIdToUnusedSegments.remove(parent);
          }
        });
      }
    }
    catch (Exception e) {
      // Do not proceed with the kill of any segment as we cannot be sure if their
      // load specs are shared by any other segment
      throw new ISE(
          e,
          "Could not perform task action[retrieveUpgradedToSegmentIds] to retrieve"
          + " segment IDs which share load specs with segments being killed."
          + " Stopping kill task to avoid data loss in case the segment files"
          + " are shared by other segments."
      );
    }

    // Filter using the used segment load specs as segment upgrades predate the above task action
    return parentIdToUnusedSegments.values()
                                   .stream()
                                   .flatMap(Set::stream)
                                   .filter(segment -> !isSegmentLoadSpecPresentIn(segment, usedSegmentLoadSpecs))
                                   .collect(Collectors.toList());
  }

  /**
   * @return true if the load spec of the segment is present in the given set of
   * used load specs.
   */
  private boolean isSegmentLoadSpecPresentIn(
      DataSegment segment,
      Set<Map<String, Object>> usedSegmentLoadSpecs
  )
  {
    boolean isPresent = usedSegmentLoadSpecs.contains(segment.getLoadSpec());
    if (isPresent) {
      LOG.info("Skipping kill of segment[%s] as its load spec is shared by other 'used' segments.", segment);
    }
    return isPresent;
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

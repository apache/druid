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

package org.apache.druid.indexing.worker;

import com.google.common.collect.Iterators;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.TaskStatus;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This class manages intermediary segments for data shuffle between native parallel index tasks.
 * In native parallel indexing, phase 1 tasks store segment files in local storage of middleManagers
 * and phase 2 tasks read those files via HTTP.
 * <p>
 * The directory where segment files are placed is structured as
 * {@link StorageLocation#path}/supervisorTaskId/startTimeOfSegment/endTimeOfSegment/partitionIdOfSegment.
 * <p>
 * This class provides interfaces to store, find, and remove segment files.
 * It also has a self-cleanup mechanism to clean up stale segment files. It periodically checks the last access time
 * per supervisorTask and removes its all segment files if the supervisorTask is not running anymore.
 */
@ManageLifecycle
public class IntermediaryDataManager
{
  private static final Logger log = new Logger(IntermediaryDataManager.class);

  private final long intermediaryPartitionDiscoveryPeriodSec;
  private final long intermediaryPartitionCleanupPeriodSec;
  private final Period intermediaryPartitionTimeout;
  private final List<StorageLocation> shuffleDataLocations;
  private final IndexingServiceClient indexingServiceClient;

  // supervisorTaskId -> time to check supervisorTask status
  // This time is initialized when a new supervisorTask is found and updated whenever a partition is accessed for
  // the supervisor.
  private final ConcurrentHashMap<String, DateTime> supervisorTaskCheckTimes = new ConcurrentHashMap<>();

  // supervisorTaskId -> cyclic iterator of storage locations
  private final Map<String, Iterator<StorageLocation>> locationIterators = new HashMap<>();

  // The overlord is supposed to send a cleanup request as soon as the supervisorTask is finished in parallel indexing,
  // but middleManager or indexer could miss the request. This executor is to automatically clean up unused intermediary
  // partitions.
  // This can be null until IntermediaryDataManager is started.
  @Nullable
  private ScheduledExecutorService supervisorTaskChecker;

  @Inject
  public IntermediaryDataManager(
      WorkerConfig workerConfig,
      TaskConfig taskConfig,
      IndexingServiceClient indexingServiceClient
  )
  {
    this.intermediaryPartitionDiscoveryPeriodSec = workerConfig.getIntermediaryPartitionDiscoveryPeriodSec();
    this.intermediaryPartitionCleanupPeriodSec = workerConfig.getIntermediaryPartitionCleanupPeriodSec();
    this.intermediaryPartitionTimeout = workerConfig.getIntermediaryPartitionTimeout();
    this.shuffleDataLocations = taskConfig
        .getShuffleDataLocations()
        .stream()
        .map(config -> new StorageLocation(config.getPath(), config.getMaxSize(), config.getFreeSpacePercent()))
        .collect(Collectors.toList());
    this.indexingServiceClient = indexingServiceClient;
  }

  @LifecycleStart
  public void start()
  {
    supervisorTaskChecker = Execs.scheduledSingleThreaded("intermediary-data-manager-%d");
    // Discover partitions for new supervisorTasks
    supervisorTaskChecker.scheduleAtFixedRate(
        () -> {
          try {
            discoverSupervisorTaskPartitions();
          }
          catch (Exception e) {
            log.warn(e, "Error while discovering supervisorTasks");
          }
        },
        intermediaryPartitionDiscoveryPeriodSec,
        intermediaryPartitionDiscoveryPeriodSec,
        TimeUnit.SECONDS
    );

    supervisorTaskChecker.scheduleAtFixedRate(
        () -> {
          try {
            deleteExpiredSuprevisorTaskPartitionsIfNotRunning();
          }
          catch (InterruptedException e) {
            log.error(e, "Error while cleaning up partitions for expired supervisors");
          }
          catch (Exception e) {
            log.warn(e, "Error while cleaning up partitions for expired supervisors");
          }
        },
        intermediaryPartitionCleanupPeriodSec,
        intermediaryPartitionCleanupPeriodSec,
        TimeUnit.SECONDS
    );
  }

  @LifecycleStop
  public void stop() throws InterruptedException
  {
    if (supervisorTaskChecker != null) {
      supervisorTaskChecker.shutdownNow();
      supervisorTaskChecker.awaitTermination(10, TimeUnit.SECONDS);
    }
    supervisorTaskCheckTimes.clear();
  }

  private void discoverSupervisorTaskPartitions()
  {
    for (StorageLocation location : shuffleDataLocations) {
      final MutableInt numDiscovered = new MutableInt(0);
      final File[] dirsPerSupervisorTask = location.getPath().listFiles();
      if (dirsPerSupervisorTask != null) {
        for (File supervisorTaskDir : dirsPerSupervisorTask) {
          final String supervisorTaskId = supervisorTaskDir.getName();
          supervisorTaskCheckTimes.computeIfAbsent(
              supervisorTaskId,
              k -> {
                numDiscovered.increment();
                return DateTimes.nowUtc().plus(intermediaryPartitionTimeout);
              }
          );
        }
      }
      log.info("Discovered partitions for [%s] new supervisor tasks", numDiscovered.getValue());
    }
  }

  /**
   * Check supervisorTask status if its partitions have not been accessed in timeout and
   * delete all partitions for the supervisorTask if it is already finished.
   * <p>
   * Note that the overlord sends a cleanup request when a supervisorTask is finished. The below check is to trigger
   * the self-cleanup for when the cleanup request is missing.
   */
  private void deleteExpiredSuprevisorTaskPartitionsIfNotRunning() throws InterruptedException
  {
    final DateTime now = DateTimes.nowUtc();
    final Set<String> expiredSupervisorTasks = new HashSet<>();
    for (Entry<String, DateTime> entry : supervisorTaskCheckTimes.entrySet()) {
      final String supervisorTaskId = entry.getKey();
      final DateTime checkTime = entry.getValue();
      if (checkTime.isAfter(now)) {
        expiredSupervisorTasks.add(supervisorTaskId);
      }
    }

    log.info("Found [%s] expired supervisor tasks", expiredSupervisorTasks.size());

    final Map<String, TaskStatus> taskStatuses = indexingServiceClient.getTaskStatuses(expiredSupervisorTasks);
    for (Entry<String, TaskStatus> entry : taskStatuses.entrySet()) {
      final String supervisorTaskId = entry.getKey();
      final TaskStatus status = entry.getValue();
      if (status.getStatusCode().isComplete()) {
        // If it's finished, clean up all partitions for the supervisor task.
        try {
          deletePartitions(supervisorTaskId);
        }
        catch (IOException e) {
          log.warn(e, "Failed to delete partitions for task[%s]", supervisorTaskId);
        }
      } else {
        // If it's still running, update last access time.
        supervisorTaskCheckTimes.put(supervisorTaskId, DateTimes.nowUtc());
      }
    }
  }

  /**
   * Write a segment into one of configured locations. The location to write is chosen in a round-robin manner per
   * supervisorTaskId.
   * <p>
   * This method is only useful for the new Indexer model. Tasks running in the existing middleManager should the static
   * addSegment method.
   */
  public void addSegment(String supervisorTaskId, String subTaskId, DataSegment segment, File segmentFile)
  {
    final Iterator<StorageLocation> iterator = locationIterators.computeIfAbsent(
        supervisorTaskId,
        k -> Iterators.cycle(shuffleDataLocations)
    );
    addSegment(iterator, shuffleDataLocations.size(), supervisorTaskId, subTaskId, segment, segmentFile);
  }

  public List<File> findPartitionFiles(String supervisorTaskId, Interval interval, int partitionId)
  {
    for (StorageLocation location : shuffleDataLocations) {
      final File partitionDir = getPartitionDir(location, supervisorTaskId, interval, partitionId);
      if (partitionDir.exists()) {
        supervisorTaskCheckTimes.put(supervisorTaskId, DateTimes.nowUtc());
        final File[] segmentFiles = partitionDir.listFiles();
        return segmentFiles == null ? Collections.emptyList() : Arrays.asList(segmentFiles);
      }
    }

    return Collections.emptyList();
  }

  public void deletePartitions(String supervisorTaskId) throws IOException
  {
    for (StorageLocation location : shuffleDataLocations) {
      final File supervisorTaskPath = new File(location.getPath(), supervisorTaskId);
      if (supervisorTaskPath.exists()) {
        log.info("Cleaning up [%s]", supervisorTaskPath);
        for (File eachFile : FileUtils.listFiles(supervisorTaskPath, null, true)) {
          location.removeFile(eachFile);
        }
        FileUtils.forceDelete(supervisorTaskPath);
      }
    }
    supervisorTaskCheckTimes.remove(supervisorTaskId);
  }

  /**
   * Iterate through the given storage locations to find one which can handle the given segment.
   */
  public static void addSegment(
      Iterator<StorageLocation> cyclicIterator,
      int numLocations,
      String supervisorTaskId,
      String subTaskId,
      DataSegment segment,
      File segmentFile
  )
  {
    for (int i = 0; i < numLocations; i++) {
      final StorageLocation location = cyclicIterator.next();
      final File destFile = new File(
          getPartitionDir(location, supervisorTaskId, segment.getInterval(), segment.getShardSpec().getPartitionNum()),
          subTaskId
      );
      if (location.reserve(destFile, segment.getId().toString(), segmentFile.length())) {
        try {
          FileUtils.forceMkdirParent(destFile);
          final long copiedBytes = Files.asByteSource(segmentFile).copyTo(Files.asByteSink(destFile));
          if (copiedBytes == 0) {
            throw new IOE(
                "0 bytes copied after copying a segment file from [%s] to [%s]",
                segmentFile.getAbsolutePath(),
                destFile.getAbsolutePath()
            );
          } else {
            return;
          }
        }
        catch (Exception e) {
          log.warn(e, "Failed to write segmentFile at [%s]", destFile);
          location.removeFile(segmentFile);
        }
      }
    }
    throw new ISE("Can't find location to handle segment[%s]", segment);
  }

  private static File getPartitionDir(
      StorageLocation location,
      String supervisorTaskId,
      Interval interval,
      int partitionId
  )
  {
    return FileUtils.getFile(
        location.getPath(),
        supervisorTaskId,
        interval.getStart().toString(),
        interval.getEnd().toString(),
        String.valueOf(partitionId)
    );
  }
}

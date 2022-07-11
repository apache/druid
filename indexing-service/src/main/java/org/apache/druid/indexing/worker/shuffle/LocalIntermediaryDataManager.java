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

package org.apache.druid.indexing.worker.shuffle;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.batch.parallel.GenericPartitionStat;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.apache.druid.utils.CompressionUtils;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * In native parallel indexing, this class store segment files of phase 1 tasks in local storage of middleManagers (or indexer)
 * and phase 2 tasks read those files over HTTP.
 *
 * The directory where segment files are placed is structured as
 * {@link StorageLocation#path}/supervisorTaskId/startTimeOfSegment/endTimeOfSegment/bucketIdOfSegment.
 *
 * This class also has a self-cleanup mechanism to clean up stale segment files. It periodically checks the last access time
 * per supervisorTask and removes its all segment files if the supervisorTask is not running anymore.
 */
@ManageLifecycle
public class LocalIntermediaryDataManager implements IntermediaryDataManager
{
  private static final Logger LOG = new Logger(LocalIntermediaryDataManager.class);

  private final long intermediaryPartitionDiscoveryPeriodSec;
  private final long intermediaryPartitionCleanupPeriodSec;
  private final Period intermediaryPartitionTimeout;
  private final TaskConfig taskConfig;
  private final List<StorageLocation> shuffleDataLocations;
  private final OverlordClient overlordClient;

  // supervisorTaskId -> time to check supervisorTask status
  // This time is initialized when a new supervisorTask is found and updated whenever a partition is accessed for
  // the supervisor.
  private final ConcurrentHashMap<String, DateTime> supervisorTaskCheckTimes = new ConcurrentHashMap<>();

  // supervisorTaskId -> cyclic iterator of storage locations
  private final Map<String, Iterator<StorageLocation>> locationIterators = new HashMap<>();

  // The overlord is supposed to send a cleanup request as soon as the supervisorTask is finished in parallel indexing,
  // but middleManager or indexer could miss the request. This executor is to automatically clean up unused intermediary
  // partitions.
  // This can be null until LocalIntermediaryDataManager is started.
  @MonotonicNonNull
  private ScheduledExecutorService supervisorTaskChecker;

  @Inject
  public LocalIntermediaryDataManager(
      WorkerConfig workerConfig,
      TaskConfig taskConfig,
      OverlordClient overlordClient
  )
  {
    this.intermediaryPartitionDiscoveryPeriodSec = workerConfig.getIntermediaryPartitionDiscoveryPeriodSec();
    this.intermediaryPartitionCleanupPeriodSec = workerConfig.getIntermediaryPartitionCleanupPeriodSec();
    this.intermediaryPartitionTimeout = workerConfig.getIntermediaryPartitionTimeout();
    this.taskConfig = taskConfig;
    this.shuffleDataLocations = taskConfig
        .getShuffleDataLocations()
        .stream()
        .map(config -> new StorageLocation(config.getPath(), config.getMaxSize(), config.getFreeSpacePercent()))
        .collect(Collectors.toList());
    this.overlordClient = overlordClient;
  }

  @Override
  @LifecycleStart
  public void start()
  {
    discoverSupervisorTaskPartitions();
    supervisorTaskChecker = Execs.scheduledSingleThreaded("intermediary-data-manager-%d");
    // Discover partitions for new supervisorTasks
    supervisorTaskChecker.scheduleAtFixedRate(
        () -> {
          try {
            discoverSupervisorTaskPartitions();
          }
          catch (Exception e) {
            LOG.warn(e, "Error while discovering supervisorTasks");
          }
        },
        intermediaryPartitionDiscoveryPeriodSec,
        intermediaryPartitionDiscoveryPeriodSec,
        TimeUnit.SECONDS
    );

    supervisorTaskChecker.scheduleAtFixedRate(
        () -> {
          try {
            deleteExpiredSupervisorTaskPartitionsIfNotRunning();
          }
          catch (Exception e) {
            LOG.warn(e, "Error while cleaning up partitions for expired supervisors");
          }
        },
        intermediaryPartitionCleanupPeriodSec,
        intermediaryPartitionCleanupPeriodSec,
        TimeUnit.SECONDS
    );
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    if (supervisorTaskChecker != null) {
      supervisorTaskChecker.shutdownNow();
      try {
        supervisorTaskChecker.awaitTermination(10, TimeUnit.SECONDS);
      }
      catch (InterruptedException e) {
        Throwables.propagate(e);
      }
    }
    supervisorTaskCheckTimes.clear();
  }

  /**
   * LocalIntermediaryDataManager periodically calls this method after it starts up to search for unknown intermediary data.
   */
  private void discoverSupervisorTaskPartitions()
  {
    for (StorageLocation location : shuffleDataLocations) {
      final Path locationPath = location.getPath().toPath().toAbsolutePath();
      final MutableInt numDiscovered = new MutableInt(0);
      final File[] dirsPerSupervisorTask = location.getPath().listFiles();
      if (dirsPerSupervisorTask != null) {
        for (File supervisorTaskDir : dirsPerSupervisorTask) {
          final String supervisorTaskId = supervisorTaskDir.getName();
          supervisorTaskCheckTimes.computeIfAbsent(
              supervisorTaskId,
              k -> {
                for (File eachFile : org.apache.commons.io.FileUtils.listFiles(supervisorTaskDir, null, true)) {
                  final String relativeSegmentPath = locationPath
                      .relativize(eachFile.toPath().toAbsolutePath())
                      .toString();
                  // StorageLocation keeps track of how much storage capacity is being used.
                  // Newly found files should be known to the StorageLocation to keep it up to date.
                  final File reservedFile = location.reserve(
                      relativeSegmentPath,
                      eachFile.getName(),
                      eachFile.length()
                  );
                  if (reservedFile == null) {
                    LOG.warn("Can't add a discovered partition[%s]", eachFile.getAbsolutePath());
                  }
                }
                numDiscovered.increment();
                return getExpiryTimeFromNow();
              }
          );
        }
      }

      if (numDiscovered.getValue() > 0) {
        LOG.info(
            "Discovered partitions for [%s] new supervisor tasks under location[%s]",
            numDiscovered.getValue(),
            location.getPath()
        );
      }
    }
  }

  /**
   * Check supervisorTask status if its partitions have not been accessed in timeout and
   * delete all partitions for the supervisorTask if it is already finished.
   *
   * Note that the overlord sends a cleanup request when a supervisorTask is finished. The below check is to trigger
   * the self-cleanup for when the cleanup request is missing.
   */
  private void deleteExpiredSupervisorTaskPartitionsIfNotRunning()
  {
    final Set<String> expiredSupervisorTasks = new HashSet<>();
    for (Entry<String, DateTime> entry : supervisorTaskCheckTimes.entrySet()) {
      final String supervisorTaskId = entry.getKey();
      final DateTime checkTime = entry.getValue();
      if (checkTime.isBeforeNow()) {
        expiredSupervisorTasks.add(supervisorTaskId);
      }
    }

    if (!expiredSupervisorTasks.isEmpty()) {
      LOG.info("Found [%s] expired supervisor tasks", expiredSupervisorTasks.size());
    }

    if (!expiredSupervisorTasks.isEmpty()) {
      final Map<String, TaskStatus> taskStatuses =
          FutureUtils.getUnchecked(overlordClient.taskStatuses(expiredSupervisorTasks), true);
      for (Entry<String, TaskStatus> entry : taskStatuses.entrySet()) {
        final String supervisorTaskId = entry.getKey();
        final TaskStatus status = entry.getValue();
        if (status.getStatusCode().isComplete()) {
          // If it's finished, clean up all partitions for the supervisor task.
          try {
            deletePartitions(supervisorTaskId);
          }
          catch (IOException e) {
            LOG.warn(e, "Failed to delete partitions for task[%s]", supervisorTaskId);
          }
        } else {
          // If it's still running, update last access time.
          supervisorTaskCheckTimes.put(supervisorTaskId, getExpiryTimeFromNow());
        }
      }
    }
  }

  /**
   * Write a segment into one of configured locations. The location to write is chosen in a round-robin manner per
   * supervisorTaskId.
   */
  @Override
  public DataSegment addSegment(String supervisorTaskId, String subTaskId, DataSegment segment, File segmentDir)
      throws IOException
  {
    // Get or create the location iterator for supervisorTask.
    final Iterator<StorageLocation> iterator = locationIterators.computeIfAbsent(
        supervisorTaskId,
        k -> {
          final Iterator<StorageLocation> cyclicIterator = Iterators.cycle(shuffleDataLocations);
          // Random start of the iterator
          final int random = ThreadLocalRandom.current().nextInt(shuffleDataLocations.size());
          IntStream.range(0, random).forEach(i -> cyclicIterator.next());
          return cyclicIterator;
        }
    );

    // Create a zipped segment in a temp directory.
    final File taskTempDir = taskConfig.getTaskTempDir(subTaskId);
    final Closer closer = Closer.create();
    closer.register(() -> {
      try {
        org.apache.commons.io.FileUtils.forceDelete(taskTempDir);
      }
      catch (IOException e) {
        LOG.warn(e, "Failed to delete directory[%s]", taskTempDir.getAbsolutePath());
      }
    });

    if (!(segment.getShardSpec() instanceof BucketNumberedShardSpec)) {
      throw new IAE(
          "Invalid shardSpec type. Expected [%s] but got [%s]",
          BucketNumberedShardSpec.class.getName(),
          segment.getShardSpec().getClass().getName()
      );
    }
    final BucketNumberedShardSpec<?> bucketNumberedShardSpec = (BucketNumberedShardSpec<?>) segment.getShardSpec();

    //noinspection unused
    try (final Closer resourceCloser = closer) {
      FileUtils.mkdirp(taskTempDir);

      // Temporary compressed file. Will be removed when taskTempDir is deleted.
      final File tempZippedFile = new File(taskTempDir, segment.getId().toString());
      final long unzippedSizeBytes = CompressionUtils.zip(segmentDir, tempZippedFile);
      if (unzippedSizeBytes == 0) {
        throw new IOE(
            "Read 0 bytes from segmentDir[%s]",
            segmentDir.getAbsolutePath()
        );
      }

      // Try copying the zipped segment to one of storage locations
      for (int i = 0; i < shuffleDataLocations.size(); i++) {
        final StorageLocation location = iterator.next();
        final String partitionFilePath = getPartitionFilePath(
            supervisorTaskId,
            subTaskId,
            segment.getInterval(),
            bucketNumberedShardSpec.getBucketId() // we must use the bucket ID instead of partition ID
        );
        final File destFile = location.reserve(partitionFilePath, segment.getId().toString(), tempZippedFile.length());
        if (destFile != null) {
          try {
            FileUtils.mkdirp(destFile.getParentFile());
            FileUtils.writeAtomically(
                destFile,
                out -> Files.asByteSource(tempZippedFile).copyTo(out)
            );
            LOG.info(
                "Wrote intermediary segment[%s] for subtask[%s] at [%s]",
                segment.getId(),
                subTaskId,
                destFile
            );
            return segment.withSize(unzippedSizeBytes).withBinaryVersion(SegmentUtils.getVersionFromDir(segmentDir));
          }
          catch (Exception e) {
            location.release(partitionFilePath, tempZippedFile.length());
            org.apache.commons.io.FileUtils.deleteQuietly(destFile);
            LOG.warn(
                e,
                "Failed to write segment[%s] at [%s]. Trying again with the next location",
                segment.getId(),
                destFile
            );
          }
        }
      }
      throw new ISE("Can't find location to handle segment[%s]", segment);
    }
  }

  @Override
  public Optional<ByteSource> findPartitionFile(String supervisorTaskId, String subTaskId, Interval interval, int bucketId)
  {
    IdUtils.validateId("supervisorTaskId", supervisorTaskId);
    IdUtils.validateId("subTaskId", subTaskId);
    for (StorageLocation location : shuffleDataLocations) {
      final File partitionDir = new File(location.getPath(), getPartitionDirPath(supervisorTaskId, interval, bucketId));
      if (partitionDir.exists()) {
        supervisorTaskCheckTimes.put(supervisorTaskId, getExpiryTimeFromNow());
        final File segmentFile = new File(partitionDir, subTaskId);
        if (segmentFile.exists()) {
          return Optional.of(Files.asByteSource(segmentFile));
        } else {
          return Optional.empty();
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public GenericPartitionStat generatePartitionStat(TaskToolbox toolbox, DataSegment segment)
  {
    return new GenericPartitionStat(
        toolbox.getTaskExecutorNode().getHost(),
        toolbox.getTaskExecutorNode().getPortToUse(),
        toolbox.getTaskExecutorNode().isEnableTlsPort(),
        segment.getInterval(),
        (BucketNumberedShardSpec) segment.getShardSpec(),
        null, // numRows is not supported yet
        null  // sizeBytes is not supported yet
    );
  }

  private DateTime getExpiryTimeFromNow()
  {
    return DateTimes.nowUtc().plus(intermediaryPartitionTimeout);
  }

  @Override
  public void deletePartitions(String supervisorTaskId) throws IOException
  {
    IdUtils.validateId("supervisorTaskId", supervisorTaskId);
    for (StorageLocation location : shuffleDataLocations) {
      final File supervisorTaskPath = new File(location.getPath(), supervisorTaskId);
      if (supervisorTaskPath.exists()) {
        LOG.info("Cleaning up [%s]", supervisorTaskPath);
        for (File eachFile : org.apache.commons.io.FileUtils.listFiles(supervisorTaskPath, null, true)) {
          location.removeFile(eachFile);
        }
        org.apache.commons.io.FileUtils.forceDelete(supervisorTaskPath);
      }
    }
    supervisorTaskCheckTimes.remove(supervisorTaskId);
  }
}

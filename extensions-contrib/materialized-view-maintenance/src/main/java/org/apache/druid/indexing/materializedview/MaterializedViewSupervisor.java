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

package org.apache.druid.indexing.materializedview;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.HadoopIndexTask;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class MaterializedViewSupervisor implements Supervisor
{
  private static final EmittingLogger log = new EmittingLogger(MaterializedViewSupervisor.class);
  private static final int DEFAULT_MAX_TASK_COUNT = 1;
  // there is a lag between derivatives and base dataSource, to prevent repeatedly building for some delay data. 
  private static final long DEFAULT_MIN_DATA_LAG_MS = TimeUnit.DAYS.toMillis(1);

  private final MetadataSupervisorManager metadataSupervisorManager;
  private final IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private final SqlSegmentsMetadataManager sqlSegmentsMetadataManager;
  private final MaterializedViewSupervisorSpec spec;
  private final TaskMaster taskMaster;
  private final TaskStorage taskStorage;
  private final MaterializedViewTaskConfig config;
  private final SupervisorStateManager stateManager;
  private final String dataSource;
  private final String supervisorId;
  private final int maxTaskCount;
  private final long minDataLagMs;
  private final Map<Interval, HadoopIndexTask> runningTasks = new HashMap<>();
  private final Map<Interval, String> runningVersion = new HashMap<>();
  // taskLock is used to synchronize runningTask and runningVersion
  private final Object taskLock = new Object();
  // stateLock is used to synchronize materializedViewSupervisor's status
  private final Object stateLock = new Object();
  private boolean started = false;
  private ListenableFuture<?> future = null;
  private ListeningScheduledExecutorService exec = null;
  // In the missing intervals, baseDataSource has data but derivedDataSource does not, which means
  // data in these intervals of derivedDataSource needs to be rebuilt.
  private Set<Interval> missInterval = new HashSet<>();

  public MaterializedViewSupervisor(
      TaskMaster taskMaster,
      TaskStorage taskStorage,
      MetadataSupervisorManager metadataSupervisorManager,
      SqlSegmentsMetadataManager sqlSegmentsMetadataManager,
      IndexerMetadataStorageCoordinator metadataStorageCoordinator,
      MaterializedViewTaskConfig config,
      MaterializedViewSupervisorSpec spec
  )
  {
    this.taskMaster = taskMaster;
    this.taskStorage = taskStorage;
    this.metadataStorageCoordinator = metadataStorageCoordinator;
    this.sqlSegmentsMetadataManager = sqlSegmentsMetadataManager;
    this.metadataSupervisorManager = metadataSupervisorManager;
    this.config = config;
    this.spec = spec;
    this.stateManager = new SupervisorStateManager(spec.getSupervisorStateManagerConfig(), spec.isSuspended());
    this.dataSource = spec.getDataSourceName();
    this.supervisorId = StringUtils.format("MaterializedViewSupervisor-%s", dataSource);
    this.maxTaskCount = spec.getContext().containsKey("maxTaskCount")
        ? Integer.parseInt(String.valueOf(spec.getContext().get("maxTaskCount")))
        : DEFAULT_MAX_TASK_COUNT;
    this.minDataLagMs = spec.getContext().containsKey("minDataLagMs")
        ? Long.parseLong(String.valueOf(spec.getContext().get("minDataLagMs")))
        : DEFAULT_MIN_DATA_LAG_MS;
  }

  @Override
  public void start()
  {
    synchronized (stateLock) {
      Preconditions.checkState(!started, "already started");

      DataSourceMetadata metadata = metadataStorageCoordinator.retrieveDataSourceMetadata(dataSource);
      if (null == metadata) {
        metadataStorageCoordinator.insertDataSourceMetadata(
            dataSource,
            new DerivativeDataSourceMetadata(spec.getBaseDataSource(), spec.getDimensions(), spec.getMetrics())
        );
      }
      exec = MoreExecutors.listeningDecorator(Execs.scheduledSingleThreaded(StringUtils.encodeForFormat(supervisorId)));
      final Duration delay = config.getTaskCheckDuration().toStandardDuration();
      future = exec.scheduleWithFixedDelay(
          MaterializedViewSupervisor.this::run,
          0,
          delay.getMillis(),
          TimeUnit.MILLISECONDS
      );
      started = true;
    }
  }

  @VisibleForTesting
  public void run()
  {
    try {
      if (spec.isSuspended()) {
        log.info(
            "Materialized view supervisor[%s:%s] is suspended",
            spec.getId(),
            spec.getDataSourceName()
        );
        return;
      }

      DataSourceMetadata metadata = metadataStorageCoordinator.retrieveDataSourceMetadata(dataSource);
      if (metadata instanceof DerivativeDataSourceMetadata
          && spec.getBaseDataSource().equals(((DerivativeDataSourceMetadata) metadata).getBaseDataSource())
          && spec.getDimensions().equals(((DerivativeDataSourceMetadata) metadata).getDimensions())
          && spec.getMetrics().equals(((DerivativeDataSourceMetadata) metadata).getMetrics())) {
        checkSegmentsAndSubmitTasks();
      } else {
        log.error(
            "Failed to start %s. Metadata in database(%s) is different from new dataSource metadata(%s)",
            supervisorId,
            metadata,
            spec
        );
      }
    }
    catch (Exception e) {
      stateManager.recordThrowableEvent(e);
      log.makeAlert(e, StringUtils.format("uncaught exception in %s.", supervisorId)).emit();
    }
    finally {
      stateManager.markRunFinished();
    }
  }

  @Override
  public void stop(boolean stopGracefully)
  {
    synchronized (stateLock) {
      Preconditions.checkState(started, "not started");

      stateManager.maybeSetState(SupervisorStateManager.BasicState.STOPPING);

      // stop all schedulers and threads
      if (stopGracefully) {
        synchronized (taskLock) {
          future.cancel(false);
          future = null;
          exec.shutdownNow();
          exec = null;
          clearTasks();
          if (!(metadataSupervisorManager.getLatest().get(supervisorId) instanceof MaterializedViewSupervisorSpec)) {
            clearSegments();
          }
        }
      } else {
        future.cancel(true);
        future = null;
        exec.shutdownNow();
        exec = null;
        synchronized (taskLock) {
          clearTasks();
          if (!(metadataSupervisorManager.getLatest().get(supervisorId) instanceof MaterializedViewSupervisorSpec)) {
            clearSegments();
          }
        }
      }
      started = false;
    }

  }

  @Override
  public SupervisorReport getStatus()
  {
    return new MaterializedViewSupervisorReport(
        dataSource,
        DateTimes.nowUtc(),
        spec.isSuspended(),
        spec.getBaseDataSource(),
        spec.getDimensions(),
        spec.getMetrics(),
        JodaUtils.condenseIntervals(missInterval),
        stateManager.isHealthy(),
        stateManager.getSupervisorState().getBasicState(),
        stateManager.getExceptionEvents()
    );
  }

  @Override
  public SupervisorStateManager.State getState()
  {
    return stateManager.getSupervisorState();
  }

  @Override
  public Boolean isHealthy()
  {
    return stateManager.isHealthy();
  }

  @Override
  public void reset(DataSourceMetadata dataSourceMetadata)
  {
    if (dataSourceMetadata == null) {
      // if oldMetadata is different from spec, tasks and segments will be removed when reset.
      DataSourceMetadata oldMetadata = metadataStorageCoordinator.retrieveDataSourceMetadata(dataSource);
      if (oldMetadata instanceof DerivativeDataSourceMetadata) {
        if (!((DerivativeDataSourceMetadata) oldMetadata).getBaseDataSource().equals(spec.getBaseDataSource()) ||
            !((DerivativeDataSourceMetadata) oldMetadata).getDimensions().equals(spec.getDimensions()) ||
            !((DerivativeDataSourceMetadata) oldMetadata).getMetrics().equals(spec.getMetrics())) {
          synchronized (taskLock) {
            clearTasks();
            clearSegments();
          }
        }
      }
      commitDataSourceMetadata(
          new DerivativeDataSourceMetadata(spec.getBaseDataSource(), spec.getDimensions(), spec.getMetrics())
      );
    } else {
      throw new IAE("DerivedDataSourceMetadata is not allowed to reset to a new DerivedDataSourceMetadata");
    }
  }

  @Override
  public void resetOffsets(DataSourceMetadata resetDataSourceMetadata)
  {
    throw new UnsupportedOperationException("Reset offsets not supported in MaterializedViewSupervisor");
  }

  @Override
  public void checkpoint(int taskGroupId, DataSourceMetadata checkpointMetadata)
  {
    // do nothing
  }

  @Override
  public LagStats computeLagStats()
  {
    throw new UnsupportedOperationException("Compute Lag Stats not supported in MaterializedViewSupervisor");
  }

  @Override
  public Set<String> getActiveRealtimeSequencePrefixes()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getActiveTaskGroupsCount()
  {
    throw new UnsupportedOperationException("Get Active Task Groups Count is not supported in MaterializedViewSupervisor");
  }

  /**
   * Find intervals in which derived dataSource should rebuild the segments.
   * Choose the latest intervals to create new HadoopIndexTask and submit it.
   */
  @VisibleForTesting
  void checkSegmentsAndSubmitTasks()
  {
    synchronized (taskLock) {
      List<Interval> intervalsToRemove = new ArrayList<>();
      for (Map.Entry<Interval, HadoopIndexTask> entry : runningTasks.entrySet()) {
        Optional<TaskStatus> taskStatus = taskStorage.getStatus(entry.getValue().getId());
        if (!taskStatus.isPresent() || !taskStatus.get().isRunnable()) {
          intervalsToRemove.add(entry.getKey());
        }
      }
      for (Interval interval : intervalsToRemove) {
        runningTasks.remove(interval);
        runningVersion.remove(interval);
      }

      if (runningTasks.size() == maxTaskCount) {
        //if the number of running tasks reach the max task count, supervisor won't submit new tasks.
        return;
      }
      Pair<SortedMap<Interval, String>, Map<Interval, List<DataSegment>>> toBuildIntervalAndBaseSegments =
          checkSegments();
      SortedMap<Interval, String> sortedToBuildVersion = toBuildIntervalAndBaseSegments.lhs;
      Map<Interval, List<DataSegment>> baseSegments = toBuildIntervalAndBaseSegments.rhs;
      missInterval = sortedToBuildVersion.keySet();
      submitTasks(sortedToBuildVersion, baseSegments);
    }
  }

  @VisibleForTesting
  Pair<Map<Interval, HadoopIndexTask>, Map<Interval, String>> getRunningTasks()
  {
    return new Pair<>(runningTasks, runningVersion);
  }

  /**
   * Find infomation about the intervals in which derived dataSource data should be rebuilt.
   * The infomation includes the version and DataSegments list of a interval.
   * The intervals include: in the interval,
   *  1) baseDataSource has data, but the derivedDataSource does not;
   *  2) version of derived segments isn't the max(created_date) of all base segments;
   *
   *  Drop the segments of the intervals in which derivedDataSource has data, but baseDataSource does not.
   *
   * @return the left part of Pair: interval -> version, and the right part: interval -> DataSegment list.
   *          Version and DataSegment list can be used to create HadoopIndexTask.
   *          Derived datasource data in all these intervals need to be rebuilt.
   */
  @VisibleForTesting
  Pair<SortedMap<Interval, String>, Map<Interval, List<DataSegment>>> checkSegments()
  {
    // Pair<interval -> version, interval -> list<DataSegment>>
    Collection<DataSegment> derivativeSegmentsCollection =
        metadataStorageCoordinator.retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE);
    Pair<Map<Interval, String>, Map<Interval, List<DataSegment>>> derivativeSegmentsSnapshot =
        getVersionAndBaseSegments(derivativeSegmentsCollection);
    // Pair<interval -> max(created_date), interval -> list<DataSegment>>
    Pair<Map<Interval, String>, Map<Interval, List<DataSegment>>> baseSegmentsSnapshot =
        getMaxCreateDateAndBaseSegments(
            metadataStorageCoordinator.retrieveUsedSegmentsAndCreatedDates(spec.getBaseDataSource(),
                                                                           Collections.singletonList(Intervals.ETERNITY))
        );
    // baseSegments are used to create HadoopIndexTask
    Map<Interval, List<DataSegment>> baseSegments = baseSegmentsSnapshot.rhs;
    Map<Interval, List<DataSegment>> derivativeSegments = derivativeSegmentsSnapshot.rhs;
    // use max created_date of base segments as the version of derivative segments
    Map<Interval, String> maxCreatedDate = baseSegmentsSnapshot.lhs;
    Map<Interval, String> derivativeVersion = derivativeSegmentsSnapshot.lhs;
    SortedMap<Interval, String> sortedToBuildInterval =
        new TreeMap<>(Comparators.intervalsByStartThenEnd().reversed());
    // find the intervals to drop and to build
    MapDifference<Interval, String> difference = Maps.difference(maxCreatedDate, derivativeVersion);
    Map<Interval, String> toBuildInterval = new HashMap<>(difference.entriesOnlyOnLeft());
    Map<Interval, String> toDropInterval = new HashMap<>(difference.entriesOnlyOnRight());
    // update version of derived segments if isn't the max (created_date) of all base segments
    // prevent user supplied segments list did not match with segments list obtained from db
    Map<Interval, MapDifference.ValueDifference<String>> checkIfNewestVersion =
            new HashMap<>(difference.entriesDiffering());
    for (Map.Entry<Interval, MapDifference.ValueDifference<String>> entry : checkIfNewestVersion.entrySet()) {
      final String versionOfBase = maxCreatedDate.get(entry.getKey());
      final String versionOfDerivative = derivativeVersion.get(entry.getKey());
      final int baseCount = baseSegments.get(entry.getKey()).size();
      if (versionOfBase.compareTo(versionOfDerivative) > 0) {
        int usedCount = metadataStorageCoordinator
            .retrieveUsedSegmentsForInterval(spec.getBaseDataSource(), entry.getKey(), Segments.ONLY_VISIBLE).size();
        if (baseCount == usedCount) {
          toBuildInterval.put(entry.getKey(), versionOfBase);
        }
      }
    }
    // if some intervals are in running tasks and the versions are the same, remove it from toBuildInterval
    // if some intervals are in running tasks, but the versions are different, stop the task.
    runningVersion.forEach((interval, version) -> {
      if (toBuildInterval.containsKey(interval)) {
        if (toBuildInterval.get(interval).equals(version)) {
          toBuildInterval.remove(interval);
        } else {
          if (taskMaster.getTaskQueue().isPresent()) {
            taskMaster.getTaskQueue().get().shutdown(runningTasks.get(interval).getId(), "version mismatch");
            runningTasks.remove(interval);
          }
        }
      }
    });
    // drop derivative segments which interval equals the interval in toDeleteBaseSegments 
    for (Interval interval : toDropInterval.keySet()) {
      for (DataSegment segment : derivativeSegments.get(interval)) {
        sqlSegmentsMetadataManager.markSegmentAsUnused(segment.getId());
      }
    }
    // data of the latest interval will be built firstly.
    sortedToBuildInterval.putAll(toBuildInterval);
    return new Pair<>(sortedToBuildInterval, baseSegments);
  }

  private void submitTasks(
      SortedMap<Interval, String> sortedToBuildVersion,
      Map<Interval, List<DataSegment>> baseSegments
  )
  {
    for (Map.Entry<Interval, String> entry : sortedToBuildVersion.entrySet()) {
      if (runningTasks.size() < maxTaskCount) {
        HadoopIndexTask task = spec.createTask(entry.getKey(), entry.getValue(), baseSegments.get(entry.getKey()));
        try {
          if (taskMaster.getTaskQueue().isPresent()) {
            taskMaster.getTaskQueue().get().add(task);
            runningVersion.put(entry.getKey(), entry.getValue());
            runningTasks.put(entry.getKey(), task);
          }
        }
        catch (EntryExistsException e) {
          log.error("task %s already exsits", task);
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private Pair<Map<Interval, String>, Map<Interval, List<DataSegment>>> getVersionAndBaseSegments(
      Collection<DataSegment> snapshot
  )
  {
    Map<Interval, String> versions = new HashMap<>();
    Map<Interval, List<DataSegment>> segments = new HashMap<>();
    for (DataSegment segment : snapshot) {
      Interval interval = segment.getInterval();
      versions.put(interval, segment.getVersion());
      segments.computeIfAbsent(interval, i -> new ArrayList<>()).add(segment);
    }
    return new Pair<>(versions, segments);
  }

  private Pair<Map<Interval, String>, Map<Interval, List<DataSegment>>> getMaxCreateDateAndBaseSegments(
      Collection<Pair<DataSegment, String>> snapshot
  )
  {
    Interval maxAllowedToBuildInterval = snapshot.parallelStream()
        .map(pair -> pair.lhs)
        .map(DataSegment::getInterval)
        .max(Comparators.intervalsByStartThenEnd())
        .get();
    Map<Interval, String> maxCreatedDate = new HashMap<>();
    Map<Interval, List<DataSegment>> segments = new HashMap<>();
    for (Pair<DataSegment, String> entry : snapshot) {
      DataSegment segment = entry.lhs;
      String createDate = entry.rhs;
      Interval interval = segment.getInterval();
      if (!hasEnoughLag(interval, maxAllowedToBuildInterval)) {
        continue;
      }
      maxCreatedDate.merge(interval, createDate, (date1, date2) -> {
        return DateTimes.max(DateTimes.of(date1), DateTimes.of(date2)).toString();
      });
      segments.computeIfAbsent(interval, i -> new ArrayList<>()).add(segment);
    }
    return new Pair<>(maxCreatedDate, segments);
  }


  /**
   * check whether the start millis of target interval is more than minDataLagMs lagging behind maxInterval's
   * minDataLag is required to prevent repeatedly building data because of delay data.
   *
   * @param target
   * @param maxInterval
   * @return true if the start millis of target interval is more than minDataLagMs lagging behind maxInterval's
   */
  private boolean hasEnoughLag(Interval target, Interval maxInterval)
  {
    return minDataLagMs <= (maxInterval.getStartMillis() - target.getStartMillis());
  }

  private void clearTasks()
  {
    for (HadoopIndexTask task : runningTasks.values()) {
      if (taskMaster.getTaskQueue().isPresent()) {
        taskMaster.getTaskQueue().get().shutdown(task.getId(), "killing all tasks");
      }
    }
    runningTasks.clear();
    runningVersion.clear();
  }

  private void clearSegments()
  {
    log.info("Clear all metadata of dataSource %s", dataSource);
    metadataStorageCoordinator.deletePendingSegments(dataSource);
    sqlSegmentsMetadataManager.markAsUnusedAllSegmentsInDataSource(dataSource);
    metadataStorageCoordinator.deleteDataSourceMetadata(dataSource);
  }

  private void commitDataSourceMetadata(DataSourceMetadata dataSourceMetadata)
  {
    if (!metadataStorageCoordinator.insertDataSourceMetadata(dataSource, dataSourceMetadata)) {
      try {
        metadataStorageCoordinator.resetDataSourceMetadata(
            dataSource,
            dataSourceMetadata
        );
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

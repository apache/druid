/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.materializedview;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.indexing.common.task.HadoopIndexTask;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.supervisor.Supervisor;
import io.druid.indexing.overlord.supervisor.SupervisorReport;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.JodaUtils;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.metadata.EntryExistsException;
import io.druid.metadata.MetadataSupervisorManager;
import io.druid.metadata.SQLMetadataSegmentManager;
import io.druid.timeline.DataSegment;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class MaterializedViewSupervisor implements Supervisor
{
  private static final EmittingLogger log = new EmittingLogger(MaterializedViewSupervisor.class);
  private static final Interval ALL_INTERVAL = Intervals.of("0000-01-01/3000-01-01");
  private static final int MAX_TASK_COUNT = 1;
  private final MetadataSupervisorManager metadataSupervisorManager;
  private final IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private final SQLMetadataSegmentManager segmentManager;
  private final MaterializedViewSupervisorSpec spec;
  private final TaskMaster taskMaster;
  private final TaskStorage taskStorage;
  private final MaterializedViewTaskConfig config;
  private final String dataSource;
  private final String supervisorId;
  private final int maxTaskCount;
  private final Map<Interval, HadoopIndexTask> runningTasks = Maps.newHashMap();
  private final Object taskLock = new Object();
  private final Object stateLock = new Object();
  private volatile boolean started = false;
  private volatile ListenableFuture<?> future = null;
  private volatile ListeningScheduledExecutorService exec = null;
  private volatile Set<Interval> missInterval = Sets.newHashSet();
  private Map<Interval, String> runningVersion = Maps.newHashMap();
  
  public MaterializedViewSupervisor(
      TaskMaster taskMaster,
      TaskStorage taskStorage,
      MetadataSupervisorManager metadataSupervisorManager,
      SQLMetadataSegmentManager segmentManager,
      IndexerMetadataStorageCoordinator metadataStorageCoordinator,
      MaterializedViewTaskConfig config,
      MaterializedViewSupervisorSpec spec
  )
  {
    this.taskMaster = taskMaster;
    this.taskStorage = taskStorage;
    this.metadataStorageCoordinator = metadataStorageCoordinator;
    this.segmentManager = segmentManager;
    this.metadataSupervisorManager = metadataSupervisorManager;
    this.config = config;
    this.spec = spec;
    this.dataSource = spec.getDataSourceName();
    this.supervisorId = StringUtils.format("MaterializedViewSupervisor-%s", dataSource);
    this.maxTaskCount = spec.getContext().containsKey("maxTaskCount") ? Integer.parseInt(String.valueOf(spec.getContext().get("maxTaskCount"))) : MAX_TASK_COUNT;
  }
  
  @Override
  public void start() 
  {
    synchronized (stateLock) {
      Preconditions.checkState(!started, "already started");
      
      DataSourceMetadata metadata = metadataStorageCoordinator.getDataSourceMetadata(dataSource);
      if (null == metadata) {
        metadataStorageCoordinator.insertDataSourceMetadata(
            dataSource, 
            new DerivativeDataSourceMetadata(spec.getBaseDataSource(), spec.getDimensions(), spec.getMetrics())
        );
      }
      exec = MoreExecutors.listeningDecorator(Execs.scheduledSingleThreaded(supervisorId));
      final Duration delay = config.getTaskCheckDuration().toStandardDuration();
      future = exec.scheduleWithFixedDelay(
          new Runnable() {
            @Override
            public void run()
            {
              try {
                DataSourceMetadata metadata = metadataStorageCoordinator.getDataSourceMetadata(dataSource);
                if (metadata != null 
                    && metadata instanceof DerivativeDataSourceMetadata 
                    && spec.getBaseDataSource().equals(((DerivativeDataSourceMetadata) metadata).getBaseDataSource())
                    && spec.getDimensions().equals(((DerivativeDataSourceMetadata) metadata).getDimensions())
                    && spec.getMetrics().equals(((DerivativeDataSourceMetadata) metadata).getMetrics())) {
                  checkSegmentsAndSubmitTasks();
                } else {
                  log.error("Failed to start %s. Metadata in database(%s) is different from new dataSource metadata(%s)", supervisorId, metadata, spec);
                }
              }
              catch (Exception e) {
                log.makeAlert(e, StringUtils.format("uncaught exception in %s.", supervisorId)).emit();
              }
            }
          },
          0,
          delay.getMillis(),
          TimeUnit.MILLISECONDS
      );
      started = true;
    }
  }
  
  @Override
  public void stop(boolean stopGracefully) 
  {
    synchronized (stateLock) {
      Preconditions.checkState(started, "not started");
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
        spec.getBaseDataSource(),
        spec.getDimensions(),
        spec.getMetrics(),
        JodaUtils.condenseIntervals(missInterval)
    );
  }

  @Override
  public void reset(DataSourceMetadata dataSourceMetadata)
  {
    if (dataSourceMetadata == null) {
      // if oldMetadata is different from spec, tasks and segments will be removed when reset.
      DataSourceMetadata oldMetadata = metadataStorageCoordinator.getDataSourceMetadata(dataSource);
      if (oldMetadata != null && oldMetadata instanceof DerivativeDataSourceMetadata) {
        if (!((DerivativeDataSourceMetadata) oldMetadata).getBaseDataSource().equals(spec.getBaseDataSource()) ||
            !((DerivativeDataSourceMetadata) oldMetadata).getDimensions().equals(spec.getDimensions()) ||
            !((DerivativeDataSourceMetadata) oldMetadata).getMetrics().equals(spec.getMetrics())) {
          synchronized (taskLock) {
            clearTasks();
            clearSegments();
          }
        }
      }
      commitDataSourceMetadata(new DerivativeDataSourceMetadata(spec.getBaseDataSource(), spec.getDimensions(), spec.getMetrics()));
    } else {
      throw new IAE("DerivedDataSourceMetadata is not allowed to reset to a new DerivedDataSourceMetadata");
    }
  }

  @Override
  public void checkpoint(@Nullable String sequenceName, @Nullable DataSourceMetadata previousCheckPoint, @Nullable DataSourceMetadata currentCheckPoint)
  {
    // do nothing
  }

  @VisibleForTesting
  void checkSegmentsAndSubmitTasks()
  {
    synchronized (taskLock) {
      for (Map.Entry<Interval, HadoopIndexTask> entry : runningTasks.entrySet()) {
        if (!taskStorage.getStatus(entry.getValue().getId()).get().isRunnable()) {
          runningTasks.remove(entry.getKey());
          runningVersion.remove(entry.getKey());
        }
      }
      if (runningTasks.size() == maxTaskCount) {
        //if the number of running tasks reach the max task count, supervisor won't submit new tasks.
        return;
      }
      Pair<SortedMap<Interval, String>, Map<Interval, List<DataSegment>>> toBuildInterval = checkSegments();
      SortedMap<Interval, String> sortedToBuildVersion = toBuildInterval.lhs;
      Map<Interval, List<DataSegment>> baseSegments = toBuildInterval.rhs;
      missInterval = sortedToBuildVersion.keySet();
      submitTasks(sortedToBuildVersion, baseSegments);
    }
  }

  @VisibleForTesting
  Pair<SortedMap<Interval, String>, Map<Interval, List<DataSegment>>> checkSegments()
  {
    Pair<Map<Interval, String>, Map<Interval, List<DataSegment>>> derivativeSegmentsSnapshot =
        getVersionAndBaseSegments(metadataStorageCoordinator.getUsedSegmentsForInterval(dataSource, ALL_INTERVAL));
    Pair<Map<Interval, String>, Map<Interval, List<DataSegment>>> baseSegmentsSnapshot =
        getMaxCreateDateAndBaseSegments(metadataStorageCoordinator.getUsedSegmentAndCreatedDateForInterval(spec.getBaseDataSource(), ALL_INTERVAL));
    // baseSegments are used to create HadoopIndexTask
    Map<Interval, List<DataSegment>> baseSegments = baseSegmentsSnapshot.rhs;
    Map<Interval, List<DataSegment>> derivativeSegments = derivativeSegmentsSnapshot.rhs;
    // use max created_date of base segments as the version of derivative segments
    Map<Interval, String> maxCreatedDate = baseSegmentsSnapshot.lhs;
    Map<Interval, String> derivativeVersion = derivativeSegmentsSnapshot.lhs;
    
    SortedMap<Interval, String> sortedToBuildInterval = Maps.newTreeMap(Comparators.inverse(Comparators.intervalsByStartThenEnd()));
    MapDifference<Interval, String> difference = Maps.difference(maxCreatedDate, derivativeVersion);
    Map<Interval, String> toBuildInterval = Maps.newHashMap(difference.entriesOnlyOnLeft());
    Map<Interval, String> toDropInterval = Maps.newHashMap(difference.entriesOnlyOnRight());
    // if some intervals are in running tasks and the versions are the same, remove it from toBuildInterval
    // if some intervals are in running tasks, but the versions are different, stop the task. 
    for (Interval interval : runningVersion.keySet()) {
      if (toBuildInterval.containsKey(interval) && toBuildInterval.get(interval).equals(runningVersion.get(interval))) {
        toBuildInterval.remove(interval);

      } else if (toBuildInterval.containsKey(interval) && !toBuildInterval.get(interval).equals(runningVersion.get(interval))) {
        if (taskMaster.getTaskQueue().isPresent()) {
          taskMaster.getTaskQueue().get().shutdown(runningTasks.get(interval).getId());
          runningTasks.remove(interval);
        }
      }
    }
    // drop derivative segments which interval equals the interval in toDeleteBaseSegments 
    for (Interval interval : toDropInterval.keySet()) {
      for (DataSegment segment : derivativeSegments.get(interval)) {
        segmentManager.removeSegment(dataSource, segment.getIdentifier());
      }
    }
    // data in latest interval will be built firstly.
    sortedToBuildInterval.putAll(toBuildInterval);
    return new Pair<>(sortedToBuildInterval, baseSegments);
  }

  private void submitTasks(SortedMap<Interval, String> sortedToBuildVersion, Map<Interval, List<DataSegment>> baseSegments)
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
  
  private Pair<Map<Interval, String>, Map<Interval, List<DataSegment>>> getVersionAndBaseSegments(List<DataSegment> snapshot)
  {
    Map<Interval, String> versions = Maps.newHashMap();
    Map<Interval, List<DataSegment>> segments = Maps.newHashMap();
    for (DataSegment segment : snapshot) {
      Interval interval = segment.getInterval();
      versions.put(interval, segment.getVersion());
      segments.putIfAbsent(interval, Lists.newArrayList());
      segments.get(interval).add(segment);
    }
    return new Pair<>(versions, segments);
  }
  
  private Pair<Map<Interval, String>, Map<Interval, List<DataSegment>>> getMaxCreateDateAndBaseSegments(List<Pair<DataSegment, String>> snapshot)
  {
    Map<Interval, String> maxCreatedDate = Maps.newHashMap();
    Map<Interval, List<DataSegment>> segments = Maps.newHashMap();
    for (Pair<DataSegment, String> entry : snapshot) {
      DataSegment segment = entry.lhs;
      String createDate = entry.rhs;
      Interval interval = segment.getInterval();
      maxCreatedDate.put(
          interval, 
          DateTimes.max(
              DateTimes.of(createDate), 
              DateTimes.of(maxCreatedDate.getOrDefault(interval, DateTimes.MIN.toString()))
          ).toString()
      );
      segments.putIfAbsent(interval, Lists.newArrayList());
      segments.get(interval).add(segment);
    }
    return new Pair<>(maxCreatedDate, segments);
  }
  
  private void clearTasks() 
  {
    for (HadoopIndexTask task : runningTasks.values()) {
      if (taskMaster.getTaskQueue().isPresent()) {
        taskMaster.getTaskQueue().get().shutdown(task.getId());
      }
    }
    runningTasks.clear();
    runningVersion.clear();
  }
  
  private void clearSegments()
  {
    log.info("Clear all metadata of dataSource %s", dataSource);
    metadataStorageCoordinator.deletePendingSegments(dataSource, ALL_INTERVAL);
    segmentManager.removeDatasource(dataSource);
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

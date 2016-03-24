/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.coordinator.helper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.server.coordinator.CoordinatorHadoopMergeSpec;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DatasourceWhitelist;
import io.druid.server.coordinator.DruidCoordinatorHadoopMergeConfig;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class DruidCoordinatorHadoopSegmentMerger implements DruidCoordinatorHelper
{

  private static final Logger log = new Logger(DruidCoordinatorHadoopSegmentMerger.class);

  public static final String HADOOP_REINDEX_TASK_ID_PREFIX = "coordinator_hadoop_reindex";

  private final IndexingServiceClient indexingServiceClient;
  private final AtomicReference<DatasourceWhitelist> whiteListRef;


  private boolean scanFromOldToNew;

  public DruidCoordinatorHadoopSegmentMerger(
      IndexingServiceClient indexingServiceClient,
      AtomicReference<DatasourceWhitelist> whiteListRef,
      boolean scanFromOldToNew
  )
  {
    this.indexingServiceClient = indexingServiceClient;
    this.whiteListRef = whiteListRef;
    this.scanFromOldToNew = scanFromOldToNew;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final DruidCoordinatorHadoopMergeConfig hadoopMergeConfig = params.getCoordinatorDynamicConfig()
                                                                      .getHadoopMergeConfig();
    if (hadoopMergeConfig == null
        || hadoopMergeConfig.getHadoopMergeSpecs() == null
        || hadoopMergeConfig.getHadoopMergeSpecs().isEmpty()) {
      log.info("No HadoopMergeConfig was found, skipping Hadoop segment merging");
      return params;
    }

    final Map<String, CoordinatorHadoopMergeSpec> hadoopMergeSpecs = new HashMap<>();
    for (CoordinatorHadoopMergeSpec spec : hadoopMergeConfig.getHadoopMergeSpecs()) {
      hadoopMergeSpecs.put(spec.getDataSource(), spec);
    }

    final boolean keepSegmentGapDuringMerge = hadoopMergeConfig.isKeepGap();
    final DatasourceWhitelist whitelist = whiteListRef.get();
    final long segmentSizeThreshold = params.getCoordinatorDynamicConfig().getMergeBytesLimit();

    final CoordinatorStats stats = new CoordinatorStats();
    final Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources = Maps.newHashMap();

    // Find serviced segments by using a timeline
    for (DataSegment dataSegment : params.getAvailableSegments()) {
      if (whitelist == null || whitelist.contains(dataSegment.getDataSource())) {
        VersionedIntervalTimeline<String, DataSegment> timeline = dataSources.get(dataSegment.getDataSource());
        if (timeline == null) {
          timeline = new VersionedIntervalTimeline<String, DataSegment>(Ordering.<String>natural());
          dataSources.put(dataSegment.getDataSource(), timeline);
        }
        timeline.add(
            dataSegment.getInterval(),
            dataSegment.getVersion(),
            dataSegment.getShardSpec().createChunk(dataSegment)
        );
      }
    }

    for (final Map.Entry<String, VersionedIntervalTimeline<String, DataSegment>> entry : dataSources.entrySet()) {
      final String dataSource = entry.getKey();
      final CoordinatorHadoopMergeSpec mergeSpec = hadoopMergeSpecs.get(dataSource);
      if (mergeSpec == null) {
        log.info("Didn't find CoordinatorHadoopMergeSpec for dataSource [%s], skip merging", dataSource);
        continue;
      }

      log.info("Finding imbalanced segments for datasource [%s]", dataSource);

      final VersionedIntervalTimeline<String, DataSegment> timeline = entry.getValue();
      final List<TimelineObjectHolder<String, DataSegment>> timelineObjects = timeline.lookup(
          new Interval(new DateTime(0), new DateTime("3000-01-01"))
      );
      final List<Interval> unbalancedIntervals = new ArrayList<Interval>();

      long currTotalSize = 0;
      Interval intervalToReindex = null;
      boolean shouldBeMerged = false;

      final Iterator<TimelineObjectHolder<String, DataSegment>> listIterator = scanFromOldToNew
                                                                               ? timelineObjects.iterator()
                                                                               : Lists.reverse(timelineObjects)
                                                                                      .iterator();

      while (listIterator.hasNext()) {
        TimelineObjectHolder<String, DataSegment> objectHolder = listIterator.next();
        final Interval currInterval = objectHolder.getInterval();
        if (intervalToReindex == null) {
          intervalToReindex = currInterval;
        } else {
          if (currInterval.abuts(intervalToReindex)) {
            intervalToReindex = expandInterval(intervalToReindex, currInterval);
          } else if (keepSegmentGapDuringMerge) {
            intervalToReindex = currInterval;
            currTotalSize = 0;
            shouldBeMerged = false;
          } else {
            intervalToReindex = expandInterval(intervalToReindex, currInterval);
          }
        }

        for (DataSegment segment : objectHolder.getObject().payloads()) {
          if (segment.getSize() < segmentSizeThreshold) {
            shouldBeMerged = true;
          }
          currTotalSize += segment.getSize();
        }

        log.debug("currTotalSize [%d], target [%d]", currTotalSize, segmentSizeThreshold);
        if (currTotalSize >= segmentSizeThreshold) {
          if (shouldBeMerged) {
            unbalancedIntervals.add(intervalToReindex);
          }
          currTotalSize = 0;
          intervalToReindex = null;
          shouldBeMerged = false;
        }

      }

      if (!unbalancedIntervals.isEmpty()) {
        submitHadoopReindexTask(dataSource, unbalancedIntervals, stats, mergeSpec, hadoopMergeConfig);
      }
    }

    // invert the scan direction if "keepGap" is set
    if (keepSegmentGapDuringMerge) {
      scanFromOldToNew = !scanFromOldToNew;
    }

    log.info("Issued merge requests for [%s] dataSource", stats.getGlobalStats().get("hadoopMergeCount").get());

    params.getEmitter().emit(
        new ServiceMetricEvent.Builder().build(
            "coordinator/hadoopMerge/count", stats.getGlobalStats().get("hadoopMergeCount")
        )
    );
    return params.buildFromExisting().withCoordinatorStats(stats).build();
  }

  private Interval expandInterval(Interval intervalToReindex, Interval currInterval)
  {
    return scanFromOldToNew
           ? intervalToReindex.withEnd(currInterval.getEnd())
           : intervalToReindex.withStart(currInterval.getStart());
  }

  private void submitHadoopReindexTask(
      String dataSource,
      List<Interval> intervalsToReindex,
      CoordinatorStats stats,
      CoordinatorHadoopMergeSpec mergeSpec,
      DruidCoordinatorHadoopMergeConfig hadoopMergeConfig
  )
  {
    final List<Map<String, Object>> incompleteTasks = indexingServiceClient.getIncompleteTasks();
    if (incompleteTasks == null || isPreviousTaskFinished(dataSource, incompleteTasks)) {

      final String taskId = indexingServiceClient.hadoopMergeSegments(
          dataSource,
          intervalsToReindex,
          mergeSpec.getMetricsSpec(),
          mergeSpec.getQueryGranularity(),
          mergeSpec.getDimensions(),
          hadoopMergeConfig.getTuningConfig(),
          hadoopMergeConfig.getHadoopDependencyCoordinates()
      );
      log.info(
          "Submitted Hadoop Reindex Task for dataSource [%s] at intervals [%s]. TaskID is [%s]",
          dataSource,
          intervalsToReindex,
          taskId
      );
      stats.addToGlobalStat("hadoopMergeCount", 1);

    } else {
      log.info(
          "An existing Hadoop Reindex Task for dataSource [%s] at intervals [%s] is still running, skipping",
          dataSource,
          intervalsToReindex
      );
    }
  }

  private boolean isPreviousTaskFinished(String dataSource, List<Map<String, Object>> incompleteTasks)
  {
    for (Map<String, Object> task : incompleteTasks) {
      final String taskId = (String) task.get("id");
      if (taskId != null && taskId.startsWith(HADOOP_REINDEX_TASK_ID_PREFIX + "_" + dataSource)) {
        return false;
      }
    }
    return true;
  }

}

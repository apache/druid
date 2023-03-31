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

import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.incremental.ParseExceptionReport;
import org.apache.druid.segment.incremental.RowIngestionMetersTotals;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MultiPhaseParallelIndexStatsReporter extends ParallelIndexStatsReporter
{
  private static final Logger LOG = new Logger(MultiPhaseParallelIndexStatsReporter.class);

  @Override
  ParallelIndexStats report(
      ParallelIndexSupervisorTask task,
      Object runner,
      boolean includeUnparseable,
      boolean full
  )
  {
    // use cached version if available
    ParallelIndexStats cached = task.getIndexGenerateRowStats();
    if (null != cached) {
      return cached;
    }

    ParallelIndexTaskRunner<?, ?> currentRunner = (ParallelIndexTaskRunner<?, ?>) runner;
    if (!InputSourceSplitParallelIndexTaskRunner.PARTIAL_SEGMENT_GENERATION_PHASE.equals(currentRunner.getName())) {
      return new ParallelIndexStats();
    }

    Map<String, GeneratedPartitionsReport> completedSubtaskReports =
        (Map<String, GeneratedPartitionsReport>) currentRunner.getReports();

    final SimpleRowIngestionMeters buildSegmentsRowStats = new SimpleRowIngestionMeters();
    final List<ParseExceptionReport> unparseableEvents = new ArrayList<>();
    Set<Interval> intervalsIngested = new HashSet<>();
    for (GeneratedPartitionsReport generatedPartitionsReport : completedSubtaskReports.values()) {
      Map<String, TaskReport> taskReport = generatedPartitionsReport.getTaskReport();
      if (taskReport == null || taskReport.isEmpty()) {
        LOG.warn("Got an empty task report from subtask: " + generatedPartitionsReport.getTaskId());
        continue;
      }

      IngestionStatsAndErrorsTaskReport iseReport =
          (IngestionStatsAndErrorsTaskReport) taskReport.get(IngestionStatsAndErrorsTaskReport.REPORT_KEY);
      IngestionStatsAndErrorsTaskReportData payload = (IngestionStatsAndErrorsTaskReportData) iseReport.getPayload();
      intervalsIngested.addAll(payload.getIngestedIntervals());

      RowIngestionMetersTotals rowStatsForCompletedTask =
          getBuildSegmentsStatsFromTaskReport(taskReport, true, unparseableEvents);

      buildSegmentsRowStats.addRowIngestionMetersTotals(rowStatsForCompletedTask);
    }

    RowIngestionMetersTotals rowStatsForRunningTasks = getRowStatsAndUnparseableEventsForRunningTasks(
        task,
        currentRunner.getRunningTaskIds(),
        unparseableEvents,
        includeUnparseable
    );
    buildSegmentsRowStats.addRowIngestionMetersTotals(rowStatsForRunningTasks);

    Pair<Map<String, Object>, Map<String, Object>> report =
        createStatsAndErrorsReport(buildSegmentsRowStats.getTotals(), unparseableEvents);

    return new ParallelIndexStats(
        report.lhs,
        report.rhs,
        intervalsIngested
    );
  }
}

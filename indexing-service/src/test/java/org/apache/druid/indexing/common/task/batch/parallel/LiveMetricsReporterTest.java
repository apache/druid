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

import org.apache.druid.indexing.common.task.batch.parallel.LiveMetricsReporter.ReportRunnable;
import org.apache.druid.indexing.stats.NoopIngestionMetrics;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class LiveMetricsReporterTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final String SUPERVISOR_TASK_ID = "supervisorTaskId";
  private static final String SUBTASK_ID = "subtaskId";

  @Test
  public void testStartAndStopVerifyReports() throws InterruptedException
  {
    final ParallelIndexSupervisorTaskClient client = Mockito.mock(ParallelIndexSupervisorTaskClient.class);
    final LiveMetricsReporter reporter = new LiveMetricsReporter(
        SUPERVISOR_TASK_ID,
        SUBTASK_ID,
        client,
        NoopIngestionMetrics.INSTANCE,
        100,
        3
    );

    reporter.start();
    Thread.sleep(1000);
    reporter.stop();

    Mockito.verify(client, Mockito.atLeast(10))
           .report(ArgumentMatchers.matches(SUPERVISOR_TASK_ID), ArgumentMatchers.any(RunningSubtaskReport.class));
  }

  @Test
  public void testStopWithoutStartStopSuccessfully()
  {
    final ParallelIndexSupervisorTaskClient client = Mockito.mock(ParallelIndexSupervisorTaskClient.class);
    final LiveMetricsReporter reporter = new LiveMetricsReporter(
        SUPERVISOR_TASK_ID,
        SUBTASK_ID,
        client,
        NoopIngestionMetrics.INSTANCE,
        100,
        3
    );
    reporter.stop();
    // Nothing to verify
  }

  @Test
  public void testSucceedAfterRetriesOnFailuresInReportRunnable()
  {
    final ParallelIndexSupervisorTaskClient client = Mockito.mock(ParallelIndexSupervisorTaskClient.class);
    final LiveMetricsReporter reporter = new LiveMetricsReporter(
        SUPERVISOR_TASK_ID,
        SUBTASK_ID,
        client,
        NoopIngestionMetrics.INSTANCE,
        100,
        3
    );
    Mockito.doThrow(new RuntimeException("1st failure"))
           .doThrow(new RuntimeException("2nd failure"))
           .doNothing()
           .when(client)
           .report(ArgumentMatchers.matches(SUPERVISOR_TASK_ID), ArgumentMatchers.any(RunningSubtaskReport.class));
    final ReportRunnable reportRunnable = reporter.newReportRunnable();
    reportRunnable.run();

    Mockito.verify(client, Mockito.times(3))
           .report(ArgumentMatchers.matches(SUPERVISOR_TASK_ID), ArgumentMatchers.any(RunningSubtaskReport.class));
  }

  @Test
  public void testFailAfterRetriesOnFailuresInReportRunnable()
  {
    final ParallelIndexSupervisorTaskClient client = Mockito.mock(ParallelIndexSupervisorTaskClient.class);
    final LiveMetricsReporter reporter = new LiveMetricsReporter(
        SUPERVISOR_TASK_ID,
        SUBTASK_ID,
        client,
        NoopIngestionMetrics.INSTANCE,
        100,
        3
    );
    Mockito.doThrow(new RuntimeException("1st failure"))
           .doThrow(new RuntimeException("2nd failure"))
           .doThrow(new RuntimeException("3rd failure"))
           .doNothing()
           .when(client)
           .report(ArgumentMatchers.matches(SUPERVISOR_TASK_ID), ArgumentMatchers.any(RunningSubtaskReport.class));
    final ReportRunnable reportRunnable = reporter.newReportRunnable();
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("3rd failure");
    reportRunnable.run();

    Mockito.verify(client, Mockito.times(3))
           .report(ArgumentMatchers.matches(SUPERVISOR_TASK_ID), ArgumentMatchers.any(RunningSubtaskReport.class));
  }
}

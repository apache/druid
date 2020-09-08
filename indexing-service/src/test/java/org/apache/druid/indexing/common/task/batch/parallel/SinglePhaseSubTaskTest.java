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

import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.timeline.DataSegment;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.Collections;
import java.util.Set;

public class SinglePhaseSubTaskTest
{
  private static final String SUPERVISOR_TASK_ID = "supervisorTaskId";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  
  @Test
  public void testRunTaskWithExceptionReportIt() throws Exception
  {
    final ParallelIndexSupervisorTaskClient supervisorTaskClient = EasyMock.mock(
        ParallelIndexSupervisorTaskClient.class
    );
    Capture<SubTaskReport> capturedReport = EasyMock.newCapture();
    supervisorTaskClient.report(EasyMock.eq(SUPERVISOR_TASK_ID), EasyMock.capture(capturedReport));
    EasyMock.expectLastCall();
    final TaskToolbox toolbox = EasyMock.mock(TaskToolbox.class);
    EasyMock.expect(toolbox.getSupervisorTaskClientFactory())
            .andReturn((taskInfoProvider, callerId, numThreads, httpTimeout, numRetries) -> supervisorTaskClient);
    EasyMock.expect(toolbox.getIndexingServiceClient())
            .andReturn(new NoopIndexingServiceClient());
    EasyMock.expect(toolbox.getIndexingTmpDir())
            .andReturn(null);
    EasyMock.replay(supervisorTaskClient, toolbox);

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Fail test");
    final SinglePhaseSubTask task = new FailingTask();
    task.runTask(toolbox);
    Assert.assertSame(FailedSubtaskReport.class, capturedReport.getValue().getClass());
    Assert.assertEquals(task.getId(), capturedReport.getValue().getTaskId());
    Assert.assertEquals(TaskState.FAILED, capturedReport.getValue().getState());
    Assert.assertNull(capturedReport.getValue().getMetrics());
  }

  private static class FailingTask extends SinglePhaseSubTask
  {
    private FailingTask()
    {
      super(
          null,
          null,
          null,
          SUPERVISOR_TASK_ID,
          0,
          new ParallelIndexIngestionSpec(
              new DataSchema(
                  "datasource",
                  new TimestampSpec(null, null, null),
                  new DimensionsSpec(Collections.emptyList()),
                  new AggregatorFactory[0],
                  new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
                  null
              ),
              new ParallelIndexIOConfig(
                  null,
                  new InlineInputSource("{}"),
                  new JsonInputFormat(null, null, null),
                  null
              ),
              null
          ),
          null
      );
    }

    @Override
    Set<DataSegment> generateAndPushSegments(
        TaskToolbox toolbox,
        ParallelIndexSupervisorTaskClient taskClient,
        InputSource inputSource,
        File tmpDir
    )
    {
      throw new RuntimeException("Fail test");
    }
  }
}

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

import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class ParallelIndexPhaseRunnerTest extends AbstractParallelIndexSupervisorTaskTest
{
  private File inputDir;

  @Before
  public void setup() throws IOException
  {
    inputDir = temporaryFolder.newFolder("data");
    // set up data
    for (int i = 0; i < 5; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "test_" + i).toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("2017-12-%d,%d th test file\n", 24 + i, i));
        writer.write(StringUtils.format("2017-12-%d,%d th test file\n", 25 + i, i));
      }
    }

    for (int i = 0; i < 5; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "filtered_" + i).toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("2017-12-%d,%d th test file\n", 25 + i, i));
      }
    }

    getObjectMapper().registerSubtypes(new NamedType(ReportingNoopTask.class, "reporting_noop"));
  }

  @After
  public void tearDown()
  {
    temporaryFolder.delete();
  }

  @Test
  public void testLargeEstimatedNumSplits() throws Exception
  {
    final NoopTask task = NoopTask.create();
    final TaskActionClient actionClient = createActionClient(task);
    final TaskToolbox toolbox = createTaskToolbox(task, actionClient);
    final TestPhaseRunner runner = new TestPhaseRunner(
        toolbox,
        "supervisorTaskId",
        "groupId",
        AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING,
        getIndexingServiceClient(),
        10,
        12
    );
    Assert.assertEquals(TaskState.SUCCESS, runner.run());
  }

  @Test
  public void testSmallEstimatedNumSplits() throws Exception
  {
    final NoopTask task = NoopTask.create();
    final TaskActionClient actionClient = createActionClient(task);
    final TaskToolbox toolbox = createTaskToolbox(task, actionClient);
    final TestPhaseRunner runner = new TestPhaseRunner(
        toolbox,
        "supervisorTaskId",
        "groupId",
        AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING,
        getIndexingServiceClient(),
        10,
        8
    );
    Assert.assertEquals(TaskState.SUCCESS, runner.run());
  }

  private static class TestPhaseRunner extends ParallelIndexPhaseRunner<ReportingNoopTask, EmptySubTaskReport>
  {
    private final int actualNumSubTasks;
    private final int estimatedNumSubTasks;

    TestPhaseRunner(
        TaskToolbox toolbox,
        String supervisorTaskId,
        String groupId,
        ParallelIndexTuningConfig tuningConfig,
        IndexingServiceClient indexingServiceClient,
        int actualNumSubTasks,
        int estimatedNumSubTasks
    )
    {
      super(
          toolbox,
          supervisorTaskId,
          groupId,
          tuningConfig,
          Collections.emptyMap(),
          indexingServiceClient
      );
      this.actualNumSubTasks = actualNumSubTasks;
      this.estimatedNumSubTasks = estimatedNumSubTasks;
    }

    @Override
    Iterator<SubTaskSpec<ReportingNoopTask>> subTaskSpecIterator()
    {
      return new Iterator<SubTaskSpec<ReportingNoopTask>>()
      {
        int subTaskCount = 0;

        @Override
        public boolean hasNext()
        {
          return subTaskCount < actualNumSubTasks;
        }

        @Override
        public SubTaskSpec<ReportingNoopTask> next()
        {
          return new TestSubTaskSpec(
              "specId_" + subTaskCount++,
              getGroupId(),
              getTaskId(),
              getContext(),
              new InputSplit<>(new Object()),
              TestPhaseRunner.this
          );
        }
      };
    }

    @Override
    int estimateTotalNumSubTasks()
    {
      return estimatedNumSubTasks;
    }

    @Override
    public String getName()
    {
      return "TestPhaseRunner";
    }
  }

  private static class TestSubTaskSpec extends SubTaskSpec<ReportingNoopTask>
  {
    private final TestPhaseRunner phaseRunner;

    private TestSubTaskSpec(
        String id,
        String groupId,
        String supervisorTaskId,
        Map<String, Object> context,
        InputSplit inputSplit,
        TestPhaseRunner phaseRunner
    )
    {
      super(id, groupId, supervisorTaskId, context, inputSplit);
      this.phaseRunner = phaseRunner;
    }

    @Override
    public ReportingNoopTask newSubTask(int numAttempts)
    {
      return new ReportingNoopTask(getGroupId(), phaseRunner);
    }
  }

  private static class EmptySubTaskReport implements SubTaskReport
  {
    private final String taskId;

    private EmptySubTaskReport(String taskId)
    {
      this.taskId = taskId;
    }

    @Override
    public String getTaskId()
    {
      return taskId;
    }
  }

  private static class ReportingNoopTask extends NoopTask
  {
    private final TestPhaseRunner phaseRunner;

    private ReportingNoopTask(String groupId, TestPhaseRunner phaseRunner)
    {
      super(
          null,
          groupId,
          null,
          10,
          0,
          null,
          null,
          Collections.singletonMap(AbstractParallelIndexSupervisorTaskTest.DISABLE_TASK_INJECT_CONTEXT_KEY, true)
      );
      this.phaseRunner = phaseRunner;
    }

    @Override
    public TaskStatus run(TaskToolbox toolbox) throws Exception
    {
      final TaskStatus result = super.run(toolbox);
      phaseRunner.collectReport(new EmptySubTaskReport(getId()));
      return result;
    }
  }
}

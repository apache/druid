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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.config.ForkingTaskRunnerConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.assertj.core.util.Lists;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ForkingTaskRunnerTest
{
  // This tests the test to make sure the test fails when it should.
  @Test(expected = AssertionError.class)
  public void testPatternMatcherFailureForJavaOptions()
  {
    checkValues(new String[]{"not quoted has space"});
  }

  @Test(expected = AssertionError.class)
  public void testPatternMatcherFailureForSpaceOnlyJavaOptions()
  {
    checkValues(new String[]{" "});
  }

  @Test
  public void testPatternMatcherLeavesUnbalancedQuoteJavaOptions()
  {
    Assert.assertEquals("\"", Iterators.get(new QuotableWhiteSpaceSplitter("\"").iterator(), 0));
  }

  @Test
  public void testPatternMatcherPreservesNonBreakingSpacesJavaOptions()
  {
    // SUPER AWESOME VOODOO MAGIC. These are not normal spaces, they are non-breaking spaces.
    checkValues(new String[]{"keep me around"});
  }


  @Test
  public void testPatternMatcherForSimpleJavaOptions()
  {
    checkValues(
        new String[]{
            "test",
            "-mmm\"some quote with\"suffix",
            "test2",
            "\"completely quoted\"",
            "more",
            "☃",
            "-XX:SomeCoolOption=false",
            "-XX:SomeOption=\"with spaces\"",
            "someValues",
            "some\"strange looking\"option",
            "andOtherOptions",
            "\"\"",
            "AndMaybeEmptyQuotes",
            "keep me around"
        }
    );
    checkValues(new String[]{"\"completely quoted\""});
    checkValues(new String[]{"\"\""});
    checkValues(new String[]{"-foo=\"\""});
    checkValues(new String[]{"-foo=\"\"suffix"});
    checkValues(new String[]{"-foo=\"\t\"suffix"});
    checkValues(new String[]{"-foo=\"\t\r\n\f     \"suffix"});
    checkValues(new String[]{"-foo=\"\t\r\n\f     \""});
    checkValues(new String[]{"\"\t\r\n\f     \"suffix"});
    checkValues(new String[]{"-foo=\"\"suffix", "more"});
  }

  @Test
  public void testEmpty()
  {
    Assert.assertTrue(ImmutableList.copyOf(new QuotableWhiteSpaceSplitter("")).isEmpty());
  }

  @Test
  public void testFarApart()
  {
    Assert.assertEquals(
        ImmutableList.of("start", "stop"), ImmutableList.copyOf(
            new QuotableWhiteSpaceSplitter(
                "start\t\t\t\t \n\f\r\n \f\f \n\r\f\n\r\t stop"
            )
        )
    );
  }

  @Test
  public void testOmitEmpty()
  {
    Assert.assertTrue(
        ImmutableList.copyOf(
            new QuotableWhiteSpaceSplitter(" \t     \t\t\t\t \n\n \f\f \n\f\r\t")
        ).isEmpty()
    );
  }

  private static void checkValues(String[] strings)
  {
    Assert.assertEquals(
        ImmutableList.copyOf(strings),
        ImmutableList.copyOf(new QuotableWhiteSpaceSplitter(Joiner.on(" ").join(strings)))
    );
  }

  @Test
  public void testMaskedIterator()
  {
    Pair<List<String>, String> originalAndExpectedCommand = new Pair<>(
        Lists.list(
            "java -cp",
            "/path/to/somewhere:some-jars.jar",
            "/some===file",
            "/asecretFileNa=me", // this should not be masked but there is not way to know this not a property and probably this is an unrealistic scenario anyways
            "-Dsome.property=random",
            "-Dsome.otherproperty = random=random",
            "-Dsome.somesecret = secretvalue",
            "-Dsome.somesecret=secretvalue",
            "-Dsome.somepassword = secret=value",
            "-Dsome.some=notasecret",
            "-Dsome.otherSecret= =asfdhkj352872598====fasdlkjfa="
        ),
        "java -cp /path/to/somewhere:some-jars.jar /some===file /asecretFileNa=<masked> -Dsome.property=random -Dsome.otherproperty = random=random "
        + "-Dsome.somesecret =<masked> -Dsome.somesecret=<masked> -Dsome.somepassword =<masked> -Dsome.some=notasecret -Dsome.otherSecret=<masked>"
    );
    StartupLoggingConfig startupLoggingConfig = new StartupLoggingConfig();
    ForkingTaskRunner forkingTaskRunner = new ForkingTaskRunner(
        new ForkingTaskRunnerConfig(),
        null,
        new WorkerConfig(),
        null,
        null,
        null,
        null,
        startupLoggingConfig
    );
    Assert.assertEquals(
        originalAndExpectedCommand.rhs,
        forkingTaskRunner.getMaskedCommand(
            startupLoggingConfig.getMaskProperties(),
            originalAndExpectedCommand.lhs
        )
    );
  }

  @Test
  public void testTaskStatusWhenTaskProcessFails() throws ExecutionException, InterruptedException
  {
    ForkingTaskRunner forkingTaskRunner = new ForkingTaskRunner(
        new ForkingTaskRunnerConfig(),
        new TaskConfig(
            null,
            null,
            null,
            null,
            ImmutableList.of(),
            false,
            new Period("PT0S"),
            new Period("PT10S"),
            ImmutableList.of(),
            false,
            false,
            TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name()
        ),
        new WorkerConfig(),
        new Properties(),
        new NoopTaskLogs(),
        new DefaultObjectMapper(),
        new DruidNode("middleManager", "host", false, 8091, null, true, false),
        new StartupLoggingConfig()
    )
    {
      @Override
      ProcessHolder runTaskProcess(List<String> command, File logFile, TaskLocation taskLocation)
      {
        ProcessHolder processHolder = Mockito.mock(ProcessHolder.class);
        Mockito.doNothing().when(processHolder).registerWithCloser(ArgumentMatchers.any());
        Mockito.doNothing().when(processHolder).shutdown();
        return processHolder;
      }

      @Override
      int waitForTaskProcessToComplete(Task task, ProcessHolder processHolder, File logFile, File reportsFile)
      {
        // Emulate task process failure
        return 1;
      }
    };

    final TaskStatus status = forkingTaskRunner.run(NoopTask.create()).get();
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());
    Assert.assertEquals(
        "Task execution process exited unsuccessfully with code[1]. See middleManager logs for more details.",
        status.getErrorMsg()
    );
  }

  @Test
  public void testTaskStatusWhenTaskProcessSucceedsTaskSucceeds() throws ExecutionException, InterruptedException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    Task task = NoopTask.create();
    ForkingTaskRunner forkingTaskRunner = new ForkingTaskRunner(
        new ForkingTaskRunnerConfig(),
        new TaskConfig(
            null,
            null,
            null,
            null,
            ImmutableList.of(),
            false,
            new Period("PT0S"),
            new Period("PT10S"),
            ImmutableList.of(),
            false,
            false,
            TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name()
        ),
        new WorkerConfig(),
        new Properties(),
        new NoopTaskLogs(),
        mapper,
        new DruidNode("middleManager", "host", false, 8091, null, true, false),
        new StartupLoggingConfig()
    )
    {
      @Override
      ProcessHolder runTaskProcess(List<String> command, File logFile, TaskLocation taskLocation) throws IOException
      {
        ProcessHolder processHolder = Mockito.mock(ProcessHolder.class);
        Mockito.doNothing().when(processHolder).registerWithCloser(ArgumentMatchers.any());
        Mockito.doNothing().when(processHolder).shutdown();

        for (String param : command) {
          if (param.endsWith("status.json")) {
            mapper.writeValue(new File(param), TaskStatus.success(task.getId()));
            break;
          }
        }

        return processHolder;
      }

      @Override
      int waitForTaskProcessToComplete(Task task, ProcessHolder processHolder, File logFile, File reportsFile)
      {
        return 0;
      }
    };

    final TaskStatus status = forkingTaskRunner.run(task).get();
    Assert.assertEquals(TaskState.SUCCESS, status.getStatusCode());
    Assert.assertNull(status.getErrorMsg());
  }

  @Test
  public void testTaskStatusWhenTaskProcessSucceedsTaskFails() throws ExecutionException, InterruptedException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    Task task = NoopTask.create();
    ForkingTaskRunner forkingTaskRunner = new ForkingTaskRunner(
        new ForkingTaskRunnerConfig(),
        new TaskConfig(
            null,
            null,
            null,
            null,
            ImmutableList.of(),
            false,
            new Period("PT0S"),
            new Period("PT10S"),
            ImmutableList.of(),
            false,
            false,
            TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name()
        ),
        new WorkerConfig(),
        new Properties(),
        new NoopTaskLogs(),
        mapper,
        new DruidNode("middleManager", "host", false, 8091, null, true, false),
        new StartupLoggingConfig()
    )
    {
      @Override
      ProcessHolder runTaskProcess(List<String> command, File logFile, TaskLocation taskLocation) throws IOException
      {
        ProcessHolder processHolder = Mockito.mock(ProcessHolder.class);
        Mockito.doNothing().when(processHolder).registerWithCloser(ArgumentMatchers.any());
        Mockito.doNothing().when(processHolder).shutdown();

        for (String param : command) {
          if (param.endsWith("status.json")) {
            mapper.writeValue(new File(param), TaskStatus.failure(task.getId(), "task failure test"));
            break;
          }
        }

        return processHolder;
      }

      @Override
      int waitForTaskProcessToComplete(Task task, ProcessHolder processHolder, File logFile, File reportsFile)
      {
        return 0;
      }
    };

    final TaskStatus status = forkingTaskRunner.run(task).get();
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());
    Assert.assertEquals("task failure test", status.getErrorMsg());
  }
}

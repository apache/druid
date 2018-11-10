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

package org.apache.druid.indexing.overlord.setup;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.TestRemoteTaskRunnerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.js.JavaScriptConfig;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JavaScriptWorkerSelectStrategyTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final JavaScriptWorkerSelectStrategy STRATEGY = new JavaScriptWorkerSelectStrategy(
      "function (config, zkWorkers, task) {\n"
      + "var batch_workers = new java.util.ArrayList();\n"
      + "batch_workers.add(\"10.0.0.1\");\n"
      + "batch_workers.add(\"10.0.0.2\");\n"
      + "workers = zkWorkers.keySet().toArray();\n"
      + "var sortedWorkers = new Array()\n;"
      + "for (var i = 0; i < workers.length; i++) {\n"
      + " sortedWorkers[i] = workers[i];\n"
      + "}\n"
      + "Array.prototype.sort.call(sortedWorkers,function(a, b){return zkWorkers.get(b).getCurrCapacityUsed() - zkWorkers.get(a).getCurrCapacityUsed();});\n"
      + "var minWorkerVer = config.getMinWorkerVersion();\n"
      + "for (var i = 0; i < sortedWorkers.length; i++) {\n"
      + " var worker = sortedWorkers[i];\n"
      + "  var zkWorker = zkWorkers.get(worker);\n"
      + "  if (zkWorker.canRunTask(task) && zkWorker.isValidVersion(minWorkerVer)) {\n"
      + "    if (task.getType() == 'index_hadoop' && batch_workers.contains(worker)) {\n"
      + "      return worker;\n"
      + "    } else {\n"
      + "      if (task.getType() != 'index_hadoop' && !batch_workers.contains(worker)) {\n"
      + "        return worker;\n"
      + "      }\n"
      + "    }\n"
      + "  }\n"
      + "}\n"
      + "return null;\n"
      + "}",
      JavaScriptConfig.getEnabledInstance()
  );

  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std().addValue(
            JavaScriptConfig.class,
            JavaScriptConfig.getEnabledInstance()
        )
    );

    Assert.assertEquals(
        STRATEGY,
        mapper.readValue(
            mapper.writeValueAsString(STRATEGY),
            WorkerSelectStrategy.class
        )
    );
  }

  @Test
  public void testDisabled() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std().addValue(
            JavaScriptConfig.class,
            new JavaScriptConfig(false)
        )
    );

    final String strategyString = mapper.writeValueAsString(STRATEGY);

    expectedException.expect(JsonMappingException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(IllegalStateException.class));
    expectedException.expectMessage("JavaScript is disabled");

    mapper.readValue(strategyString, WorkerSelectStrategy.class);
  }

  @Test
  public void testFindWorkerForTask()
  {
    ImmutableWorkerInfo worker1 = createMockWorker(1, true, true);
    ImmutableWorkerInfo worker2 = createMockWorker(1, true, true);
    ImmutableMap<String, ImmutableWorkerInfo> workerMap = ImmutableMap.of(
        "10.0.0.1", worker1,
        "10.0.0.3", worker2
    );

    ImmutableWorkerInfo workerForBatchTask = STRATEGY.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("index_hadoop")
    );
    // batch tasks should be sent to worker1
    Assert.assertEquals(worker1, workerForBatchTask);

    ImmutableWorkerInfo workerForOtherTask = STRATEGY.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("other_type")
    );
    // all other tasks should be sent to worker2
    Assert.assertEquals(worker2, workerForOtherTask);
  }

  @Test
  public void testIsolationOfBatchWorker()
  {
    ImmutableMap<String, ImmutableWorkerInfo> workerMap = ImmutableMap.of(
        "10.0.0.1", createMockWorker(1, true, true),
        "10.0.0.2", createMockWorker(1, true, true)
    );
    ImmutableWorkerInfo workerForOtherTask = STRATEGY.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("other_type")
    );
    Assert.assertNull(workerForOtherTask);
  }

  @Test
  public void testNoValidWorker()
  {
    ImmutableMap<String, ImmutableWorkerInfo> workerMap = ImmutableMap.of(
        "10.0.0.1", createMockWorker(1, true, false),
        "10.0.0.4", createMockWorker(1, true, false)
    );
    ImmutableWorkerInfo workerForBatchTask = STRATEGY.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("index_hadoop")
    );
    Assert.assertNull(workerForBatchTask);

    ImmutableWorkerInfo workerForOtherTask = STRATEGY.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("otherTask")
    );
    // all other tasks should be sent to worker2
    Assert.assertNull(workerForOtherTask);
  }

  @Test
  public void testNoWorkerCanRunTask()
  {
    ImmutableMap<String, ImmutableWorkerInfo> workerMap = ImmutableMap.of(
        "10.0.0.1", createMockWorker(1, false, true),
        "10.0.0.4", createMockWorker(1, false, true)
    );
    ImmutableWorkerInfo workerForBatchTask = STRATEGY.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("index_hadoop")
    );
    Assert.assertNull(workerForBatchTask);

    ImmutableWorkerInfo workerForOtherTask = STRATEGY.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("otherTask")
    );
    // all other tasks should be sent to worker2
    Assert.assertNull(workerForOtherTask);
  }

  @Test
  public void testFillWorkerCapacity()
  {
    // tasks shoudl be assigned to the worker with maximum currCapacity used until its full
    ImmutableMap<String, ImmutableWorkerInfo> workerMap = ImmutableMap.of(
        "10.0.0.1", createMockWorker(1, true, true),
        "10.0.0.2", createMockWorker(5, true, true)
    );
    ImmutableWorkerInfo workerForBatchTask = STRATEGY.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("index_hadoop")
    );
    Assert.assertEquals(workerMap.get("10.0.0.2"), workerForBatchTask);

  }

  private Task createMockTask(String type)
  {
    Task mock = EasyMock.createMock(Task.class);
    EasyMock.expect(mock.getType()).andReturn(type).anyTimes();
    EasyMock.replay(mock);
    return mock;
  }

  private ImmutableWorkerInfo createMockWorker(int currCapacityUsed, boolean canRunTask, boolean isValidVersion)
  {
    ImmutableWorkerInfo worker = EasyMock.createMock(ImmutableWorkerInfo.class);
    EasyMock.expect(worker.canRunTask(EasyMock.anyObject(Task.class))).andReturn(canRunTask).anyTimes();
    EasyMock.expect(worker.getCurrCapacityUsed()).andReturn(currCapacityUsed).anyTimes();
    EasyMock.expect(worker.isValidVersion(EasyMock.anyString())).andReturn(isValidVersion).anyTimes();
    EasyMock.replay(worker);
    return worker;
  }

}

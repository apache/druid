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

package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ImmutableWorkerInfo;
import io.druid.indexing.overlord.TestRemoteTaskRunnerConfig;
import io.druid.jackson.DefaultObjectMapper;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class JavaScriptWorkerSelectStrategyTest
{

  final JavaScriptWorkerSelectStrategy strategy = new JavaScriptWorkerSelectStrategy(
      "function (config, zkWorkers, task) {\n"
      + "var batch_workers = new java.util.ArrayList();\n"
      + "batch_workers.add(\"10.0.0.1\");\n"
      + "batch_workers.add(\"10.0.0.2\");\n"
      + "workers = zkWorkers.keySet().toArray();\n"
      + "var sortedWorkers = new Array()\n;"
      + "for(var i = 0; i < workers.length; i++){\n"
      + " sortedWorkers[i] = workers[i];\n"
      + "}\n"
      + "Array.prototype.sort.call(sortedWorkers,function(a, b){return zkWorkers.get(b).getCurrCapacityUsed() - zkWorkers.get(a).getCurrCapacityUsed();});\n"
      + "var minWorkerVer = config.getMinWorkerVersion();\n"
      + "for (var i = 0; i < sortedWorkers.length; i++) {\n"
      + " var worker = sortedWorkers[i];\n"
      + "  var zkWorker = zkWorkers.get(worker);\n"
      + "  if(zkWorker.canRunTask(task) && zkWorker.isValidVersion(minWorkerVer)){\n"
      + "    if(task.getType() == 'index_hadoop' && batch_workers.contains(worker)){\n"
      + "      return worker;\n"
      + "    } else {\n"
      + "      if(task.getType() != 'index_hadoop' && !batch_workers.contains(worker)){\n"
      + "        return worker;\n"
      + "      }\n"
      + "    }\n"
      + "  }\n"
      + "}\n"
      + "return null;\n"
      + "}"
  );

  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertEquals(
        strategy,
        mapper.readValue(
            mapper.writeValueAsString(strategy),
            JavaScriptWorkerSelectStrategy.class
        )
    );
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

    ImmutableWorkerInfo workerForBatchTask = strategy.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("index_hadoop")
    ).get();
    // batch tasks should be sent to worker1
    Assert.assertEquals(worker1, workerForBatchTask);

    ImmutableWorkerInfo workerForOtherTask = strategy.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("other_type")
    ).get();
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
    Optional<ImmutableWorkerInfo> workerForOtherTask = strategy.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("other_type")
    );
    Assert.assertFalse(workerForOtherTask.isPresent());
  }

  @Test
  public void testNoValidWorker()
  {
    ImmutableMap<String, ImmutableWorkerInfo> workerMap = ImmutableMap.of(
        "10.0.0.1", createMockWorker(1, true, false),
        "10.0.0.4", createMockWorker(1, true, false)
    );
    Optional<ImmutableWorkerInfo> workerForBatchTask = strategy.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("index_hadoop")
    );
    Assert.assertFalse(workerForBatchTask.isPresent());

    Optional<ImmutableWorkerInfo> workerForOtherTask = strategy.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("otherTask")
    );
    // all other tasks should be sent to worker2
    Assert.assertFalse(workerForOtherTask.isPresent());
  }

  @Test
  public void testNoWorkerCanRunTask()
  {
    ImmutableMap<String, ImmutableWorkerInfo> workerMap = ImmutableMap.of(
        "10.0.0.1", createMockWorker(1, false, true),
        "10.0.0.4", createMockWorker(1, false, true)
    );
    Optional<ImmutableWorkerInfo> workerForBatchTask = strategy.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("index_hadoop")
    );
    Assert.assertFalse(workerForBatchTask.isPresent());

    Optional<ImmutableWorkerInfo> workerForOtherTask = strategy.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("otherTask")
    );
    // all other tasks should be sent to worker2
    Assert.assertFalse(workerForOtherTask.isPresent());
  }

  @Test
  public void testFillWorkerCapacity()
  {
    // tasks shoudl be assigned to the worker with maximum currCapacity used until its full
    ImmutableMap<String, ImmutableWorkerInfo> workerMap = ImmutableMap.of(
        "10.0.0.1", createMockWorker(1, true, true),
        "10.0.0.2", createMockWorker(5, true, true)
    );
    Optional<ImmutableWorkerInfo> workerForBatchTask = strategy.findWorkerForTask(
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        workerMap,
        createMockTask("index_hadoop")
    );
    Assert.assertEquals(workerMap.get("10.0.0.2"), workerForBatchTask.get());

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

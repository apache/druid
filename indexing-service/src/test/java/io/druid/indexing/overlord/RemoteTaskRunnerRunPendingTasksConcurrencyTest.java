/*
 *
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 *
 */

package io.druid.indexing.overlord;

import com.google.common.util.concurrent.ListenableFuture;

import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TestTasks;
import io.druid.indexing.common.task.Task;
import io.druid.java.util.common.ISE;

import org.apache.zookeeper.ZooKeeper;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class RemoteTaskRunnerRunPendingTasksConcurrencyTest
{
  private RemoteTaskRunner remoteTaskRunner;
  private RemoteTaskRunnerTestUtils rtrTestUtils = new RemoteTaskRunnerTestUtils();

  @Before
  public void setUp() throws Exception
  {
    rtrTestUtils.setUp();
  }

  @After
  public void tearDown() throws Exception
  {
    if (remoteTaskRunner != null) {
      remoteTaskRunner.stop();
    }
    rtrTestUtils.tearDown();
  }

  // This task reproduces the races described in https://github.com/druid-io/druid/issues/2842
  @Test(timeout = 60_000)
  public void testConcurrency() throws Exception
  {
    rtrTestUtils.makeWorker("worker0");
    rtrTestUtils.makeWorker("worker1");

    remoteTaskRunner = rtrTestUtils.makeRemoteTaskRunner(
        new TestRemoteTaskRunnerConfig(new Period("PT3600S"))
        {
          public int getPendingTasksRunnerNumThreads()
          {
            return 2;
          }
        }
    );

    int numTasks = 6;
    ListenableFuture<TaskStatus>[] results = new ListenableFuture[numTasks];
    Task[] tasks = new Task[numTasks];

    //2 tasks
    for (int i = 0; i < 2; i++) {
      tasks[i] = TestTasks.unending("task" + i);
      results[i] = (remoteTaskRunner.run(tasks[i]));
    }

    waitForBothWorkersToHaveUnackedTasks();

    //3 more tasks, all of which get queued up
    for (int i = 2; i < 5; i++) {
      tasks[i] = TestTasks.unending("task" + i);
      results[i] = (remoteTaskRunner.run(tasks[i]));
    }

    //simulate completion of task0 and task1
    mockWorkerRunningAndCompletionSuccessfulTasks(tasks[0], tasks[1]);

    Assert.assertEquals(TaskStatus.Status.SUCCESS, results[0].get().getStatusCode());
    Assert.assertEquals(TaskStatus.Status.SUCCESS, results[1].get().getStatusCode());

    // now both threads race to run the last 3 tasks. task2 and task3 are being assigned
    waitForBothWorkersToHaveUnackedTasks();

    if (remoteTaskRunner.getWorkersWithUnacknowledgedTask().containsValue(tasks[2].getId())
        && remoteTaskRunner.getWorkersWithUnacknowledgedTask().containsValue(tasks[3].getId())) {
      remoteTaskRunner.shutdown("task4");
      mockWorkerRunningAndCompletionSuccessfulTasks(tasks[3], tasks[2]);
      Assert.assertEquals(TaskStatus.Status.SUCCESS, results[3].get().getStatusCode());
      Assert.assertEquals(TaskStatus.Status.SUCCESS, results[2].get().getStatusCode());
    } else if (remoteTaskRunner.getWorkersWithUnacknowledgedTask().containsValue(tasks[3].getId())
               && remoteTaskRunner.getWorkersWithUnacknowledgedTask().containsValue(tasks[4].getId())) {
      remoteTaskRunner.shutdown("task2");
      mockWorkerRunningAndCompletionSuccessfulTasks(tasks[4], tasks[3]);
      Assert.assertEquals(TaskStatus.Status.SUCCESS, results[4].get().getStatusCode());
      Assert.assertEquals(TaskStatus.Status.SUCCESS, results[3].get().getStatusCode());
    } else if (remoteTaskRunner.getWorkersWithUnacknowledgedTask().containsValue(tasks[4].getId())
               && remoteTaskRunner.getWorkersWithUnacknowledgedTask().containsValue(tasks[2].getId())) {
      remoteTaskRunner.shutdown("task3");
      mockWorkerRunningAndCompletionSuccessfulTasks(tasks[4], tasks[2]);
      Assert.assertEquals(TaskStatus.Status.SUCCESS, results[4].get().getStatusCode());
      Assert.assertEquals(TaskStatus.Status.SUCCESS, results[2].get().getStatusCode());
    } else {
      throw new ISE("two out of three tasks 2,3 and 4 must be waiting for ack.");
    }

    //ensure that RTR is doing OK and still making progress
    tasks[5] = TestTasks.unending("task5");
    results[5] = remoteTaskRunner.run(tasks[5]);
    waitForOneWorkerToHaveUnackedTasks();
    if (rtrTestUtils.taskAnnounced("worker0", tasks[5].getId())) {
      rtrTestUtils.mockWorkerRunningTask("worker0", tasks[5]);
      rtrTestUtils.mockWorkerCompleteSuccessfulTask("worker0", tasks[5]);
    } else {
      rtrTestUtils.mockWorkerRunningTask("worker1", tasks[5]);
      rtrTestUtils.mockWorkerCompleteSuccessfulTask("worker1", tasks[5]);
    }
    Assert.assertEquals(TaskStatus.Status.SUCCESS, results[5].get().getStatusCode());
  }

  private void mockWorkerRunningAndCompletionSuccessfulTasks(Task t1, Task t2) throws Exception
  {
    if (rtrTestUtils.taskAnnounced("worker0", t1.getId())) {
      rtrTestUtils.mockWorkerRunningTask("worker0", t1);
      rtrTestUtils.mockWorkerCompleteSuccessfulTask("worker0", t1);
      rtrTestUtils.mockWorkerRunningTask("worker1", t2);
      rtrTestUtils.mockWorkerCompleteSuccessfulTask("worker1", t2);
    } else {
      rtrTestUtils.mockWorkerRunningTask("worker1", t1);
      rtrTestUtils.mockWorkerCompleteSuccessfulTask("worker1", t1);
      rtrTestUtils.mockWorkerRunningTask("worker0", t2);
      rtrTestUtils.mockWorkerCompleteSuccessfulTask("worker0", t2);
    }
  }

  private void waitForOneWorkerToHaveUnackedTasks() throws Exception
  {
    while (remoteTaskRunner.getWorkersWithUnacknowledgedTask().size() < 1) {
      Thread.sleep(5);
    }

    ZooKeeper zk = rtrTestUtils.getCuratorFramework().getZookeeperClient().getZooKeeper();
    while (zk.getChildren(rtrTestUtils.tasksPath + "/worker0", false).size() < 1
           && zk.getChildren(rtrTestUtils.tasksPath + "/worker1", false).size() < 1) {
      Thread.sleep(5);
    }
  }

  private void waitForBothWorkersToHaveUnackedTasks() throws Exception
  {
    while (remoteTaskRunner.getWorkersWithUnacknowledgedTask().size() < 2) {
      Thread.sleep(5);
    }

    ZooKeeper zk = rtrTestUtils.getCuratorFramework().getZookeeperClient().getZooKeeper();
    while (zk.getChildren(rtrTestUtils.tasksPath + "/worker0", false).size() < 1
           || zk.getChildren(rtrTestUtils.tasksPath + "/worker1", false).size() < 1) {
      Thread.sleep(5);
    }
  }
}

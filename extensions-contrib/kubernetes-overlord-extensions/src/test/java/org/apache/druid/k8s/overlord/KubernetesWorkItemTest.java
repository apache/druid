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

package org.apache.druid.k8s.overlord;

import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class KubernetesWorkItemTest extends EasyMockSupport
{
  private KubernetesWorkItem workItem;
  private Task task;

  @Mock
  KubernetesPeonLifecycle kubernetesPeonLifecycle;

  @Before
  public void setup()
  {
    task = NoopTask.create();
  }

  @Test
  public void test_shutdown_withKubernetesPeonLifecycle()
  {
    kubernetesPeonLifecycle.shutdown();
    EasyMock.expectLastCall();
    kubernetesPeonLifecycle.startWatchingLogs();
    EasyMock.expectLastCall();

    replayAll();
    workItem = new KubernetesWorkItem(
        task,
        null,
        kubernetesPeonLifecycle
    );

    workItem.shutdown();
    verifyAll();
  }

  @Test
  public void test_isPending_withTaskStateWaiting_returnsFalse()
  {
    workItem = new KubernetesWorkItem(task, null, kubernetesPeonLifecycle) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.WAITING;
      }
    };
    Assert.assertFalse(workItem.isPending());
  }

  @Test
  public void test_isPending_withTaskStatePending_returnsTrue()
  {
    workItem = new KubernetesWorkItem(task, null, kubernetesPeonLifecycle) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.PENDING;
      }
    };
    Assert.assertTrue(workItem.isPending());
  }

  @Test
  public void test_isRunning_withTaskStateWaiting_returnsFalse()
  {
    workItem = new KubernetesWorkItem(task, null, kubernetesPeonLifecycle) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.WAITING;
      }
    };
    Assert.assertFalse(workItem.isRunning());
  }

  @Test
  public void test_isRunning_withTaskStatePending_returnsTrue()
  {
    workItem = new KubernetesWorkItem(task, null, kubernetesPeonLifecycle) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.RUNNING;
      }
    };
    Assert.assertTrue(workItem.isRunning());
  }

  @Test
  public void test_getRunnerTaskState_withKubernetesPeonLifecycle_returnsPending()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        null,
        null,
        null,
        null
    );
    workItem = new KubernetesWorkItem(task, null, peonLifecycle);

    Assert.assertEquals(RunnerTaskState.PENDING, workItem.getRunnerTaskState());
  }

  @Test
  public void test_getRunnerTaskState_withKubernetesPeonLifecycle_inPendingState_returnsPending()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        null,
        null,
        null,
        null
    ) {
      @Override
      protected State getState()
      {
        return State.PENDING;
      }
    };

    workItem = new KubernetesWorkItem(task, null, peonLifecycle);

    Assert.assertEquals(RunnerTaskState.PENDING, workItem.getRunnerTaskState());
  }

  @Test
  public void test_getRunnerTaskState_withKubernetesPeonLifecycle_inRunningState_returnsRunning()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        null,
        null,
        null,
        null
    ) {
      @Override
      protected State getState()
      {
        return State.RUNNING;
      }
    };

    workItem = new KubernetesWorkItem(task, null, peonLifecycle);

    Assert.assertEquals(RunnerTaskState.RUNNING, workItem.getRunnerTaskState());
  }

  @Test
  public void test_getRunnerTaskState_withKubernetesPeonLifecycle_inStoppedState_returnsNone()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        null,
        null,
        null,
        null
    ) {
      @Override
      protected State getState()
      {
        return State.STOPPED;
      }
    };

    workItem = new KubernetesWorkItem(task, null, peonLifecycle);

    Assert.assertEquals(RunnerTaskState.NONE, workItem.getRunnerTaskState());
  }

  @Test
  public void test_streamTaskLogs_withKubernetesPeonLifecycle()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        null,
        null,
        null,
        null
    );
    workItem = new KubernetesWorkItem(task, null, peonLifecycle);
    Assert.assertFalse(workItem.streamTaskLogs().isPresent());
  }

  @Test
  public void test_getLocation_withKubernetesPeonLifecycle()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        null,
        null,
        null,
        null
    );
    workItem = new KubernetesWorkItem(task, null, peonLifecycle);

    Assert.assertEquals(TaskLocation.unknown(), workItem.getLocation());
  }

  @Test
  public void test_getTaskType()
  {
    workItem = new KubernetesWorkItem(task, null, kubernetesPeonLifecycle);
    Assert.assertEquals(task.getType(), workItem.getTaskType());
  }

  @Test
  public void test_getDataSource()
  {
    workItem = new KubernetesWorkItem(task, null, kubernetesPeonLifecycle);
    Assert.assertEquals(task.getDataSource(), workItem.getDataSource());
  }

  @Test
  public void test_getTask()
  {
    workItem = new KubernetesWorkItem(task, null, kubernetesPeonLifecycle);
    Assert.assertEquals(task, workItem.getTask());
  }

  @Test
  public void test_peonLifeycle()
  {
    workItem = new KubernetesWorkItem(task, null, kubernetesPeonLifecycle);
    Assert.assertEquals(kubernetesPeonLifecycle, workItem.getPeonLifeycle());
  }
}

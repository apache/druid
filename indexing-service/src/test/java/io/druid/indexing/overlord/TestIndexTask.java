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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SetLockCriticalStateAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskLockCriticalState;
import io.druid.indexing.common.task.IndexTask;
import io.druid.indexing.common.task.TaskResource;

import java.util.concurrent.CountDownLatch;

/**
 * Task that simulates IndexTask with some instrumentation, used for unit testing.
 * */
@JsonTypeName("test_index")
public class TestIndexTask extends IndexTask
{
  private CountDownLatch lockAcquisitionCountDownLatch;
  private CountDownLatch runStartAwaitLatch;
  private CountDownLatch runFinishAwaitLatch;
  private CountDownLatch initialLockUpgradeCountDownLatch;
  private boolean alternateTaskLockState;

  @JsonCreator
  public TestIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("spec") IndexIngestionSpec ingestionSchema,
      @JacksonInject ObjectMapper jsonMapper
  ){
    super(id, taskResource, ingestionSchema, jsonMapper, null);
  }
  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception {
    boolean retVal = super.isReady(taskActionClient);
    if(lockAcquisitionCountDownLatch != null) {
      lockAcquisitionCountDownLatch.countDown();
    }
    return retVal;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    if(runStartAwaitLatch != null) {
      runStartAwaitLatch.await();
    }
    TaskStatus retVal = super.run(toolbox);

    // Lock was upgraded in super.run (above statement)
    if(initialLockUpgradeCountDownLatch != null) {
      initialLockUpgradeCountDownLatch.countDown();
    }

    if (alternateTaskLockState && runFinishAwaitLatch != null) {
      while (runFinishAwaitLatch.getCount() > 0) {
        if (!toolbox.getTaskActionClient().submit(
            new SetLockCriticalStateAction(getInterval(), TaskLockCriticalState.DOWNGRADE))) {
          // set custom message instead of task id inside task status so that
          // the cause of failure can be ascertained
          return TaskStatus.failure("We should not fail here");
        }
        if (!toolbox.getTaskActionClient().submit(
            new SetLockCriticalStateAction(getInterval(), TaskLockCriticalState.UPGRADE))) {
          return TaskStatus.failure(getId());
        }
        Thread.sleep(10);
      }
    }

    if(runFinishAwaitLatch != null) {
      runFinishAwaitLatch.await();
    }

    return retVal;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "test_index";
  }

  public void setLatches(CountDownLatch lockAcquisitionCountDownLatch,
                         CountDownLatch runAwaitLatch,
                         CountDownLatch runFinishAwaitLatch,
                         CountDownLatch initialLockUpgradeCountDownLatch) {
    this.lockAcquisitionCountDownLatch = lockAcquisitionCountDownLatch;
    this.runStartAwaitLatch = runAwaitLatch;
    this.runFinishAwaitLatch = runFinishAwaitLatch;
    this.initialLockUpgradeCountDownLatch = initialLockUpgradeCountDownLatch;
  }

  public void setAlternateTaskLockState(boolean alternateTaskLockState) {
    this.alternateTaskLockState = alternateTaskLockState;
  }
}

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


package org.apache.druid.indexing.seekablestream.supervisor;

import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.seekablestream.SeekableStreamSupervisorConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SeekableStreamSupervisorStateManagerTest
{
  private SeekableStreamSupervisorStateManager stateManager;
  private static SeekableStreamSupervisorConfig config;

  @BeforeClass
  public static void setupClass()
  {
    config = new SeekableStreamSupervisorConfig();

  }

  @Before
  public void setupTest()
  {
    stateManager = new SeekableStreamSupervisorStateManager(
        SeekableStreamSupervisorStateManager.State.WAITING_TO_RUN,
        new SeekableStreamSupervisorConfig()
    );
  }

  @Test
  public void testHappyPath()
  {
    stateManager.setStateIfNoSuccessfulRunYet(SeekableStreamSupervisorStateManager.State.CONNECTING_TO_STREAM);
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.CONNECTING_TO_STREAM,
        stateManager.getSupervisorState()
    );
    stateManager.setStateIfNoSuccessfulRunYet(SeekableStreamSupervisorStateManager.State.DISCOVERING_INITIAL_TASKS);
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.DISCOVERING_INITIAL_TASKS,
        stateManager.getSupervisorState()
    );
    stateManager.setStateIfNoSuccessfulRunYet(SeekableStreamSupervisorStateManager.State.CREATING_TASKS);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.CREATING_TASKS, stateManager.getSupervisorState());
    stateManager.setState(SeekableStreamSupervisorStateManager.State.RUNNING);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
    stateManager.markRunFinishedAndEvaluateHealth();

    stateManager.setStateIfNoSuccessfulRunYet(SeekableStreamSupervisorStateManager.State.CONNECTING_TO_STREAM);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
    stateManager.setStateIfNoSuccessfulRunYet(SeekableStreamSupervisorStateManager.State.DISCOVERING_INITIAL_TASKS);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
    stateManager.setStateIfNoSuccessfulRunYet(SeekableStreamSupervisorStateManager.State.CREATING_TASKS);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
    stateManager.setState(SeekableStreamSupervisorStateManager.State.RUNNING);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
    stateManager.markRunFinishedAndEvaluateHealth();
  }

  @Test
  public void testTransientStreamFailure()
  {

  }

  @Test
  public void testNonTransientStreamFailure()
  {

  }

  @Test
  public void testNonTransientUnhealthiness()
  {
    stateManager.setState(SeekableStreamSupervisorStateManager.State.RUNNING);
    for (int i = 0; i < config.getSupervisorUnhealthinessThreshold(); i++) {
      Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
      stateManager.storeThrowableEvent(new NullPointerException("someone goofed"));
      stateManager.markRunFinishedAndEvaluateHealth();
    }
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.UNHEALTHY_SUPERVISOR,
        stateManager.getSupervisorState()
    );
  }

  @Test
  public void testTransientUnhealthinessAndRecovery()
  {
    stateManager.setState(SeekableStreamSupervisorStateManager.State.RUNNING);
    for (int i = 0; i < config.getSupervisorUnhealthinessThreshold() - 1; i++) {
      stateManager.storeThrowableEvent(new NullPointerException("someone goofed"));
      stateManager.markRunFinishedAndEvaluateHealth();
      Assert.assertEquals(
          SeekableStreamSupervisorStateManager.State.RUNNING,
          stateManager.getSupervisorState()
      );
    }
    stateManager.markRunFinishedAndEvaluateHealth(); // clean run
    for (int i = 0; i < config.getSupervisorUnhealthinessThreshold() - 1; i++) {
      stateManager.storeThrowableEvent(new NullPointerException("someone goofed"));
      stateManager.markRunFinishedAndEvaluateHealth();
      Assert.assertEquals(
          SeekableStreamSupervisorStateManager.State.RUNNING,
          stateManager.getSupervisorState()
      );
    }
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.RUNNING,
        stateManager.getSupervisorState()
    );
    stateManager.markRunFinishedAndEvaluateHealth(); // clean run
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.RUNNING,
        stateManager.getSupervisorState()
    );
  }

  @Test
  public void testNonTransientTaskUnhealthiness()
  {
    for (int i = 0; i < config.getSupervisorTaskUnhealthinessThreshold(); i++) {
      Assert.assertNotEquals(
          stateManager.getSupervisorState(),
          SeekableStreamSupervisorStateManager.State.UNHEALTHY_TASKS
      );
      stateManager.storeCompletedTaskState(TaskState.FAILED);
      stateManager.markRunFinishedAndEvaluateHealth();
    }
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.UNHEALTHY_TASKS,
        stateManager.getSupervisorState()
    );
  }

  @Test
  public void testTransientTaskUnhealthiness()
  {

  }

  @Test
  public void testTwoUnhealthyStates() // priority check
  {

  }
}

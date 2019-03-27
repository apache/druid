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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SeekableStreamSupervisorStateManagerTest
{
  SeekableStreamSupervisorStateManager stateManager;

  @Before
  public void setup()
  {
    stateManager = new SeekableStreamSupervisorStateManager(
        SeekableStreamSupervisorStateManager.State.WAITING_TO_RUN,
        1,
        1,
        1,
        1
    );
  }

  @Test
  public void testHappyPath()
  {
    stateManager.setStateIfNoSuccessfulRunYet(SeekableStreamSupervisorStateManager.State.CONNECTING_TO_STREAM);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.CONNECTING_TO_STREAM, stateManager.getState());
    stateManager.setStateIfNoSuccessfulRunYet(SeekableStreamSupervisorStateManager.State.DISCOVERING_INITIAL_TASKS);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.DISCOVERING_INITIAL_TASKS, stateManager.getState());
    stateManager.setStateIfNoSuccessfulRunYet(SeekableStreamSupervisorStateManager.State.CREATING_TASKS);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.CREATING_TASKS, stateManager.getState());
    stateManager.setState(SeekableStreamSupervisorStateManager.State.RUNNING);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getState());
    stateManager.markRunFinishedAndEvaluateHealth();

    stateManager.setStateIfNoSuccessfulRunYet(SeekableStreamSupervisorStateManager.State.CONNECTING_TO_STREAM);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getState());
    stateManager.setStateIfNoSuccessfulRunYet(SeekableStreamSupervisorStateManager.State.DISCOVERING_INITIAL_TASKS);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getState());
    stateManager.setStateIfNoSuccessfulRunYet(SeekableStreamSupervisorStateManager.State.CREATING_TASKS);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getState());
    stateManager.setState(SeekableStreamSupervisorStateManager.State.RUNNING);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getState());
    stateManager.markRunFinishedAndEvaluateHealth();
  }

  @Test
  public void testTransientStreamFailure()
  {

  }
}

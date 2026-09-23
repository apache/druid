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

package org.apache.druid.indexing.overlord.supervisor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class SupervisorStateManagerTest
{
  SupervisorStateManagerConfig stateManagerConfig;

  @Test
  public void testMarkRunFinishedIfSupervisorIsIdle()
  {
    stateManagerConfig = new SupervisorStateManagerConfig();
    SupervisorStateManager supervisorStateManager = new SupervisorStateManager(
        stateManagerConfig,
        false
    );

    Assertions.assertFalse(stateManagerConfig.isIdleConfigEnabled());
    Assertions.assertEquals(600000, stateManagerConfig.getInactiveAfterMillis());

    supervisorStateManager.markRunFinished();

    Assertions.assertEquals(SupervisorStateManager.BasicState.RUNNING, supervisorStateManager.getSupervisorState());

    supervisorStateManager.maybeSetState(SupervisorStateManager.BasicState.IDLE);
    supervisorStateManager.markRunFinished();

    Assertions.assertEquals(SupervisorStateManager.BasicState.IDLE, supervisorStateManager.getSupervisorState());
  }

  @Test
  public void testIdleConfigSerde()
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    Map<String, String> config = ImmutableMap.of(
        "idleConfig.enabled", "true",
        "idleConfig.inactiveAfterMillis", "60000"
    );
    stateManagerConfig = mapper.convertValue(config, SupervisorStateManagerConfig.class);

    Assertions.assertTrue(stateManagerConfig.isIdleConfigEnabled());
    Assertions.assertEquals(60000, stateManagerConfig.getInactiveAfterMillis());
  }

  @Test
  public void testStoppingStateIsTerminal()
  {
    stateManagerConfig = new SupervisorStateManagerConfig();
    SupervisorStateManager supervisorStateManager = new SupervisorStateManager(
        stateManagerConfig,
        false
    );

    // Start in PENDING state
    Assertions.assertEquals(SupervisorStateManager.BasicState.PENDING, supervisorStateManager.getSupervisorState());

    // Transition to STOPPING
    supervisorStateManager.maybeSetState(SupervisorStateManager.BasicState.STOPPING);
    Assertions.assertEquals(SupervisorStateManager.BasicState.STOPPING, supervisorStateManager.getSupervisorState());

    // Attempt to transition out of STOPPING should be ignored
    supervisorStateManager.maybeSetState(SupervisorStateManager.BasicState.RUNNING);
    Assertions.assertEquals(SupervisorStateManager.BasicState.STOPPING, supervisorStateManager.getSupervisorState());

    supervisorStateManager.maybeSetState(SupervisorStateManager.BasicState.IDLE);
    Assertions.assertEquals(SupervisorStateManager.BasicState.STOPPING, supervisorStateManager.getSupervisorState());

    // Cannot transition to COMPLETED from STOPPING
    supervisorStateManager.maybeSetState(SupervisorStateManager.BasicState.COMPLETED);
    Assertions.assertEquals(SupervisorStateManager.BasicState.STOPPING, supervisorStateManager.getSupervisorState());
  }

  @Test
  public void testCompletedStateIsHealthy()
  {
    stateManagerConfig = new SupervisorStateManagerConfig();
    SupervisorStateManager supervisorStateManager = new SupervisorStateManager(
        stateManagerConfig,
        false
    );

    supervisorStateManager.maybeSetState(SupervisorStateManager.BasicState.COMPLETED);

    Assertions.assertTrue(supervisorStateManager.isHealthy());
    Assertions.assertEquals(SupervisorStateManager.BasicState.COMPLETED, supervisorStateManager.getSupervisorState());
  }

  @Test
  public void testCompletedStateIsNotFirstRunOnly()
  {
    stateManagerConfig = new SupervisorStateManagerConfig();
    SupervisorStateManager supervisorStateManager = new SupervisorStateManager(
        stateManagerConfig,
        false
    );

    supervisorStateManager.maybeSetState(SupervisorStateManager.BasicState.COMPLETED);

    Assertions.assertFalse(SupervisorStateManager.BasicState.COMPLETED.isFirstRunOnly());
  }

  @Test
  public void testMarkRunFinished_completedStateSkipsHealthyCheck()
  {
    stateManagerConfig = new SupervisorStateManagerConfig();
    SupervisorStateManager supervisorStateManager = new SupervisorStateManager(
        stateManagerConfig,
        false
    );

    supervisorStateManager.maybeSetState(SupervisorStateManager.BasicState.COMPLETED);
    supervisorStateManager.markRunFinished();

    Assertions.assertEquals(SupervisorStateManager.BasicState.COMPLETED, supervisorStateManager.getSupervisorState());
  }
}

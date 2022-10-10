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
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SupervisorStateManagerTest
{
  private final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testMarkRunFinishedIfSupervisorIsIdle()
  {
    Map<String, Object> config = new HashMap<>();
    config.put("enableIdleBehaviour", true);
    SupervisorStateManagerConfig supervisorStateManagerConfig =
        OBJECT_MAPPER.convertValue(config, SupervisorStateManagerConfig.class);
    SupervisorStateManager supervisorStateManager = new SupervisorStateManager(
        supervisorStateManagerConfig,
        false
    );

    supervisorStateManager.maybeSetState(SupervisorStateManager.BasicState.IDLE);
    supervisorStateManager.markRunFinished();

    Assert.assertTrue(supervisorStateManager.isEnableIdleBehaviour());
    Assert.assertEquals(SupervisorStateManager.BasicState.IDLE, supervisorStateManager.getSupervisorState());
  }

  @Test
  public void testMarkRunFinishedIfSupervisorNotIdle()
  {
    SupervisorStateManager supervisorStateManager = new SupervisorStateManager(
        new SupervisorStateManagerConfig(),
        false
    );

    supervisorStateManager.markRunFinished();

    Assert.assertFalse(supervisorStateManager.isEnableIdleBehaviour());
    Assert.assertEquals(SupervisorStateManager.BasicState.RUNNING, supervisorStateManager.getSupervisorState());
  }
}

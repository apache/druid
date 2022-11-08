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
import org.junit.Assert;
import org.junit.Test;

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

    Assert.assertFalse(stateManagerConfig.isIdleConfigEnabled());
    Assert.assertEquals(600000, stateManagerConfig.getInactiveAfterMillis());

    supervisorStateManager.markRunFinished();

    Assert.assertEquals(SupervisorStateManager.BasicState.RUNNING, supervisorStateManager.getSupervisorState());

    supervisorStateManager.maybeSetState(SupervisorStateManager.BasicState.IDLE);
    supervisorStateManager.markRunFinished();

    Assert.assertEquals(SupervisorStateManager.BasicState.IDLE, supervisorStateManager.getSupervisorState());
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

    Assert.assertTrue(stateManagerConfig.isIdleConfigEnabled());
    Assert.assertEquals(60000, stateManagerConfig.getInactiveAfterMillis());
  }
}

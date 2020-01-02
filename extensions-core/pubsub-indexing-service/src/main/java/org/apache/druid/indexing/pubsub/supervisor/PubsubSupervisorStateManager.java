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

package org.apache.druid.indexing.pubsub.supervisor;

import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;

public class PubsubSupervisorStateManager extends SupervisorStateManager
{
  public PubsubSupervisorStateManager(SupervisorStateManagerConfig supervisorConfig, boolean suspended)
  {
    super(supervisorConfig, suspended);
  }

  @Override
  protected State getSpecificUnhealthySupervisorState()
  {
    return BasicState.UNHEALTHY_SUPERVISOR;
  }

  public enum PubsubState implements State
  {
    UNABLE_TO_CONNECT_TO_STREAM(false, true),
    LOST_CONTACT_WITH_STREAM(false, false),

    CONNECTING_TO_STREAM(true, true),
    DISCOVERING_INITIAL_TASKS(true, true),
    CREATING_TASKS(true, true);

    private final boolean healthy;
    private final boolean firstRunOnly;

    PubsubState(boolean healthy, boolean firstRunOnly)
    {
      this.healthy = healthy;
      this.firstRunOnly = firstRunOnly;
    }

    @Override
    public boolean isHealthy()
    {
      return healthy;
    }

    @Override
    public boolean isFirstRunOnly()
    {
      return firstRunOnly;
    }

    @Override
    public State getBasicState()
    {
      return healthy ? BasicState.RUNNING : BasicState.UNHEALTHY_SUPERVISOR;
    }
  }

}

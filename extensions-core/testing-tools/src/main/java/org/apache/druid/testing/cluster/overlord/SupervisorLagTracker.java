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

package org.apache.druid.testing.cluster.overlord;

import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks {@link LagStats} in memory for supervisor IDs. Used in embedded tests.
 */
public class SupervisorLagTracker
{
  private final ConcurrentHashMap<String, LagStats> supervisorIdToLag = new ConcurrentHashMap<>();

  public void setLag(String supervisorId, LagStats lag)
  {
    supervisorIdToLag.put(supervisorId, lag);
  }

  public LagStats getLag(String supervisorId)
  {
    return supervisorIdToLag.get(supervisorId);
  }
}

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

package org.apache.druid.server.coordinator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manager to store and update the status of ongoing cloning operations.
 */
public class CloneStatusManager
{
  private final AtomicReference<Map<String, ServerCloneStatus>> cloneStatusSnapshot;

  public CloneStatusManager()
  {
    this.cloneStatusSnapshot = new AtomicReference<>(Map.of());
  }

  /**
   * Returns the status of cloning as a list of {@link ServerCloneStatus}.
   */
  public List<ServerCloneStatus> getStatusForAllServers()
  {
    return ImmutableList.copyOf(cloneStatusSnapshot.get().values());
  }

  /**
   * Returns the status of cloning as a {@link ServerCloneStatus} for a specific target server.
   */
  @Nullable
  public ServerCloneStatus getStatusForServer(String targetServer)
  {
    return cloneStatusSnapshot.get().get(targetServer);
  }

  /**
   * Updates the stored status with the provided parameter.
   */
  public void updateStatus(Map<String, ServerCloneStatus> newStatusMap)
  {
    cloneStatusSnapshot.set(ImmutableMap.copyOf(newStatusMap));
  }
}


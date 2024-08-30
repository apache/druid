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

package org.apache.druid.indexing.compact;

import org.apache.druid.server.compaction.CompactionSimulateResult;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.CompactionSupervisorConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;

import java.util.Map;

/**
 * Compaction scheduler that runs on the Overlord if {@link CompactionSupervisorConfig}
 * is enabled.
 * <p>
 * Usage:
 * <ul>
 * <li>When an active {@link CompactionSupervisor} starts, it should register
 * itself by calling {@link #startCompaction}.</li>
 * <li>When a suspended {@link CompactionSupervisor} starts, it should stop
 * compaction by calling {@link #stopCompaction}.</li>
 * <li>When stopping, any {@link CompactionSupervisor} (active or suspended)
 * should call {@link #stopCompaction}.</li>
 * </ul>
 */
public interface CompactionScheduler
{
  void start();

  void stop();

  boolean isRunning();

  CompactionConfigValidationResult validateCompactionConfig(DataSourceCompactionConfig compactionConfig);

  void startCompaction(String dataSourceName, DataSourceCompactionConfig compactionConfig);

  void stopCompaction(String dataSourceName);

  Map<String, AutoCompactionSnapshot> getAllCompactionSnapshots();

  AutoCompactionSnapshot getCompactionSnapshot(String dataSource);

  CompactionSimulateResult simulateRunWithConfigUpdate(ClusterCompactionConfig updateRequest);

}

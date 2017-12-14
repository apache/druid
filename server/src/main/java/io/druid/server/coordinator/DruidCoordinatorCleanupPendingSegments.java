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

package io.druid.server.coordinator;

import com.google.inject.Inject;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.indexer.TaskStatusPlus;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.coordinator.helper.DruidCoordinatorHelper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class DruidCoordinatorCleanupPendingSegments implements DruidCoordinatorHelper
{
  private static final Logger log = new Logger(DruidCoordinatorCleanupPendingSegments.class);

  private final IndexingServiceClient indexingServiceClient;

  @Inject
  public DruidCoordinatorCleanupPendingSegments(IndexingServiceClient indexingServiceClient)
  {
    this.indexingServiceClient = indexingServiceClient;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final List<TaskStatusPlus> taskStatusPluses = new ArrayList<>();
    taskStatusPluses.addAll(indexingServiceClient.getRunningTasks());
    taskStatusPluses.addAll(indexingServiceClient.getPendingTasks());
    taskStatusPluses.addAll(indexingServiceClient.getWaitingTasks());

    final TaskStatusPlus completeTaskStatus = indexingServiceClient.getLastCompleteTask();
    if (completeTaskStatus != null) {
      taskStatusPluses.add(completeTaskStatus);
    }
    taskStatusPluses.sort(Comparator.comparing(TaskStatusPlus::getCreatedTime));

    if (!taskStatusPluses.isEmpty()) {
      final TaskStatusPlus firstCreatedTaskStatus = taskStatusPluses.get(0);
      for (ImmutableDruidDataSource dataSource : params.getDataSources()) {
        if (!params.getCoordinatorDynamicConfig().getKillPendingSegmentsSkipList().contains(dataSource.getName())) {
          log.info(
              "Kill pendingSegments created until [%s] for dataSource[%s]",
              dataSource,
              firstCreatedTaskStatus.getCreatedTime()
          );
          indexingServiceClient.killPendingSegments(dataSource.getName(), firstCreatedTaskStatus.getCreatedTime());
        }
      }
    }
    return params;
  }
}

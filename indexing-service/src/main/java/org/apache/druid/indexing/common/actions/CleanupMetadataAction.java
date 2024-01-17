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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.indexing.common.task.Task;

/**
 * Launched by a task to clean up stale metadata upon completion
 * Currently, this action cleans up the entries in upgradeSegments metadata table
 */
public class CleanupMetadataAction implements TaskAction<CleanupMetadataResult>
{
  @Override
  public TypeReference<CleanupMetadataResult> getReturnTypeReference()
  {
    return new TypeReference<CleanupMetadataResult>()
    {
    };
  }

  @Override
  public CleanupMetadataResult perform(Task task, TaskActionToolbox toolbox)
  {
    // TODO delete only for Index, ParallelIndexSupervisor and MSQController tasks
    final int deletedUpgradeSegmentEntries = toolbox.getIndexerMetadataStorageCoordinator()
                                                    .deleteUpgradeSegmentsForTask(task.getId());
    return new CleanupMetadataResult(deletedUpgradeSegmentEntries);
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "CleanupMetadataAction{" +
           '}';
  }
}

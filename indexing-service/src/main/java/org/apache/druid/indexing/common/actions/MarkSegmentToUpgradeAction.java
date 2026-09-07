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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.timeline.DataSegment;

import java.util.Map;
import java.util.Set;

/**
 * Task action that records segments as being upgraded in the metadata store.
 * <p>
 * This action is used during compaction to track which segments are being replaced.
 * It validates that all segments to be upgraded are covered by
 * {@link ReplaceTaskLock}s before inserting them into the upgrade segments table.
 * <p>
 * The action will fail if any of the upgrade segments do not have a corresponding
 * replace lock, ensuring that only properly locked segments can be marked for upgrade.
 *
 * @return the number of segments successfully inserted into the upgrade segments table
 */
public class MarkSegmentToUpgradeAction implements TaskAction<Integer>
{
  private final String dataSource;
  private final Set<DataSegment> upgradeSegments;

  /**
   * @param dataSource the datasource containing the segments to upgrade
   * @param upgradeSegments the set of segments to be recorded as upgraded
   */
  @JsonCreator
  public MarkSegmentToUpgradeAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("upgradeSegments") Set<DataSegment> upgradeSegments
  )
  {
    this.dataSource = dataSource;
    this.upgradeSegments = upgradeSegments;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Set<DataSegment> getUpgradeSegments()
  {
    return upgradeSegments;
  }

  @Override
  public TypeReference<Integer> getReturnTypeReference()
  {
    return new TypeReference<>()
    {
    };
  }

  @Override
  public Integer perform(Task task, TaskActionToolbox toolbox)
  {
    final String datasource = task.getDataSource();
    final Map<DataSegment, ReplaceTaskLock> segmentToReplaceLock
        = TaskLocks.findReplaceLocksCoveringSegments(datasource, toolbox.getTaskLockbox(), upgradeSegments);

    if (segmentToReplaceLock.size() < upgradeSegments.size()) {
      throw InvalidInput.exception(
          "Segments to upgrade must be covered by a REPLACE lock. Only [%d] out of [%d] segments are covered.",
          segmentToReplaceLock.size(),
          upgradeSegments.size()
      );
    }

    return toolbox.getIndexerMetadataStorageCoordinator()
                  .insertIntoUpgradeSegmentsTable(segmentToReplaceLock);
  }
}

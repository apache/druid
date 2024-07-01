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
import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.metadata.SegmentUpgradeInfo;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Task action to retrieve the segment upgrade infos of a given set of used or unused upgraded segment ids.
 */
public class RetrieveUpgradedToSegmentsIdsAction implements TaskAction<List<SegmentUpgradeInfo>>
{
  private final String dataSource;
  private final Set<String> upgradedSegmentIds;

  @JsonCreator
  public RetrieveUpgradedToSegmentsIdsAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("upgradedSegmentIds") Set<String> upgradedSegmentIds
  )
  {
    this.dataSource = dataSource;
    this.upgradedSegmentIds = upgradedSegmentIds;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Set<String> getUpgradedSegmentIds()
  {
    return upgradedSegmentIds;
  }

  @Override
  public TypeReference<List<SegmentUpgradeInfo>> getReturnTypeReference()
  {
    return new TypeReference<List<SegmentUpgradeInfo>>()
    {
    };
  }

  @Override
  public List<SegmentUpgradeInfo> perform(Task task, TaskActionToolbox toolbox)
  {
    return toolbox.getIndexerMetadataStorageCoordinator()
                  .retrieveUpgradedToSegmentIds(dataSource, ImmutableList.copyOf(getUpgradedSegmentIds()));
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RetrieveUpgradedToSegmentsIdsAction that = (RetrieveUpgradedToSegmentsIdsAction) o;
    return Objects.equals(dataSource, that.dataSource) && Objects.equals(upgradedSegmentIds, that.upgradedSegmentIds);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, upgradedSegmentIds);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "dataSource='" + dataSource + '\'' +
           ", upgradedSegmentIds=" + upgradedSegmentIds +
           '}';
  }
}

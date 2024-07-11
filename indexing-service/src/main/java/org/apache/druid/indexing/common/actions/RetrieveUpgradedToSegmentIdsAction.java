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
import org.apache.druid.indexing.common.task.Task;

import java.util.Objects;
import java.util.Set;

/**
 * Task action to retrieve all the segment IDs to which a given set of segments were upgraded.
 */
public class RetrieveUpgradedToSegmentIdsAction implements TaskAction<UpgradedToSegmentsResponse>
{
  private final String dataSource;
  private final Set<String> segmentIds;

  @JsonCreator
  public RetrieveUpgradedToSegmentIdsAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segmentIds") Set<String> segmentIds
  )
  {
    this.dataSource = dataSource;
    this.segmentIds = segmentIds;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Set<String> getSegmentIds()
  {
    return segmentIds;
  }

  @Override
  public TypeReference<UpgradedToSegmentsResponse> getReturnTypeReference()
  {
    return new TypeReference<UpgradedToSegmentsResponse>()
    {
    };
  }

  @Override
  public UpgradedToSegmentsResponse perform(Task task, TaskActionToolbox toolbox)
  {
    return new UpgradedToSegmentsResponse(
        toolbox.getIndexerMetadataStorageCoordinator()
               .retrieveUpgradedToSegmentIds(dataSource, segmentIds)
    );
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
    RetrieveUpgradedToSegmentIdsAction that = (RetrieveUpgradedToSegmentIdsAction) o;
    return Objects.equals(dataSource, that.dataSource) && Objects.equals(segmentIds, that.segmentIds);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, segmentIds);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "dataSource='" + dataSource + '\'' +
           ", segmentIds=" + segmentIds +
           '}';
  }
}

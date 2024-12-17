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

import java.util.Set;

/**
 * Task action to determine the set of all segments containing the same load spec given the parent id. <br/>
 * Returns a map from a segment ID to a set containing:
 * <ol>
 * <li> all segment IDs that were upgraded from it AND are still present in the metadata store </li>
 * <li> the segment ID itself if and only if it is still present in the metadata store </li>
 * </ol>
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
    return new TypeReference<>() {};
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
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "dataSource='" + dataSource + '\'' +
           ", segmentIds=" + segmentIds +
           '}';
  }
}

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
import org.apache.druid.timeline.DataSegment;

import java.util.Objects;
import java.util.Set;

/**
 * Task action to retrieve segments from the metadata store. Matching segments
 * are returned regardless of their visibility i.e. visible, overshadowed or unused.
 */
public class RetrieveSegmentsByIdAction implements TaskAction<Set<DataSegment>>
{
  private final String dataSource;
  private final Set<String> segmentIds;

  @JsonCreator
  public RetrieveSegmentsByIdAction(
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
  public TypeReference<Set<DataSegment>> getReturnTypeReference()
  {
    return new TypeReference<>() {};
  }

  @Override
  public Set<DataSegment> perform(Task task, TaskActionToolbox toolbox)
  {
    return toolbox.getIndexerMetadataStorageCoordinator()
                  .retrieveSegmentsById(dataSource, segmentIds);
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
    RetrieveSegmentsByIdAction that = (RetrieveSegmentsByIdAction) o;
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

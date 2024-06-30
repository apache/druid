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
 * Given a set of segments, this task action determines the subset that can be killed from deep storage
 * This task action assumes that the all the segments provided are unused and belong to the same datasource.
 */
public class DetermineSegmentsToKillAction implements TaskAction<Set<DataSegment>>
{
  private final Set<DataSegment> segments;

  @JsonCreator
  public DetermineSegmentsToKillAction(@JsonProperty("segments") Set<DataSegment> segments)
  {
    this.segments = segments;
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  @Override
  public TypeReference<Set<DataSegment>> getReturnTypeReference()
  {
    return new TypeReference<Set<DataSegment>>()
    {
    };
  }

  @Override
  public Set<DataSegment> perform(Task task, TaskActionToolbox toolbox)
  {
    return toolbox.getIndexerMetadataStorageCoordinator()
                  .determineSegmentsWithUnreferencedLoadSpecs(segments);
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
    DetermineSegmentsToKillAction that = (DetermineSegmentsToKillAction) o;
    return Objects.equals(segments, that.segments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segments);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "segments=" + segments +
           '}';
  }
}

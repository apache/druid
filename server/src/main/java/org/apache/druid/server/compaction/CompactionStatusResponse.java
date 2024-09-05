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

package org.apache.druid.server.compaction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;

import java.util.Collection;
import java.util.Objects;

/**
 * Response of {@code /compaction/status} API exposed by Coordinator and
 * Overlord (when compaction supervisors are enabled).
 */
public class CompactionStatusResponse
{
  private final Collection<AutoCompactionSnapshot> latestStatus;
  
  @JsonCreator
  public CompactionStatusResponse(
      @JsonProperty("latestStatus") Collection<AutoCompactionSnapshot> latestStatus
  )
  {
    this.latestStatus = latestStatus;
  }

  @JsonProperty
  public Collection<AutoCompactionSnapshot> getLatestStatus()
  {
    return latestStatus;
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
    CompactionStatusResponse that = (CompactionStatusResponse) o;
    return Objects.equals(latestStatus, that.latestStatus);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(latestStatus);
  }
}

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

package io.druid.indexing.overlord.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.indexing.overlord.DataSourceMetadata;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Used as a tombstone marker in the supervisors metadata table to indicate that the supervisor has been removed.
 */
public class NoopSupervisorSpec implements SupervisorSpec
{
  // NoopSupervisorSpec is used as a tombstone, added when a previously running supervisor is stopped.
  // Inherit the datasources of the previous running spec, so that we can determine whether a user is authorized to see
  // this tombstone (users can only see tombstones for datasources that they have access to).
  @Nullable
  @JsonProperty("dataSources")
  private List<String> datasources;

  @Nullable
  @JsonProperty("id")
  private String id;

  @JsonCreator
  public NoopSupervisorSpec(
      @Nullable @JsonProperty("id") String id,
      @Nullable @JsonProperty("dataSources") List<String> datasources
  )
  {
    this.id = id;
    this.datasources = datasources == null ? new ArrayList<>() : datasources;
  }

  @Override
  @JsonProperty
  public String getId()
  {
    return id;
  }

  @Override
  public Supervisor createSupervisor()
  {
    return new Supervisor()
    {
      @Override
      public void start() {}

      @Override
      public void stop(boolean stopGracefully) {}

      @Override
      public SupervisorReport getStatus()
      {
        return null;
      }

      @Override
      public void reset(DataSourceMetadata dataSourceMetadata) {}

      @Override
      public void checkpoint(
          @Nullable String sequenceName,
          @Nullable DataSourceMetadata previousCheckPoint,
          @Nullable DataSourceMetadata currentCheckPoint
      )
      {

      }
    };
  }

  @Override
  @Nullable
  @JsonProperty("dataSources")
  public List<String> getDataSources()
  {
    return datasources;
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
    NoopSupervisorSpec spec = (NoopSupervisorSpec) o;
    return Objects.equals(datasources, spec.datasources) &&
           Objects.equals(getId(), spec.getId());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(datasources, getId());
  }
}

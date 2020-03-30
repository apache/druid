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

package org.apache.druid.indexing.overlord.supervisor;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * This class contains the attributes of a supervisor which are returned by the API's in
 * org.apache.druid.indexing.overlord.supervisor.SupervisorResource
 * and used by org.apache.druid.sql.calcite.schema.SystemSchema.SupervisorsTable
 */
@JsonDeserialize(builder = SupervisorStatus.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SupervisorStatus
{
  private final String id;
  private final String state;
  private final String detailedState;
  private final boolean healthy;
  private final SupervisorSpec spec;
  /**
   * This is a JSON representation of {@code spec}
   * The explicit serialization is present here so that users of {@code SupervisorStatus} which cannot
   * deserialize {@link SupervisorSpec} can use this attribute instead
   */
  private final String specString;
  private final String type;
  private final String source;
  private final boolean suspended;

  private SupervisorStatus(
      Builder builder
  )
  {
    this.id = Preconditions.checkNotNull(builder.id, "id");
    this.state = builder.state;
    this.detailedState = builder.detailedState;
    this.healthy = builder.healthy;
    this.spec = builder.spec;
    this.specString = builder.specString;
    this.type = builder.type;
    this.source = builder.source;
    this.suspended = builder.suspended;
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
    SupervisorStatus that = (SupervisorStatus) o;
    return healthy == that.healthy &&
           Objects.equals(id, that.id) &&
           Objects.equals(state, that.state) &&
           Objects.equals(detailedState, that.detailedState) &&
           Objects.equals(spec, that.spec) &&
           Objects.equals(specString, that.specString) &&
           Objects.equals(type, that.type) &&
           Objects.equals(source, that.source) &&
           suspended == that.suspended;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(id, state, detailedState, healthy, spec, specString, type, source, suspended);
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @JsonProperty
  public String getState()
  {
    return state;
  }

  @JsonProperty
  public String getDetailedState()
  {
    return detailedState;
  }

  @JsonProperty
  public boolean isHealthy()
  {
    return healthy;
  }

  @JsonProperty
  public SupervisorSpec getSpec()
  {
    return spec;
  }

  @JsonProperty
  public String getSpecString()
  {
    return specString;
  }

  @JsonProperty
  public String getType()
  {
    return type;
  }

  @JsonProperty
  public String getSource()
  {
    return source;
  }

  @JsonProperty
  public boolean isSuspended()
  {
    return suspended;
  }

  @JsonPOJOBuilder
  public static class Builder
  {
    @JsonProperty("id")
    private String id;
    @JsonProperty("state")
    private String state;
    @JsonProperty("detailedState")
    private String detailedState;
    @JsonProperty("healthy")
    private boolean healthy;
    @JsonProperty("spec")
    private SupervisorSpec spec;
    @JsonProperty("specString")
    private String specString;
    @JsonProperty("type")
    private String type;
    @JsonProperty("source")
    private String source;
    @JsonProperty("suspended")
    private boolean suspended;

    @JsonProperty
    public Builder withId(String id)
    {
      this.id = Preconditions.checkNotNull(id, "id");
      return this;
    }

    @JsonProperty
    public Builder withState(String state)
    {
      this.state = state;
      return this;
    }

    @JsonProperty
    public Builder withDetailedState(String detailedState)
    {
      this.detailedState = detailedState;
      return this;
    }

    @JsonProperty
    public Builder withHealthy(boolean healthy)
    {
      this.healthy = healthy;
      return this;
    }

    @JsonProperty
    public Builder withSpec(SupervisorSpec spec)
    {
      this.spec = spec;
      return this;
    }

    @JsonProperty
    public Builder withSpecString(String spec)
    {
      this.specString = spec;
      return this;
    }

    @JsonProperty
    public Builder withType(String type)
    {
      this.type = type;
      return this;
    }

    @JsonProperty
    public Builder withSource(String source)
    {
      this.source = source;
      return this;
    }

    @JsonProperty
    public Builder withSuspended(boolean suspended)
    {
      this.suspended = suspended;
      return this;
    }

    public SupervisorStatus build()
    {
      return new SupervisorStatus(this);
    }
  }
}


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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public class VersionedSupervisorSpec
{
  @Nullable
  private final SupervisorSpec spec;
  private final String version;

  @JsonCreator
  public VersionedSupervisorSpec(
      @JsonProperty("spec") @Nullable SupervisorSpec spec,
      @JsonProperty("version") String version
  )
  {
    this.spec = spec;
    this.version = version;
  }

  @JsonProperty
  @Nullable
  public SupervisorSpec getSpec()
  {
    return spec;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
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

    VersionedSupervisorSpec that = (VersionedSupervisorSpec) o;

    if (getSpec() != null ? !getSpec().equals(that.getSpec()) : that.getSpec() != null) {
      return false;
    }
    return getVersion() != null ? getVersion().equals(that.getVersion()) : that.getVersion() == null;

  }

  @Override
  public int hashCode()
  {
    int result = getSpec() != null ? getSpec().hashCode() : 0;
    result = 31 * result + (getVersion() != null ? getVersion().hashCode() : 0);
    return result;
  }
}

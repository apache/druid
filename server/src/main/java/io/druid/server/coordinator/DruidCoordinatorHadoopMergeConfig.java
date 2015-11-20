/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 */
public class DruidCoordinatorHadoopMergeConfig
{

  private final boolean keepGap;
  private final List<String> hadoopDependencyCoordinates;
  private final Map<String, Object> tuningConfig;
  private final List<CoordinatorHadoopMergeSpec> hadoopMergeSpecs;

  @JsonCreator
  public DruidCoordinatorHadoopMergeConfig(
      @JsonProperty("keepGap") boolean keepGap,
      @JsonProperty("hadoopDependencyCoordinates") List<String> hadoopDependencyCoordinates,
      @JsonProperty("tuningConfig") Map<String, Object> tuningConfig,
      @JsonProperty("hadoopMergeSpecs") List<CoordinatorHadoopMergeSpec> hadoopMergeSpecs
  )
  {
    this.keepGap = keepGap;
    this.hadoopDependencyCoordinates = hadoopDependencyCoordinates;
    this.tuningConfig = tuningConfig;
    this.hadoopMergeSpecs = hadoopMergeSpecs;
  }

  @JsonProperty
  public boolean isKeepGap()
  {
    return keepGap;
  }

  @JsonProperty
  public List<String> getHadoopDependencyCoordinates()
  {
    return hadoopDependencyCoordinates;
  }

  @JsonProperty
  public Map<String, Object> getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty
  public List<CoordinatorHadoopMergeSpec> getHadoopMergeSpecs()
  {
    return hadoopMergeSpecs;
  }

  @Override
  public String toString()
  {
    return "DruidCoordinatorHadoopMergeConfig{" +
           "keepGap=" + keepGap +
           ", hadoopDependencyCoordinates=" + hadoopDependencyCoordinates +
           ", tuningConfig=" + tuningConfig +
           ", hadoopMergeSpecs=" + hadoopMergeSpecs +
           '}';
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

    DruidCoordinatorHadoopMergeConfig that = (DruidCoordinatorHadoopMergeConfig) o;

    if (keepGap != that.keepGap) {
      return false;
    }
    if (hadoopDependencyCoordinates != null
        ? !hadoopDependencyCoordinates.equals(that.hadoopDependencyCoordinates)
        : that.hadoopDependencyCoordinates != null) {
      return false;
    }
    if (tuningConfig != null ? !tuningConfig.equals(that.tuningConfig) : that.tuningConfig != null) {
      return false;
    }
    return hadoopMergeSpecs != null ? hadoopMergeSpecs.equals(that.hadoopMergeSpecs) : that.hadoopMergeSpecs == null;

  }

  @Override
  public int hashCode()
  {
    int result = (keepGap ? 1 : 0);
    result = 31 * result + (hadoopDependencyCoordinates != null ? hadoopDependencyCoordinates.hashCode() : 0);
    result = 31 * result + (tuningConfig != null ? tuningConfig.hashCode() : 0);
    result = 31 * result + (hadoopMergeSpecs != null ? hadoopMergeSpecs.hashCode() : 0);
    return result;
  }
}

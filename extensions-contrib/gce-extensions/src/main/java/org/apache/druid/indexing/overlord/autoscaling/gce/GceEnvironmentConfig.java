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

package org.apache.druid.indexing.overlord.autoscaling.gce;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 */
public class GceEnvironmentConfig
{
  /**
   * numInstances: the number of workers to try to spawn at each call to provision
   * projectId: the id of the project where to operate
   * zoneName: the name of the zone where to operata
   * instanceTemplate: the template to use when creating the instances
   * minworkers: the minimum number of workers in the pool (*)
   * maxWorkers: the maximum number of workers in the pool (*)
   *
   * (*) both used by the caller of the AutoScaler to know if it makes sense to call
   *     provision / terminate or if there is no hope that something would be done
   */
  private final int numInstances;
  private final String projectId;
  private final String zoneName;
  private final String managedInstanceGroupName;

  @JsonCreator
  public GceEnvironmentConfig(
          @JsonProperty("numInstances") int numInstances,
          @JsonProperty("projectId") String projectId,
          @JsonProperty("zoneName") String zoneName,
          @JsonProperty("managedInstanceGroupName") String managedInstanceGroupName
  )
  {
    Preconditions.checkArgument(numInstances > 0,
                                "numInstances must be greater than 0");
    this.numInstances = numInstances;
    this.projectId = Preconditions.checkNotNull(projectId,
                                                "projectId must be not null");
    this.zoneName = Preconditions.checkNotNull(zoneName,
                                               "zoneName nust be not null");
    this.managedInstanceGroupName = Preconditions.checkNotNull(
            managedInstanceGroupName,
            "managedInstanceGroupName must be not null"
    );
  }

  @JsonProperty
  public int getNumInstances()
  {
    return numInstances;
  }


  @JsonProperty
  String getZoneName()
  {
    return zoneName;
  }

  @JsonProperty
  String getProjectId()
  {
    return projectId;
  }

  @JsonProperty
  String getManagedInstanceGroupName()
  {
    return managedInstanceGroupName;
  }

  @Override
  public String toString()
  {
    return "GceEnvironmentConfig={" +
            "projectId=" + projectId +
            ", zoneName=" + zoneName +
            ", numInstances=" + numInstances +
            ", managedInstanceGroupName=" + managedInstanceGroupName +
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

    GceEnvironmentConfig that = (GceEnvironmentConfig) o;
    return (numInstances == that.numInstances &&
            projectId.equals(that.projectId) &&
            zoneName.equals(that.zoneName) &&
            managedInstanceGroupName.equals(that.managedInstanceGroupName));
  }

  @Override
  public int hashCode()
  {
    int result = 0;
    result = 31 * result + Objects.hashCode(projectId);
    result = 31 * result + Objects.hashCode(zoneName);
    result = 31 * result + Objects.hashCode(managedInstanceGroupName);
    result = 31 * result + numInstances;
    return result;
  }
}

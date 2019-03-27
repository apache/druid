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

package org.apache.druid.indexing.overlord.autoscaling.ec2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class EC2EnvironmentConfig
{
  private final String availabilityZone;
  private final EC2NodeData nodeData;
  private final EC2UserData userData;

  @JsonCreator
  public EC2EnvironmentConfig(
      @JsonProperty("availabilityZone") String availabilityZone,
      @JsonProperty("nodeData") EC2NodeData nodeData,
      @JsonProperty("userData") EC2UserData userData
  )
  {
    this.availabilityZone = availabilityZone;
    this.nodeData = nodeData;
    this.userData = userData;
  }

  @JsonProperty
  public String getAvailabilityZone()
  {
    return availabilityZone;
  }

  @JsonProperty
  public EC2NodeData getNodeData()
  {
    return nodeData;
  }

  @JsonProperty
  public EC2UserData getUserData()
  {
    return userData;
  }

  @Override
  public String toString()
  {
    return "EC2EnvironmentConfig{" +
           "availabilityZone='" + availabilityZone + '\'' +
           ", nodeData=" + nodeData +
           ", userData=" + userData +
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

    EC2EnvironmentConfig that = (EC2EnvironmentConfig) o;

    if (availabilityZone != null ? !availabilityZone.equals(that.availabilityZone) : that.availabilityZone != null) {
      return false;
    }
    if (nodeData != null ? !nodeData.equals(that.nodeData) : that.nodeData != null) {
      return false;
    }
    if (userData != null ? !userData.equals(that.userData) : that.userData != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = availabilityZone != null ? availabilityZone.hashCode() : 0;
    result = 31 * result + (nodeData != null ? nodeData.hashCode() : 0);
    result = 31 * result + (userData != null ? userData.hashCode() : 0);
    return result;
  }
}

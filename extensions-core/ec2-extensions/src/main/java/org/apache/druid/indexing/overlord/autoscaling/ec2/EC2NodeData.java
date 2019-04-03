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

import java.util.List;

/**
 */
public class EC2NodeData
{
  private final String amiId;
  private final String instanceType;
  private final int minInstances;
  private final int maxInstances;
  private final List<String> securityGroupIds;
  private final String keyName;
  private final String subnetId;
  private final EC2IamProfileData iamProfile;
  private final Boolean associatePublicIpAddress;

  @JsonCreator
  public EC2NodeData(
      @JsonProperty("amiId") String amiId,
      @JsonProperty("instanceType") String instanceType,
      @JsonProperty("minInstances") int minInstances,
      @JsonProperty("maxInstances") int maxInstances,
      @JsonProperty("securityGroupIds") List<String> securityGroupIds,
      @JsonProperty("keyName") String keyName,
      @JsonProperty("subnetId") String subnetId,
      @JsonProperty("iamProfile") EC2IamProfileData iamProfile,
      @JsonProperty("associatePublicIpAddress") Boolean associatePublicIpAddress
  )
  {
    this.amiId = amiId;
    this.instanceType = instanceType;
    this.minInstances = minInstances;
    this.maxInstances = maxInstances;
    this.securityGroupIds = securityGroupIds;
    this.keyName = keyName;
    this.subnetId = subnetId;
    this.iamProfile = iamProfile;
    this.associatePublicIpAddress = associatePublicIpAddress;
  }

  @JsonProperty
  public String getAmiId()
  {
    return amiId;
  }

  @JsonProperty
  public String getInstanceType()
  {
    return instanceType;
  }

  @JsonProperty
  public int getMinInstances()
  {
    return minInstances;
  }

  @JsonProperty
  public int getMaxInstances()
  {
    return maxInstances;
  }

  @JsonProperty
  public List<String> getSecurityGroupIds()
  {
    return securityGroupIds;
  }

  @JsonProperty
  public String getKeyName()
  {
    return keyName;
  }

  @JsonProperty
  public String getSubnetId()
  {
    return subnetId;
  }

  @JsonProperty
  public EC2IamProfileData getIamProfile()
  {
    return iamProfile;
  }

  @JsonProperty
  public Boolean getAssociatePublicIpAddress()
  {
    return associatePublicIpAddress;
  }

  @Override
  public String toString()
  {
    return "EC2NodeData{" +
           "amiId='" + amiId + '\'' +
           ", instanceType='" + instanceType + '\'' +
           ", minInstances=" + minInstances +
           ", maxInstances=" + maxInstances +
           ", securityGroupIds=" + securityGroupIds +
           ", keyName='" + keyName + '\'' +
           ", subnetId='" + subnetId + '\'' +
           ", iamProfile=" + iamProfile +
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

    EC2NodeData that = (EC2NodeData) o;

    if (maxInstances != that.maxInstances) {
      return false;
    }
    if (minInstances != that.minInstances) {
      return false;
    }
    if (amiId != null ? !amiId.equals(that.amiId) : that.amiId != null) {
      return false;
    }
    if (iamProfile != null ? !iamProfile.equals(that.iamProfile) : that.iamProfile != null) {
      return false;
    }
    if (instanceType != null ? !instanceType.equals(that.instanceType) : that.instanceType != null) {
      return false;
    }
    if (keyName != null ? !keyName.equals(that.keyName) : that.keyName != null) {
      return false;
    }
    if (securityGroupIds != null ? !securityGroupIds.equals(that.securityGroupIds) : that.securityGroupIds != null) {
      return false;
    }
    if (subnetId != null ? !subnetId.equals(that.subnetId) : that.subnetId != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = amiId != null ? amiId.hashCode() : 0;
    result = 31 * result + (instanceType != null ? instanceType.hashCode() : 0);
    result = 31 * result + minInstances;
    result = 31 * result + maxInstances;
    result = 31 * result + (securityGroupIds != null ? securityGroupIds.hashCode() : 0);
    result = 31 * result + (keyName != null ? keyName.hashCode() : 0);
    result = 31 * result + (subnetId != null ? subnetId.hashCode() : 0);
    result = 31 * result + (iamProfile != null ? iamProfile.hashCode() : 0);
    return result;
  }
}

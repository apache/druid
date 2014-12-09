/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.overlord.autoscaling.ec2;

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

  @JsonCreator
  public EC2NodeData(
      @JsonProperty("amiId") String amiId,
      @JsonProperty("instanceType") String instanceType,
      @JsonProperty("minInstances") int minInstances,
      @JsonProperty("maxInstances") int maxInstances,
      @JsonProperty("securityGroupIds") List<String> securityGroupIds,
      @JsonProperty("keyName") String keyName
  )
  {
    this.amiId = amiId;
    this.instanceType = instanceType;
    this.minInstances = minInstances;
    this.maxInstances = maxInstances;
    this.securityGroupIds = securityGroupIds;
    this.keyName = keyName;
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
    if (instanceType != null ? !instanceType.equals(that.instanceType) : that.instanceType != null) {
      return false;
    }
    if (keyName != null ? !keyName.equals(that.keyName) : that.keyName != null) {
      return false;
    }
    if (securityGroupIds != null ? !securityGroupIds.equals(that.securityGroupIds) : that.securityGroupIds != null) {
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
    return result;
  }
}

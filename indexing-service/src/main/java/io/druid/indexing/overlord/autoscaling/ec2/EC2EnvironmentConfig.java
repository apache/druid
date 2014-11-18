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

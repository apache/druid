/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.merger.coordinator.setup;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 */
public class EC2NodeData implements WorkerNodeData
{
  private final String amiId;
  private final String instanceType;
  private final int minInstances;
  private final int maxInstances;

  @JsonCreator
  public EC2NodeData(
      @JsonProperty("amiId") String amiId,
      @JsonProperty("instanceType") String instanceType,
      @JsonProperty("minInstances") int minInstances,
      @JsonProperty("maxInstances") int maxInstances
  )
  {
    this.amiId = amiId;
    this.instanceType = instanceType;
    this.minInstances = minInstances;
    this.maxInstances = maxInstances;
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
}

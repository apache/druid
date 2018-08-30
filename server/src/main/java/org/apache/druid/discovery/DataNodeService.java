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

package org.apache.druid.discovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.server.coordination.ServerType;

import java.util.Objects;

/**
 * Metadata announced by any node that serves segments.
 */
public class DataNodeService extends DruidService
{
  public static final String DISCOVERY_SERVICE_KEY = "dataNodeService";

  private final String tier;
  private final long maxSize;
  private final ServerType type;
  private final int priority;

  @JsonCreator
  public DataNodeService(
      @JsonProperty("tier") String tier,
      @JsonProperty("maxSize") long maxSize,
      @JsonProperty("type") ServerType type,
      @JsonProperty("priority") int priority
  )
  {
    this.tier = tier;
    this.maxSize = maxSize;
    this.type = type;
    this.priority = priority;
  }

  @Override
  public String getName()
  {
    return DISCOVERY_SERVICE_KEY;
  }

  @JsonProperty
  public String getTier()
  {
    return tier;
  }

  @JsonProperty
  public long getMaxSize()
  {
    return maxSize;
  }

  @JsonProperty
  public ServerType getType()
  {
    return type;
  }

  @JsonProperty
  public int getPriority()
  {
    return priority;
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
    DataNodeService that = (DataNodeService) o;
    return maxSize == that.maxSize &&
           priority == that.priority &&
           Objects.equals(tier, that.tier) &&
           type == that.type;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(tier, maxSize, type, priority);
  }

  @Override
  public String toString()
  {
    return "DataNodeService{" +
           "tier='" + tier + '\'' +
           ", maxSize=" + maxSize +
           ", type=" + type +
           ", priority=" + priority +
           '}';
  }
}

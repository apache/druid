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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Worker metadata announced by Middle Manager.
 */
public class WorkerNodeService extends DruidService
{
  public static final String DISCOVERY_SERVICE_KEY = "workerNodeService";

  private final String ip;
  private final int capacity;
  private final String version;
  private final String category;

  public WorkerNodeService(
      @JsonProperty("ip") String ip,
      @JsonProperty("capacity") int capacity,
      @JsonProperty("version") String version,
      @JsonProperty("category") String category
  )
  {
    this.ip = ip;
    this.capacity = capacity;
    this.version = version;
    this.category = category;
  }

  @Override
  public String getName()
  {
    return DISCOVERY_SERVICE_KEY;
  }

  @JsonProperty
  public String getIp()
  {
    return ip;
  }

  @JsonProperty
  public int getCapacity()
  {
    return capacity;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @JsonProperty
  public String getCategory()
  {
    return category;
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
    WorkerNodeService that = (WorkerNodeService) o;
    return capacity == that.capacity &&
           Objects.equals(ip, that.ip) &&
           Objects.equals(version, that.version) &&
           Objects.equals(category, that.category);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(ip, capacity, version, category);
  }

  @Override
  public String toString()
  {
    return "WorkerNodeService{" +
           "ip='" + ip + '\'' +
           ", capacity=" + capacity +
           ", version='" + version + '\'' +
           ", category='" + category + '\'' +
           '}';
  }
}

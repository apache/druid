/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.worker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A container for worker metadata.
 */
public class Worker
{
  private final String host;
  private final String ip;
  private final int capacity;
  private final String version;
  private final AtomicReference<DateTime> lastCompletedTaskTime;

  @JsonCreator
  public Worker(
      @JsonProperty("host") String host,
      @JsonProperty("ip") String ip,
      @JsonProperty("capacity") int capacity,
      @JsonProperty("version") String version,
      @JsonProperty("lastCompletedTaskTime") DateTime lastCompletedTaskTime
  )
  {
    this.host = host;
    this.ip = ip;
    this.capacity = capacity;
    this.version = version;
    this.lastCompletedTaskTime = new AtomicReference<>(lastCompletedTaskTime == null
                                                       ? DateTime.now()
                                                       : lastCompletedTaskTime);
  }

  @JsonProperty
  public String getHost()
  {
    return host;
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
  public DateTime getLastCompletedTaskTime()
  {
    return lastCompletedTaskTime.get();
  }

  public void setLastCompletedTaskTime(DateTime completedTaskTime)
  {
    lastCompletedTaskTime.set(completedTaskTime);
  }

  public boolean isValidVersion(final String minVersion)
  {
    return getVersion().compareTo(minVersion) >= 0;
  }

  @Override
  public String toString()
  {
    return "Worker{" +
           "host='" + host + '\'' +
           ", ip='" + ip + '\'' +
           ", capacity=" + capacity +
           ", version='" + version + '\'' +
           '}';
  }
}

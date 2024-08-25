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

package org.apache.druid.server.coordinator.duty;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.List;

public class DutyGroupStatus
{
  private final String name;
  private final Duration period;
  private final List<String> dutyNames;

  private final DateTime lastRunStart;
  private final DateTime lastRunEnd;

  private final long avgRuntimeMillis;
  private final long avgRunGapMillis;

  public DutyGroupStatus(
      String name,
      Duration period,
      List<String> dutyNames,
      DateTime lastRunStart,
      DateTime lastRunEnd,
      long avgRuntimeMillis,
      long avgRunGapMillis
  )
  {
    this.name = name;
    this.period = period;
    this.dutyNames = dutyNames;
    this.lastRunStart = lastRunStart;
    this.lastRunEnd = lastRunEnd;
    this.avgRuntimeMillis = avgRuntimeMillis;
    this.avgRunGapMillis = avgRunGapMillis;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public Duration getPeriod()
  {
    return period;
  }

  @JsonProperty
  public List<String> getDutyNames()
  {
    return dutyNames;
  }

  @JsonProperty
  public DateTime getLastRunStart()
  {
    return lastRunStart;
  }

  @JsonProperty
  public DateTime getLastRunEnd()
  {
    return lastRunEnd;
  }

  @JsonProperty
  public long getAvgRuntimeMillis()
  {
    return avgRuntimeMillis;
  }

  @JsonProperty
  public long getAvgRunGapMillis()
  {
    return avgRunGapMillis;
  }

}

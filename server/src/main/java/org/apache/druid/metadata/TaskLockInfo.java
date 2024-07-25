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

package org.apache.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;

import java.util.Objects;

/**
 * Contains information about an active task lock for a given datasource
 */
public class TaskLockInfo
{
  private final String granularity;
  private final String type;
  private final int priority;
  private final Interval interval;

  @JsonCreator
  public TaskLockInfo(
      @JsonProperty("granularity") String granularity,
      @JsonProperty("type") String type,
      @JsonProperty("priority") int priority,
      @JsonProperty("interval") Interval interval
  )
  {
    this.granularity = granularity;
    this.type = type;
    this.priority = priority;
    this.interval = interval;
  }

  @JsonProperty
  public String getGranularity()
  {
    return granularity;
  }

  @JsonProperty
  public String getType()
  {
    return type;
  }

  @JsonProperty
  public int getPriority()
  {
    return priority;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
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
    TaskLockInfo that = (TaskLockInfo) o;
    return Objects.equals(granularity, that.granularity)
           && Objects.equals(type, that.type)
           && priority == that.priority
           && Objects.equals(interval, that.interval);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(granularity, type, priority, interval);
  }

  @Override
  public String toString()
  {
    return "TaskLockInfo{" +
           "granularity=" + granularity +
           ", type=" + type +
           ", interval=" + interval +
           ", priority=" + priority +
           '}';
  }
}

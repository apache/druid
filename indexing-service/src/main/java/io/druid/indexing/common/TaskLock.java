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

package io.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.joda.time.Interval;

/**
 * Represents a lock held by some task. Immutable.
 */
public class TaskLock
{
  private final String groupId;
  private final String dataSource;
  private final Interval interval;
  private final String version;

  @JsonCreator
  public TaskLock(
      @JsonProperty("groupId") String groupId,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version
  )
  {
    this.groupId = groupId;
    this.dataSource = dataSource;
    this.interval = interval;
    this.version = version;
  }

  @JsonProperty
  public String getGroupId()
  {
    return groupId;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof TaskLock)) {
      return false;
    } else {
      final TaskLock x = (TaskLock) o;
      return Objects.equal(this.groupId, x.groupId) &&
             Objects.equal(this.dataSource, x.dataSource) &&
             Objects.equal(this.interval, x.interval) &&
             Objects.equal(this.version, x.version);
    }
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(groupId, dataSource, interval, version);
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("groupId", groupId)
                  .add("dataSource", dataSource)
                  .add("interval", interval)
                  .add("version", version)
                  .toString();
  }
}

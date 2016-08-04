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
 *
 */
public class TaskLock
{
  /**
   * Represents the groupdId for the lock, tasks having same groupdId can share TaskLock
   * */
  private final String groupId;
  private final String dataSource;
  private final Interval interval;
  /**
   * This version will be used to publish the segments
   * */
  private final String version;
  /**
   * Priority used for acquiring the lock, value depends on the task type
   * */
  private final int priority;
  /**
   * If false this lock can be revoked by a higher priority TaskLock otherwise not
   * */
  private final boolean upgraded;

  @JsonCreator
  public TaskLock(
      @JsonProperty("groupId") String groupId,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JsonProperty("priority") int priority,
      @JsonProperty("upgraded") boolean upgraded
  )
  {
    this.groupId = groupId;
    this.dataSource = dataSource;
    this.interval = interval;
    this.version = version;
    this.priority = priority;
    this.upgraded = upgraded;
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

  @JsonProperty
  public int getPriority()
  {
    return priority;
  }

  @JsonProperty
  public boolean isUpgraded()
  {
    return upgraded;
  }

  public TaskLock withUpgraded(boolean upgraded) {
    return new TaskLock(
        getGroupId(),
        getDataSource(),
        getInterval(),
        getVersion(),
        getPriority(),
        upgraded
    );
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
             Objects.equal(this.version, x.version) &&
             Objects.equal(this.priority, x.priority) &&
             Objects.equal(this.upgraded, x.upgraded); // added priority and upgraded to equals check
    }
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(groupId, dataSource, interval, version, priority, upgraded);
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("groupId", groupId)
                  .add("dataSource", dataSource)
                  .add("interval", interval)
                  .add("version", version)
                  .add("priority", priority)
                  .add("upgraded", upgraded)
                  .toString();
  }
}

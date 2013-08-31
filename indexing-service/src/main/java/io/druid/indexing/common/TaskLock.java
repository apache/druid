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

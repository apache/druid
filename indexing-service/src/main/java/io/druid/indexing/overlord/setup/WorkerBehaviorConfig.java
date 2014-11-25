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

package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.indexing.overlord.autoscaling.AutoScaler;

/**
 */
public class WorkerBehaviorConfig
{
  public static final String CONFIG_KEY = "worker.config";

  private final WorkerSelectStrategy selectStrategy;
  private final AutoScaler autoScaler;

  @JsonCreator
  public WorkerBehaviorConfig(
      @JsonProperty("selectStrategy") WorkerSelectStrategy selectStrategy,
      @JsonProperty("autoScaler") AutoScaler autoScaler
  )
  {
    this.selectStrategy = selectStrategy;
    this.autoScaler = autoScaler;
  }

  @JsonProperty
  public WorkerSelectStrategy getSelectStrategy()
  {
    return selectStrategy;
  }

  @JsonProperty
  public AutoScaler getAutoScaler()
  {
    return autoScaler;
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

    WorkerBehaviorConfig that = (WorkerBehaviorConfig) o;

    if (autoScaler != null ? !autoScaler.equals(that.autoScaler) : that.autoScaler != null) {
      return false;
    }
    if (selectStrategy != null ? !selectStrategy.equals(that.selectStrategy) : that.selectStrategy != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = selectStrategy != null ? selectStrategy.hashCode() : 0;
    result = 31 * result + (autoScaler != null ? autoScaler.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "WorkerConfiguration{" +
           "selectStrategy=" + selectStrategy +
           ", autoScaler=" + autoScaler +
           '}';
  }
}

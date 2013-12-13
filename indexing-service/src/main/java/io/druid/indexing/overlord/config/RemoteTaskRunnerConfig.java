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

package io.druid.indexing.overlord.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Period;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 */
public class RemoteTaskRunnerConfig
{
  @JsonProperty
  @NotNull
  private Period taskAssignmentTimeout = new Period("PT5M");

  @JsonProperty
  private boolean compressZnodes = false;

  @JsonProperty
  private String minWorkerVersion = "0";

  @JsonProperty
  @Min(10 * 1024)
  private long maxZnodeBytes = 512 * 1024;

  public Period getTaskAssignmentTimeout()
  {
    return taskAssignmentTimeout;
  }

  public boolean isCompressZnodes()
  {
    return compressZnodes;
  }

  public String getMinWorkerVersion()
  {
    return minWorkerVersion;
  }

  public long getMaxZnodeBytes()
  {
    return maxZnodeBytes;
  }
}

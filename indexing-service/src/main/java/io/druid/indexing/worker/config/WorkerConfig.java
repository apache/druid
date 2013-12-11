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

package io.druid.indexing.worker.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 */
public class WorkerConfig
{
  @JsonProperty
  @NotNull
  private String ip = "localhost";

  @JsonProperty
  @NotNull
  private String version = "0";

  @JsonProperty
  @Min(1)
  private int capacity = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

  public String getIp()
  {
    return ip;
  }

  public String getVersion()
  {
    return version;
  }

  public int getCapacity()
  {
    return capacity;
  }

  public WorkerConfig setCapacity(int capacity)
  {
    this.capacity = capacity;
    return this;
  }
}

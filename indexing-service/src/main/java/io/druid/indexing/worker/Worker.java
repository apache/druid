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

package io.druid.indexing.worker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A container for worker metadata.
 */
public class Worker
{
  private final String host;
  private final String ip;
  private final int capacity;
  private final String version;

  @JsonCreator
  public Worker(
      @JsonProperty("host") String host,
      @JsonProperty("ip") String ip,
      @JsonProperty("capacity") int capacity,
      @JsonProperty("version") String version
  )
  {
    this.host = host;
    this.ip = ip;
    this.capacity = capacity;
    this.version = version;
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

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

package io.druid.server.coordination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class DruidServerMetadata
{
  private final String name;
  private final String host;
  private final long maxSize;
  private final String tier;
  private final String type;
  private final int priority;

  @JsonCreator
  public DruidServerMetadata(
      @JsonProperty("name") String name,
      @JsonProperty("host") String host,
      @JsonProperty("maxSize") long maxSize,
      @JsonProperty("type") String type,
      @JsonProperty("tier") String tier,
      @JsonProperty("priority") int priority
  )
  {
    this.name = name;
    this.host = host;
    this.maxSize = maxSize;
    this.tier = tier;
    this.type = type;
    this.priority = priority;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getHost()
  {
    return host;
  }

  @JsonProperty
  public long getMaxSize()
  {
    return maxSize;
  }

  @JsonProperty
  public String getTier()
  {
    return tier;
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

  public boolean isAssignable()
  {
    return getType().equalsIgnoreCase("historical") || getType().equalsIgnoreCase("bridge");
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

    DruidServerMetadata metadata = (DruidServerMetadata) o;

    if (maxSize != metadata.maxSize) {
      return false;
    }
    if (priority != metadata.priority) {
      return false;
    }
    if (host != null ? !host.equals(metadata.host) : metadata.host != null) {
      return false;
    }
    if (name != null ? !name.equals(metadata.name) : metadata.name != null) {
      return false;
    }
    if (tier != null ? !tier.equals(metadata.tier) : metadata.tier != null) {
      return false;
    }
    if (type != null ? !type.equals(metadata.type) : metadata.type != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (host != null ? host.hashCode() : 0);
    result = 31 * result + (int) (maxSize ^ (maxSize >>> 32));
    result = 31 * result + (tier != null ? tier.hashCode() : 0);
    result = 31 * result + (type != null ? type.hashCode() : 0);
    result = 31 * result + priority;
    return result;
  }

  @Override
  public String toString()
  {
    return "DruidServerMetadata{" +
           "name='" + name + '\'' +
           ", host='" + host + '\'' +
           ", maxSize=" + maxSize +
           ", tier='" + tier + '\'' +
           ", type='" + type + '\'' +
           ", priority='" + priority + '\'' +
           '}';
  }
}

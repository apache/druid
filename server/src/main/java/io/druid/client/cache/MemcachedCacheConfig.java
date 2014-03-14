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

package io.druid.client.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import net.spy.memcached.DefaultConnectionFactory;

import javax.validation.constraints.NotNull;

public class MemcachedCacheConfig
{
  // default to 30 day expiration for cache entries
  // values greater than 30 days are interpreted by memcached as absolute POSIX timestamps instead of duration
  @JsonProperty
  private int expiration = 30 * 24 * 3600;

  @JsonProperty
  private int timeout = 500;

  // comma delimited list of memcached servers, given as host:port combination
  @JsonProperty
  @NotNull
  private String hosts;

  @JsonProperty
  private int maxObjectSize = 50 * 1024 * 1024;

  // memcached client read buffer size, -1 uses the spymemcached library default
  @JsonProperty
  private int readBufferSize = DefaultConnectionFactory.DEFAULT_READ_BUFFER_SIZE;

  @JsonProperty
  private String memcachedPrefix = "druid";

  // maximum size in bytes of memcached client operation queue. 0 means unbounded
  @JsonProperty
  private long maxOperationQueueSize = 0;

  public int getExpiration()
  {
    return expiration;
  }

  public int getTimeout()
  {
    return timeout;
  }

  public String getHosts()
  {
    return hosts;
  }

  public int getMaxObjectSize()
  {
    return maxObjectSize;
  }

  public String getMemcachedPrefix()
  {
    return memcachedPrefix;
  }

  public long getMaxOperationQueueSize()
  {
    return maxOperationQueueSize;
  }

  public int getReadBufferSize()
  {
    return readBufferSize;
  }
}

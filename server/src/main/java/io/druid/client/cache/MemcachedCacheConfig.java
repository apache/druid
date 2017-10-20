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

package io.druid.client.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import net.spy.memcached.DefaultConnectionFactory;

import javax.validation.constraints.NotNull;

public class MemcachedCacheConfig
{
  @JsonProperty
  private MemcachedCache.Mode mode = MemcachedCache.Mode.READ_AND_WRITE;

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

  // size of memcached connection pool
  @JsonProperty
  private int numConnections = 1;

  public MemcachedCache.Mode getMode()
  {
    return mode;
  }

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

  public int getNumConnections()
  {
    return numConnections;
  }
}

/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.client.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.FailureMode;
import org.apache.commons.lang.WordUtils;

import javax.validation.constraints.NotNull;

public class MemcachedCacheConfig
{
  public static class FailureModeWrapper {
    final FailureMode wrappedFailureMode;

    public FailureModeWrapper(FailureMode failureMode)
    {
      Preconditions.checkNotNull(failureMode);
      this.wrappedFailureMode = failureMode;
    }

    @JsonCreator
    public static FailureModeWrapper fromJsonString(String name) {
      return new FailureModeWrapper(FailureMode.valueOf(WordUtils.capitalize(name)));
    }

    @JsonValue
    public String toJsonString() {
      return wrappedFailureMode.name().toLowerCase();
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

      FailureModeWrapper that = (FailureModeWrapper) o;

      return wrappedFailureMode == that.wrappedFailureMode;

    }

    @Override
    public int hashCode()
    {
      return wrappedFailureMode.hashCode();
    }
  }

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

  @JsonProperty
  // Connection interruptions can result in failures
  private FailureModeWrapper failureMode = new FailureModeWrapper(FailureMode.Cancel);

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

  public FailureMode getFailureMode()
  {
    return failureMode.wrappedFailureMode;
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

    MemcachedCacheConfig that = (MemcachedCacheConfig) o;

    if (expiration != that.expiration) {
      return false;
    }
    if (timeout != that.timeout) {
      return false;
    }
    if (maxObjectSize != that.maxObjectSize) {
      return false;
    }
    if (readBufferSize != that.readBufferSize) {
      return false;
    }
    if (maxOperationQueueSize != that.maxOperationQueueSize) {
      return false;
    }
    if (numConnections != that.numConnections) {
      return false;
    }
    if (hosts != null ? !hosts.equals(that.hosts) : that.hosts != null) {
      return false;
    }
    if (memcachedPrefix != null ? !memcachedPrefix.equals(that.memcachedPrefix) : that.memcachedPrefix != null) {
      return false;
    }
    return !(failureMode != null ? !failureMode.equals(that.failureMode) : that.failureMode != null);

  }

  @Override
  public int hashCode()
  {
    int result = expiration;
    result = 31 * result + timeout;
    result = 31 * result + (hosts != null ? hosts.hashCode() : 0);
    result = 31 * result + maxObjectSize;
    result = 31 * result + readBufferSize;
    result = 31 * result + (memcachedPrefix != null ? memcachedPrefix.hashCode() : 0);
    result = 31 * result + (int) (maxOperationQueueSize ^ (maxOperationQueueSize >>> 32));
    result = 31 * result + numConnections;
    result = 31 * result + (failureMode != null ? failureMode.hashCode() : 0);
    return result;
  }
}

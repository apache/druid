/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.emitter.ganglia;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class GangliaEmitterConfig
{
  private static final Long DEFAULT_LOAD_PERIOD = (long) (60 * 1000); // load every one minute
  private static final Long DEFAULT_FLUSH_PERIOD = (long) (60 * 1000); // flush every one minute
  private static final long DEFAULT_GET_TIMEOUT = 1000; // default wait for get operations on the queue 1 sec
  @JsonProperty
  private final String hostname;
  @JsonProperty
  private final int port;
  @JsonProperty
  private final String dimensionMapPath;
  @JsonProperty
  private final Boolean includeHost;
  @JsonProperty
  private final String separator;
  @JsonProperty
  private final String blankHolder;
  @JsonProperty
  private final Long loadPeriod;
  @JsonProperty
  private final Long flushPeriod;
  @JsonProperty
  private final Integer maxQueueSize;
  @JsonProperty
  private final Long waitForEventTime;
  @JsonProperty
  private final Long emitWaitTime;
  //waiting up to the specified wait time if necessary for an event to become available.

  @JsonCreator
  public GangliaEmitterConfig(
      @JsonProperty("hostname") String hostname,
      @JsonProperty("port") Integer port,
      @JsonProperty("dimensionMapPath") String dimensionMapPath,
      @JsonProperty("includeHost") Boolean includeHost,
      @JsonProperty("separator") String separator,
      @JsonProperty("blankHolder") String blankHolder,
      @JsonProperty("loadPeriod") Long loadPeriod,
      @JsonProperty("flushPeriod") Long flushPeriod,
      @JsonProperty("maxQueueSize") Integer maxQueueSize,
      @JsonProperty("emitWaitTime") Long emitWaitTime,
      @JsonProperty("waitForEventTime") Long waitForEventTime
  )
  {
    this.hostname = Preconditions.checkNotNull(hostname, "hostname can not be null");
    this.port = Preconditions.checkNotNull(port, "port can not be null");
    this.dimensionMapPath = dimensionMapPath;
    this.includeHost = includeHost != null ? includeHost : false;
    this.separator = separator != null ? separator : ".";
    this.blankHolder = blankHolder != null ? blankHolder : "-";
    this.loadPeriod = loadPeriod == null ? DEFAULT_LOAD_PERIOD : loadPeriod;
    this.flushPeriod = flushPeriod == null ? DEFAULT_FLUSH_PERIOD : flushPeriod;
    this.maxQueueSize = maxQueueSize == null ? Integer.MAX_VALUE : maxQueueSize;
    this.waitForEventTime = waitForEventTime == null ? DEFAULT_GET_TIMEOUT : waitForEventTime;
    this.emitWaitTime = emitWaitTime == null ? 0 : emitWaitTime;
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

    GangliaEmitterConfig that = (GangliaEmitterConfig) o;
    if (separator != null ? !separator.equals(that.separator) : that.separator != null) {
      return false;
    }
    if (includeHost != null ? !includeHost.equals(that.includeHost) : that.includeHost != null) {
      return false;
    }
    if (getPort() != that.getPort()) {
      return false;
    }
    if (!getLoadPeriod().equals(that.getLoadPeriod())) {
      return false;
    }
    if (!getFlushPeriod().equals(that.getFlushPeriod())) {
      return false;
    }
    if (!getMaxQueueSize().equals(that.getMaxQueueSize())) {
      return false;
    }
    if (!getWaitForEventTime().equals(that.getWaitForEventTime())) {
      return false;
    }
    return dimensionMapPath != null ? dimensionMapPath.equals(that.dimensionMapPath) : that.dimensionMapPath == null;
  }

  @Override
  public int hashCode()
  {
    int result = getHostname().hashCode();
    result = 31 * result + getPort();
    result = 31 * result + (separator != null ? separator.hashCode() : 0);
    result = 31 * result + (includeHost != null ? includeHost.hashCode() : 0);
    result = 31 * result + getLoadPeriod().hashCode();
    result = 31 * result + getFlushPeriod().hashCode();
    result = 31 * result + getMaxQueueSize().hashCode();
    result = 31 * result + (dimensionMapPath != null ? dimensionMapPath.hashCode() : 0);
    result = 31 * result + (blankHolder != null ? blankHolder.hashCode() : 0);
    result = 31 * result + getWaitForEventTime().hashCode();
    result = 31 * result + getEmitWaitTime().hashCode();
    return result;
  }

  @JsonProperty
  public String getHostname()
  {
    return hostname;
  }

  @JsonProperty
  public int getPort()
  {
    return port;
  }

  @JsonProperty
  public String getDimensionMapPath()
  {
    return dimensionMapPath;
  }

  @JsonProperty
  public Boolean getIncludeHost()
  {
    return includeHost;
  }

  @JsonProperty
  public String getSeparator()
  {
    return separator;
  }

  @JsonProperty
  public String getBlankHolder()
  {
    return blankHolder;
  }

  @JsonProperty
  public Long getLoadPeriod()
  {
    return loadPeriod;
  }

  @JsonProperty
  public Long getFlushPeriod()
  {
    return flushPeriod;
  }

  @JsonProperty
  public Integer getMaxQueueSize()
  {
    return maxQueueSize;
  }

  @JsonProperty
  public Long getWaitForEventTime()
  {
    return waitForEventTime;
  }

  @JsonProperty
  public Long getEmitWaitTime()
  {
    return emitWaitTime;
  }
}

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

package org.apache.druid.emitter.influxdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class InfluxdbEmitterConfig
{

  private static final int DEFAULT_PORT = 8086;
  private static final int DEFAULT_QUEUE_SIZE = Integer.MAX_VALUE;
  private static final int DEFAULT_FLUSH_PERIOD = 60000; // milliseconds
  private static final List<String> DEFAULT_DIMENSION_WHITELIST = Arrays.asList("dataSource", "type", "numMetrics", "numDimensions", "threshold", "dimension", "taskType", "taskStatus", "tier");

  @JsonProperty
  private final String hostname;
  @JsonProperty
  private final Integer port;
  @JsonProperty
  private final String databaseName;
  @JsonProperty
  private final Integer maxQueueSize;
  @JsonProperty
  private final Integer flushPeriod;
  @JsonProperty
  private final Integer flushDelay;
  @JsonProperty
  private final String influxdbUserName;
  @JsonProperty
  private final String influxdbPassword;
  @JsonProperty
  private final ImmutableSet<String> dimensionWhitelist;

  private static Logger log = new Logger(InfluxdbEmitterConfig.class);

  @JsonCreator
  public InfluxdbEmitterConfig(
      @JsonProperty("hostname") String hostname,
      @JsonProperty("port") Integer port,
      @JsonProperty("databaseName") String databaseName,
      @JsonProperty("maxQueueSize") Integer maxQueueSize,
      @JsonProperty("flushPeriod") Integer flushPeriod,
      @JsonProperty("flushDelay") Integer flushDelay,
      @JsonProperty("influxdbUserName") String influxdbUserName,
      @JsonProperty("influxdbPassword") String influxdbPassword,
      @JsonProperty("dimensionWhitelist") Set<String> dimensionWhitelist
  )
  {
    this.hostname = Preconditions.checkNotNull(hostname, "hostname can not be null");
    this.port = port == null ? DEFAULT_PORT : port;
    this.databaseName = Preconditions.checkNotNull(databaseName, "databaseName can not be null");
    this.maxQueueSize = maxQueueSize == null ? DEFAULT_QUEUE_SIZE : maxQueueSize;
    this.flushPeriod = flushPeriod == null ? DEFAULT_FLUSH_PERIOD : flushPeriod;
    this.flushDelay = flushDelay == null ? DEFAULT_FLUSH_PERIOD : flushDelay;
    this.influxdbUserName = Preconditions.checkNotNull(influxdbUserName, "influxdbUserName can not be null");
    this.influxdbPassword = Preconditions.checkNotNull(influxdbPassword, "influxdbPassword can not be null");
    this.dimensionWhitelist = dimensionWhitelist == null ? ImmutableSet.copyOf(DEFAULT_DIMENSION_WHITELIST) : ImmutableSet.copyOf(dimensionWhitelist);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InfluxdbEmitterConfig)) {
      return false;
    }

    InfluxdbEmitterConfig that = (InfluxdbEmitterConfig) o;

    if (getPort() != that.getPort()) {
      return false;
    }
    if (!getHostname().equals(that.getHostname())) {
      return false;
    }
    if (!getDatabaseName().equals(that.getDatabaseName())) {
      return false;
    }
    if (getFlushPeriod() != that.getFlushPeriod()) {
      return false;
    }
    if (getMaxQueueSize() != that.getMaxQueueSize()) {
      return false;
    }
    if (getFlushDelay() != that.getFlushDelay()) {
      return false;
    }
    if (!getInfluxdbUserName().equals(that.getInfluxdbUserName())) {
      return false;
    }
    if (!getInfluxdbPassword().equals(that.getInfluxdbPassword())) {
      return false;
    }
    if (!getDimensionWhitelist().equals(that.getDimensionWhitelist())) {
      return false;
    }
    return true;

  }

  @Override
  public int hashCode()
  {
    int result = getHostname().hashCode();
    result = 31 * result + getPort();
    result = 31 * result + getDatabaseName().hashCode();
    result = 31 * result + getFlushPeriod();
    result = 31 * result + getMaxQueueSize();
    result = 31 * result + getFlushDelay();
    result = 31 * result + getInfluxdbUserName().hashCode();
    result = 31 * result + getInfluxdbPassword().hashCode();
    result = 31 * result + getDimensionWhitelist().hashCode();
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
  public String getDatabaseName()
  {
    return databaseName;
  }

  @JsonProperty
  public int getFlushPeriod()
  {
    return flushPeriod;
  }

  @JsonProperty
  public int getMaxQueueSize()
  {
    return maxQueueSize;
  }

  @JsonProperty
  public int getFlushDelay()
  {
    return flushDelay;
  }

  @JsonProperty
  public String getInfluxdbUserName()
  {
    return influxdbUserName;
  }

  @JsonProperty
  public String getInfluxdbPassword()
  {
    return influxdbPassword;
  }

  @JsonProperty
  public ImmutableSet<String> getDimensionWhitelist()
  {
    return dimensionWhitelist;
  }
}

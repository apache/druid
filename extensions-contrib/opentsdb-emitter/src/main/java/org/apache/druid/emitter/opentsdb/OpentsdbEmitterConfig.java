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

package org.apache.druid.emitter.opentsdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class OpentsdbEmitterConfig
{
  private static final int DEFAULT_FLUSH_THRESHOLD = 100;
  private static final int DEFAULT_MAX_QUEUE_SIZE = 1000;
  private static final long DEFAULT_CONSUME_DELAY_MILLIS = 10000;
  private static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS = 2000;
  private static final int DEFAULT_READ_TIMEOUT_MILLIS = 2000;

  @JsonProperty
  private final String host;

  @JsonProperty
  private final int port;

  @JsonProperty
  private final int connectionTimeout;

  @JsonProperty
  private final int readTimeout;

  @JsonProperty
  private final int flushThreshold;

  @JsonProperty
  private final int maxQueueSize;

  @JsonProperty
  private final long consumeDelay;

  @JsonProperty
  private final String metricMapPath;

  @JsonCreator
  public OpentsdbEmitterConfig(
      @JsonProperty("host") String host,
      @JsonProperty("port") Integer port,
      @JsonProperty("connectionTimeout") Integer connectionTimeout,
      @JsonProperty("readTimeout") Integer readTimeout,
      @JsonProperty("flushThreshold") Integer flushThreshold,
      @JsonProperty("maxQueueSize") Integer maxQueueSize,
      @JsonProperty("consumeDelay") Long consumeDelay,
      @JsonProperty("metricMapPath") String metricMapPath
  )
  {
    this.host = Preconditions.checkNotNull(host, "host can not be null.");
    this.port = Preconditions.checkNotNull(port, "port can not be null");
    this.connectionTimeout = (connectionTimeout == null || connectionTimeout < 0)
                             ? DEFAULT_CONNECTION_TIMEOUT_MILLIS
                             : connectionTimeout;
    this.readTimeout =
        (readTimeout == null || readTimeout < 0) ? DEFAULT_READ_TIMEOUT_MILLIS : readTimeout;
    this.flushThreshold = (flushThreshold == null || flushThreshold < 0) ? DEFAULT_FLUSH_THRESHOLD : flushThreshold;
    this.maxQueueSize = (maxQueueSize == null || maxQueueSize < 0) ? DEFAULT_MAX_QUEUE_SIZE : maxQueueSize;
    this.consumeDelay = (consumeDelay == null || consumeDelay < 0) ? DEFAULT_CONSUME_DELAY_MILLIS : consumeDelay;
    this.metricMapPath = metricMapPath;
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

    OpentsdbEmitterConfig that = (OpentsdbEmitterConfig) o;

    if (!host.equals(that.host)) {
      return false;
    }
    if (port != that.port) {
      return false;
    }
    if (connectionTimeout != that.connectionTimeout) {
      return false;
    }
    if (readTimeout != that.readTimeout) {
      return false;
    }
    if (flushThreshold != that.flushThreshold) {
      return false;
    }
    if (maxQueueSize != that.maxQueueSize) {
      return false;
    }
    if (consumeDelay != that.consumeDelay) {
      return false;
    }
    return metricMapPath != null ? metricMapPath.equals(that.metricMapPath)
                                 : that.metricMapPath == null;
  }

  @Override
  public int hashCode()
  {
    int result = host.hashCode();
    result = 31 * result + port;
    result = 31 * result + connectionTimeout;
    result = 31 * result + readTimeout;
    result = 31 * result + flushThreshold;
    result = 31 * result + maxQueueSize;
    result = 31 * result + (int) consumeDelay;
    result = 31 * result + (metricMapPath != null ? metricMapPath.hashCode() : 0);
    return result;
  }

  public String getHost()
  {
    return host;
  }

  public int getPort()
  {
    return port;
  }

  public int getConnectionTimeout()
  {
    return connectionTimeout;
  }

  public int getReadTimeout()
  {
    return readTimeout;
  }

  public int getFlushThreshold()
  {
    return flushThreshold;
  }

  public int getMaxQueueSize()
  {
    return maxQueueSize;
  }

  public long getConsumeDelay()
  {
    return consumeDelay;
  }

  public String getMetricMapPath()
  {
    return metricMapPath;
  }
}

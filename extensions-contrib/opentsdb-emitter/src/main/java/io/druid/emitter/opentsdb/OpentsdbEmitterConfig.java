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

package io.druid.emitter.opentsdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class OpentsdbEmitterConfig
{
  private static final int DEFAULT_BATCH_SIZE = 100;
  private static final int DEFAULT_CONNECTION_TIMEOUT = 2000;
  private static final int DEFAULT_READ_TIMEOUT = 2000;

  @JsonProperty
  private final String host;

  @JsonProperty
  private final int port;

  @JsonProperty
  private final int connectionTimeout;

  @JsonProperty
  private final int readTimeout;

  @JsonProperty
  private final int batchSize;

  @JsonProperty
  private final String metricMapPath;

  @JsonCreator
  public OpentsdbEmitterConfig(
      @JsonProperty("host") String host,
      @JsonProperty("port") Integer port,
      @JsonProperty("connectionTimeout") Integer connectionTimeout,
      @JsonProperty("readTimeout") Integer readTimeout,
      @JsonProperty("batchSize") Integer batchSize,
      @JsonProperty("metricMapPath") String metricMapPath
  )
  {
    this.host = Preconditions.checkNotNull(host, "host can not be null.");
    this.port = Preconditions.checkNotNull(port, "port can not be null");
    this.connectionTimeout = (connectionTimeout == null || connectionTimeout < 0)
                             ? DEFAULT_CONNECTION_TIMEOUT
                             : connectionTimeout;
    this.readTimeout = (readTimeout == null || readTimeout < 0) ? DEFAULT_READ_TIMEOUT : readTimeout;
    this.batchSize = (batchSize == null || batchSize < 0) ? DEFAULT_BATCH_SIZE : batchSize;
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
    if (batchSize != that.batchSize) {
      return false;
    }
    return metricMapPath != null ? metricMapPath.equals(that.metricMapPath) : that.metricMapPath == null;
  }

  @Override
  public int hashCode()
  {
    int result = host.hashCode();
    result = 31 * result + port;
    result = 31 * result + connectionTimeout;
    result = 31 * result + readTimeout;
    result = 31 * result + batchSize;
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

  public int getBatchSize()
  {
    return batchSize;
  }

  public String getMetricMapPath()
  {
    return metricMapPath;
  }
}

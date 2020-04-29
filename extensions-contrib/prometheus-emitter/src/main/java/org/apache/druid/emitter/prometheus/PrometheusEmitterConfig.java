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

package org.apache.druid.emitter.prometheus;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.regex.Pattern;

public class PrometheusEmitterConfig
{

  Pattern pattern = Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*");
  private static final int DEFAULT_FLUSH_PERIOD = 6000;

  @JsonProperty
  private final String host;

  @JsonProperty
  private final int port;

  @JsonProperty
  private final String metricMapPath;

  @JsonProperty
  private final String nameSpace;

  @JsonProperty
  private final int flushPeriod;

  @JsonProperty
  private final int flushDelay;

  @JsonCreator
  public PrometheusEmitterConfig(
      @JsonProperty("host") String host,
      @JsonProperty("port") Integer port,
      @JsonProperty("metricMapPath") String metricMapPath,
      @JsonProperty("nameSpace") String nameSpace,
      @JsonProperty("flushPeriod") Integer flushPeriod,
      @JsonProperty("flushDelay") Integer flushDelay
  )
  {
    this.host = Preconditions.checkNotNull(host, "host can not be null.");
    this.port = port == null ? 0 : port;
    this.metricMapPath = metricMapPath;
    this.nameSpace = nameSpace != null ? nameSpace : "druid";
    this.flushDelay = flushDelay == null ? DEFAULT_FLUSH_PERIOD : flushDelay;
    this.flushPeriod = flushPeriod == null ? DEFAULT_FLUSH_PERIOD : flushPeriod;
    Preconditions.checkArgument(pattern.matcher(this.nameSpace).matches(), "Invalid namespace " + this.nameSpace);
  }

  @Override
  public int hashCode()
  {
    int result = host.hashCode();
    result = 31 * result + port;
    result = 31 * result + getFlushPeriod();
    result = 31 * result + getFlushDelay();
    result = 31 * result + (metricMapPath != null ? metricMapPath.hashCode() : 0);
    result = 31 * result + (nameSpace != null ? nameSpace.hashCode() : 0);
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

  public String getMetricMapPath()
  {
    return metricMapPath;
  }

  public String getNameSpace()
  {
    return nameSpace;
  }

  public Integer getFlushPeriod()
  {
    return flushPeriod;
  }

  public Integer getFlushDelay()
  {
    return flushDelay;
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
    PrometheusEmitterConfig that = (PrometheusEmitterConfig) o;
    return port == that.port &&
        host.equals(that.host) &&
        flushDelay == that.flushDelay &&
        flushPeriod == that.flushPeriod &&
        (Objects.equals(metricMapPath, that.metricMapPath)) &&
        (Objects.equals(nameSpace, that.nameSpace));
  }
}

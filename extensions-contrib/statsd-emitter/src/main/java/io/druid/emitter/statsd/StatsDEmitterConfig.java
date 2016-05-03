/*
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.druid.emitter.statsd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 */
public class StatsDEmitterConfig
{

  @JsonProperty
  final private String hostname;
  @JsonProperty
  final private Integer port;
  @JsonProperty
  final private String prefix;
  @JsonProperty
  final private String separator;
  @JsonProperty
  final private Boolean includeHost;
  @JsonProperty
  final private String dimensionMapPath;

  @JsonCreator
  public StatsDEmitterConfig(
      @JsonProperty("hostname") String hostname,
      @JsonProperty("port") Integer port,
      @JsonProperty("prefix") String prefix,
      @JsonProperty("separator") String separator,
      @JsonProperty("includeHost") Boolean includeHost,
      @JsonProperty("dimensionMapPath") String dimensionMapPath)
  {
    this.hostname = Preconditions.checkNotNull(hostname, "StatsD hostname cannot be null.");
    this.port = Preconditions.checkNotNull(port, "StatsD port cannot be null.");
    this.prefix = prefix != null ? prefix : "";
    this.separator = separator != null ? separator : ".";
    this.includeHost = includeHost != null ? includeHost : false;
    this.dimensionMapPath = dimensionMapPath;
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

    StatsDEmitterConfig that = (StatsDEmitterConfig) o;

    if (hostname != null ? !hostname.equals(that.hostname) : that.hostname != null) {
      return false;
    }
    if (port != null ? !port.equals(that.port) : that.port != null) {
      return false;
    }
    if (prefix != null ? !prefix.equals(that.prefix) : that.prefix != null) {
      return false;
    }
    if (separator != null ? !separator.equals(that.separator) : that.separator != null) {
      return false;
    }
    if (includeHost != null ? !includeHost.equals(that.includeHost) : that.includeHost != null) {
      return false;
    }
    return dimensionMapPath != null ? dimensionMapPath.equals(that.dimensionMapPath) : that.dimensionMapPath == null;

  }

  @Override
  public int hashCode()
  {
    int result = hostname != null ? hostname.hashCode() : 0;
    result = 31 * result + (port != null ? port.hashCode() : 0);
    result = 31 * result + (prefix != null ? prefix.hashCode() : 0);
    result = 31 * result + (separator != null ? separator.hashCode() : 0);
    result = 31 * result + (includeHost != null ? includeHost.hashCode() : 0);
    result = 31 * result + (dimensionMapPath != null ? dimensionMapPath.hashCode() : 0);
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
  public String getPrefix()
  {
    return prefix;
  }

  @JsonProperty
  public String getSeparator()
  {
    return separator;
  }

  @JsonProperty
  public Boolean getIncludeHost()
  {
    return includeHost;
  }

  @JsonProperty
  public String getDimensionMapPath()
  {
    return dimensionMapPath;
  }
}

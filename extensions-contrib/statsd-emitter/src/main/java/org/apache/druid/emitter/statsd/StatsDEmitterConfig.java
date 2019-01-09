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

package org.apache.druid.emitter.statsd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 */
public class StatsDEmitterConfig
{

  @JsonProperty
  private final String hostname;
  @JsonProperty
  private final Integer port;
  @JsonProperty
  private final String prefix;
  @JsonProperty
  private final String separator;
  @JsonProperty
  private final Boolean includeHost;
  @JsonProperty
  private final String dimensionMapPath;
  @JsonProperty
  private final String blankHolder;
  @JsonProperty
  private final Boolean dogstatsd;
  @JsonProperty
  private final List<String> dogstatsdConstantTags;

  @JsonCreator
  public StatsDEmitterConfig(
      @JsonProperty("hostname") String hostname,
      @JsonProperty("port") Integer port,
      @JsonProperty("prefix") String prefix,
      @JsonProperty("separator") String separator,
      @JsonProperty("includeHost") Boolean includeHost,
      @JsonProperty("dimensionMapPath") String dimensionMapPath,
      @JsonProperty("blankHolder") String blankHolder,
      @JsonProperty("dogstatsd") Boolean dogstatsd,
      @JsonProperty("dogstatsdConstantTags") List<String> dogstatsdConstantTags
  )
  {
    this.hostname = Preconditions.checkNotNull(hostname, "StatsD hostname cannot be null.");
    this.port = Preconditions.checkNotNull(port, "StatsD port cannot be null.");
    this.prefix = prefix != null ? prefix : "";
    this.separator = separator != null ? separator : ".";
    this.includeHost = includeHost != null ? includeHost : false;
    this.dimensionMapPath = dimensionMapPath;
    this.blankHolder = blankHolder != null ? blankHolder : "-";
    this.dogstatsd = dogstatsd != null ? dogstatsd : false;
    this.dogstatsdConstantTags = dogstatsdConstantTags != null ? dogstatsdConstantTags : Collections.emptyList();
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
    if (dimensionMapPath != null ? !dimensionMapPath.equals(that.dimensionMapPath) : that.dimensionMapPath != null) {
      return false;
    }
    if (dogstatsd != null ? !dogstatsd.equals(that.dogstatsd) : that.dogstatsd != null) {
      return false;
    }
    return dogstatsdConstantTags != null ? dogstatsdConstantTags.equals(that.dogstatsdConstantTags)
            : that.dogstatsdConstantTags == null;

  }

  @Override
  public int hashCode()
  {
    return Objects.hash(hostname, port, prefix, separator, includeHost, dimensionMapPath,
            blankHolder, dogstatsd, dogstatsdConstantTags);
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

  @JsonProperty
  public String getBlankHolder()
  {
    return blankHolder;
  }

  @JsonProperty
  public Boolean isDogstatsd()
  {
    return dogstatsd;
  }

  @JsonProperty
  public List<String> getDogstatsdConstantTags()
  {
    return dogstatsdConstantTags;
  }
}

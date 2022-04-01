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

import javax.annotation.Nullable;
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
  @Nullable
  private final String dimensionMapPath;
  @JsonProperty
  private final String blankHolder;
  @JsonProperty
  private final Boolean dogstatsd;
  @JsonProperty
  private final List<String> dogstatsdConstantTags;
  @JsonProperty
  private final Boolean dogstatsdServiceAsTag;
  @JsonProperty
  private final Boolean dogstatsdEvents;

  @JsonCreator
  public StatsDEmitterConfig(
      @JsonProperty("hostname") String hostname,
      @JsonProperty("port") Integer port,
      @JsonProperty("prefix") @Nullable String prefix,
      @JsonProperty("separator") @Nullable String separator,
      @JsonProperty("includeHost") @Nullable Boolean includeHost,
      @JsonProperty("dimensionMapPath") @Nullable String dimensionMapPath,
      @JsonProperty("blankHolder") @Nullable String blankHolder,
      @JsonProperty("dogstatsd") @Nullable Boolean dogstatsd,
      @JsonProperty("dogstatsdConstantTags") @Nullable List<String> dogstatsdConstantTags,
      @JsonProperty("dogstatsdServiceAsTag") @Nullable Boolean dogstatsdServiceAsTag,
      @JsonProperty("dogstatsdEvents") @Nullable Boolean dogstatsdEvents
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
    this.dogstatsdServiceAsTag = dogstatsdServiceAsTag != null ? dogstatsdServiceAsTag : false;
    this.dogstatsdEvents = dogstatsdEvents != null ? dogstatsdEvents : false;
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

    if (!Objects.equals(hostname, that.hostname)) {
      return false;
    }
    if (!Objects.equals(port, that.port)) {
      return false;
    }
    if (!Objects.equals(prefix, that.prefix)) {
      return false;
    }
    if (!Objects.equals(separator, that.separator)) {
      return false;
    }
    if (!Objects.equals(includeHost, that.includeHost)) {
      return false;
    }
    if (!Objects.equals(dimensionMapPath, that.dimensionMapPath)) {
      return false;
    }
    if (!Objects.equals(dogstatsd, that.dogstatsd)) {
      return false;
    }
    if (!Objects.equals(dogstatsdServiceAsTag, that.dogstatsdServiceAsTag)) {
      return false;
    }
    return Objects.equals(dogstatsdConstantTags, that.dogstatsdConstantTags);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(hostname, port, prefix, separator, includeHost, dimensionMapPath,
            blankHolder, dogstatsd, dogstatsdConstantTags, dogstatsdServiceAsTag);
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
  @Nullable
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

  @JsonProperty
  public Boolean isDogstatsdServiceAsTag()
  {
    return dogstatsdServiceAsTag;
  }

  @JsonProperty
  public Boolean isDogstatsdEvents()
  {
    return dogstatsdEvents;
  }
}

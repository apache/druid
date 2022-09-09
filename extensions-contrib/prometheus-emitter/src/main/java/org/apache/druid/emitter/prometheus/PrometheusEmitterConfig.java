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

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 *
 */
public class PrometheusEmitterConfig
{

  static final Pattern PATTERN = Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*");

  @JsonProperty
  private final Strategy strategy;

  @JsonProperty
  @Nullable
  private final String namespace;

  @JsonProperty
  @Nullable
  private final String dimensionMapPath;

  @JsonProperty
  @Nullable
  private final Integer port;

  @JsonProperty
  @Nullable
  private final String pushGatewayAddress;

  @JsonProperty
  @Nullable
  private final Integer flushPeriod;

  @JsonProperty
  private final boolean addHostAsLabel;

  @JsonProperty
  private final boolean addServiceAsLabel;

  @JsonCreator
  public PrometheusEmitterConfig(
      @JsonProperty("strategy") @Nullable Strategy strategy,
      @JsonProperty("namespace") @Nullable String namespace,
      @JsonProperty("dimensionMapPath") @Nullable String dimensionMapPath,
      @JsonProperty("port") @Nullable Integer port,
      @JsonProperty("pushGatewayAddress") @Nullable String pushGatewayAddress,
      @JsonProperty("addHostAsLabel") boolean addHostAsLabel,
      @JsonProperty("addServiceAsLabel") boolean addServiceAsLabel,
      @JsonProperty("flushPeriod") Integer flushPeriod
  )
  {
    this.strategy = strategy != null ? strategy : Strategy.exporter;
    this.namespace = namespace != null ? namespace : "druid";
    Preconditions.checkArgument(PATTERN.matcher(this.namespace).matches(), "Invalid namespace " + this.namespace);
    if (strategy == Strategy.exporter) {
      Preconditions.checkArgument(port != null, "For `exporter` strategy, port must be specified.");
    } else if (this.strategy == Strategy.pushgateway) {
      Preconditions.checkArgument(pushGatewayAddress != null, "For `pushgateway` strategy, pushGatewayAddress must be specified.");
      if (Objects.nonNull(flushPeriod)) {
        Preconditions.checkArgument(flushPeriod > 0, "flushPeriod must be greater than 0.");
      } else {
        flushPeriod = 15;
      }
    }
    this.dimensionMapPath = dimensionMapPath;
    this.port = port;
    this.pushGatewayAddress = pushGatewayAddress;
    this.flushPeriod = flushPeriod;
    this.addHostAsLabel = addHostAsLabel;
    this.addServiceAsLabel = addServiceAsLabel;
  }

  public String getNamespace()
  {
    return namespace;
  }

  public String getDimensionMapPath()
  {
    return dimensionMapPath;
  }

  public int getPort()
  {
    return port;
  }

  public String getPushGatewayAddress()
  {
    return pushGatewayAddress;
  }

  @Nullable
  public Integer getFlushPeriod()
  {
    return flushPeriod;
  }

  public Strategy getStrategy()
  {
    return strategy;
  }

  public boolean isAddHostAsLabel()
  {
    return addHostAsLabel;
  }

  public boolean isAddServiceAsLabel()
  {
    return addServiceAsLabel;
  }

  public enum Strategy
  {
    exporter, pushgateway
  }
}

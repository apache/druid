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

  @JsonCreator
  public PrometheusEmitterConfig(
      @JsonProperty("strategy") @Nullable Strategy strategy,
      @JsonProperty("namespace") @Nullable String namespace,
      @JsonProperty("dimensionMapPath") @Nullable String dimensionMapPath,
      @JsonProperty("port") @Nullable Integer port,
      @JsonProperty("pushGatewayAddress") @Nullable String pushGatewayAddress
  )
  {

    this.strategy = strategy != null ? strategy : Strategy.exporter;
    this.namespace = namespace != null ? namespace : "druid";
    Preconditions.checkArgument(PATTERN.matcher(this.namespace).matches(), "Invalid namespace " + this.namespace);
    this.dimensionMapPath = dimensionMapPath;
    this.port = port;
    if (this.strategy == Strategy.pushgateway) {
      Preconditions.checkNotNull(pushGatewayAddress, "Invalid pushGateway address");
    }
    this.pushGatewayAddress = pushGatewayAddress;
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

  public Strategy getStrategy()
  {
    return strategy;
  }

  public enum Strategy
  {
    exporter, pushgateway
  }
}

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

  Pattern pattern = Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*");

  @JsonProperty
  private final String namespace;

  @JsonProperty
  @Nullable
  private final String dimensionMapPath;

  @JsonProperty
  private final Integer port;

  @JsonCreator
  public PrometheusEmitterConfig(
      @JsonProperty("namespace") @Nullable String namespace,
      @JsonProperty("dimensionMapPath") @Nullable String dimensionMapPath,
      @JsonProperty("port") Integer port
  )
  {
    this.namespace = namespace != null ? namespace : "druid";
    Preconditions.checkArgument(pattern.matcher(namespace).matches(), "Invalid namespace " + namespace);
    this.dimensionMapPath = dimensionMapPath;
    this.port = Preconditions.checkNotNull(port, "Prometheus server port cannot be null.");
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
}

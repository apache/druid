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

import javax.annotation.Nullable;

/**
 *
 */
public class PrometheusEmitterConfig
{

  @JsonProperty
  private final String namespace;

  @JsonProperty
  private final String path;

  @JsonProperty
  @Nullable
  private final String dimensionMapPath;

  @JsonCreator
  public PrometheusEmitterConfig(
      @JsonProperty("namespace") @Nullable String namespace,
      @JsonProperty("path") @Nullable String path,
      @JsonProperty("dimensionMapPath") @Nullable String dimensionMapPath
  )
  {
    this.namespace = namespace != null ? namespace : "druid";
    this.path = path != null ? path : "/prometheus";
    this.dimensionMapPath = dimensionMapPath;
  }

  public String getNamespace()
  {
    return namespace;
  }

  public String getPath()
  {
    return path;
  }

  public String getDimensionMapPath()
  {
    return dimensionMapPath;
  }
}

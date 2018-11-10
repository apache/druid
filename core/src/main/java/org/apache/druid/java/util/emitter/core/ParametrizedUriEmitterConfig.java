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

package org.apache.druid.java.util.emitter.core;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

public class ParametrizedUriEmitterConfig
{
  private static final BaseHttpEmittingConfig DEFAULT_HTTP_EMITTING_CONFIG = new BaseHttpEmittingConfig();

  @NotNull
  @JsonProperty
  private String recipientBaseUrlPattern;

  @JsonProperty("httpEmitting")
  private BaseHttpEmittingConfig httpEmittingConfig = DEFAULT_HTTP_EMITTING_CONFIG;

  public String getRecipientBaseUrlPattern()
  {
    return recipientBaseUrlPattern;
  }

  public HttpEmitterConfig buildHttpEmitterConfig(String baseUri)
  {
    return new HttpEmitterConfig(httpEmittingConfig, baseUri);
  }

  @Override
  public String toString()
  {
    return "ParametrizedUriEmitterConfig{" +
           "recipientBaseUrlPattern='" + recipientBaseUrlPattern + '\'' +
           ", httpEmittingConfig=" + httpEmittingConfig +
           '}';
  }
}

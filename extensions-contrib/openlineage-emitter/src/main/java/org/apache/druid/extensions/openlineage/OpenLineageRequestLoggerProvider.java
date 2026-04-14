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

package org.apache.druid.extensions.openlineage;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.log.RequestLoggerProvider;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 *  Configure logging type, namespace, transport type (http or default console), transportUrl in {@code runtime.properties} 
 */
@JsonTypeName("openlineage")
public class OpenLineageRequestLoggerProvider implements RequestLoggerProvider
{
  private static final Logger log = new Logger(OpenLineageRequestLoggerProvider.class);

  public enum TransportType
  {
    CONSOLE,
    HTTP
  }

  @JacksonInject
  @Json
  @NotNull
  private ObjectMapper jsonMapper;

  @JsonProperty
  @NotNull
  private String namespace = "druid://" + DruidNode.getDefaultHost();

  @JsonProperty
  @NotNull
  private TransportType transportType = TransportType.CONSOLE;

  @Nullable
  @JsonProperty
  private String transportUrl;

  @JsonProperty
  @NotNull
  private Set<String> excludedNativeQueryTypes = Set.of(
      "segmentMetadata",
      "dataSourceMetadata",
      "timeBoundary"
  );

  @Override
  public RequestLogger get()
  {
    log.debug("Creating OpenLineageRequestLogger [namespace=%s, transport=%s]", namespace, transportType);
    return new OpenLineageRequestLogger(jsonMapper, namespace, transportType, transportUrl, excludedNativeQueryTypes);
  }
}

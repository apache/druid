/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.log;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.druid.guice.annotations.Json;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.java.util.common.logger.Logger;

import javax.validation.constraints.NotNull;
import java.io.File;

/**
 */
@JsonTypeName("file")
public class FileRequestLoggerProvider implements RequestLoggerProvider
{
  private static final Logger log = new Logger(FileRequestLoggerProvider.class);

  @JsonProperty
  @NotNull
  private File dir = null;

  @JacksonInject
  @NotNull
  private ScheduledExecutorFactory factory = null;


  @JacksonInject
  @NotNull
  @Json
  private ObjectMapper jsonMapper = null;

  @Override
  public RequestLogger get()
  {
    FileRequestLogger logger = new FileRequestLogger(jsonMapper, factory.create(1, "RequestLogger-%s"), dir);
    log.info(new Exception("Stack trace"), "Creating %s at", logger);
    return logger;
  }
}

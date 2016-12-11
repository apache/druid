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

package io.druid.guice.http;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.validation.constraints.Min;

/**
 */

public class DruidHttpClientConfig
{
  private final String DEFAULT_COMPRESSION_CODEC = "gzip";

  @JsonProperty
  @Min(0)
  private int numConnections = 20;

  @JsonProperty
  private Period readTimeout = new Period("PT15M");

  @JsonProperty
  @Min(1)
  private int numMaxThreads = Math.max(10, (Runtime.getRuntime().availableProcessors() * 17) / 16 + 2) + 30;

  @JsonProperty
  private String compressionCodec = DEFAULT_COMPRESSION_CODEC;

  public int getNumConnections()
  {
    return numConnections;
  }

  public Duration getReadTimeout()
  {
    return readTimeout == null ? null : readTimeout.toStandardDuration();
  }

  public int getNumMaxThreads()
  {
    return numMaxThreads;
  }

  public String getCompressionCodec()
  {
    return compressionCodec;
  }
}

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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class HttpInputSourceConfig
{
  @VisibleForTesting
  public static final Set<String> DEFAULT_ALLOWED_PROTOCOLS = ImmutableSet.of("http", "https");

  @JsonProperty
  private final Set<String> allowedProtocols;

  @JsonCreator
  public HttpInputSourceConfig(
      @JsonProperty("allowedProtocols") @Nullable Set<String> allowedProtocols
  )
  {
    this.allowedProtocols = allowedProtocols == null || allowedProtocols.isEmpty()
                            ? DEFAULT_ALLOWED_PROTOCOLS
                            : allowedProtocols.stream().map(StringUtils::toLowerCase).collect(Collectors.toSet());
  }

  public Set<String> getAllowedProtocols()
  {
    return allowedProtocols;
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
    HttpInputSourceConfig that = (HttpInputSourceConfig) o;
    return Objects.equals(allowedProtocols, that.allowedProtocols);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(allowedProtocols);
  }

  @Override
  public String toString()
  {
    return "HttpInputSourceConfig{" +
           ", allowedProtocols=" + allowedProtocols +
           '}';
  }
}


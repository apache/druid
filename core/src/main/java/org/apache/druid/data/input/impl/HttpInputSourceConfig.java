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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class HttpInputSourceConfig
{
  @VisibleForTesting
  static final Set<String> DEFAULT_ALLOWED_PROTOCOLS = ImmutableSet.of("http", "https");

  @Nullable
  @JsonProperty
  private final List<String> allowListDomains;
  @Nullable
  @JsonProperty
  private final List<String> denyListDomains;
  @JsonProperty
  private final Set<String> allowedProtocols;

  @JsonCreator
  public HttpInputSourceConfig(
      @JsonProperty("allowListDomains") @Nullable List<String> allowListDomains,
      @JsonProperty("denyListDomains") @Nullable List<String> denyListDomains,
      @JsonProperty("allowedProtocols") @Nullable Set<String> allowedProtocols
  )
  {
    Preconditions.checkArgument(
        denyListDomains == null || allowListDomains == null,
        "Can only use one of allowList or blackList"
    );
    this.allowListDomains = allowListDomains;
    this.denyListDomains = denyListDomains;
    this.allowedProtocols = allowedProtocols == null || allowedProtocols.isEmpty()
                            ? DEFAULT_ALLOWED_PROTOCOLS
                            : allowedProtocols.stream().map(StringUtils::toLowerCase).collect(Collectors.toSet());
  }

  @Nullable
  public List<String> getAllowListDomains()
  {
    return allowListDomains;
  }

  @Nullable
  public List<String> getDenyListDomains()
  {
    return denyListDomains;
  }

  public Set<String> getAllowedProtocols()
  {
    return allowedProtocols;
  }

  private static boolean matchesAny(List<String> domains, URI uri)
  {
    for (String domain : domains) {
      if (uri.getHost().endsWith(domain)) {
        return true;
      }
    }
    return false;
  }

  public boolean isURIAllowed(URI uri)
  {
    if (allowListDomains != null) {
      return matchesAny(allowListDomains, uri);
    }
    if (denyListDomains != null) {
      return !matchesAny(denyListDomains, uri);
    }
    // No blacklist/allowList configured, all URLs are allowed
    return true;
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
    return Objects.equals(allowListDomains, that.allowListDomains) &&
           Objects.equals(denyListDomains, that.denyListDomains) &&
           Objects.equals(allowedProtocols, that.allowedProtocols);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(allowListDomains, denyListDomains, allowedProtocols);
  }

  @Override
  public String toString()
  {
    return "HttpInputSourceConfig{" +
           "allowListDomains=" + allowListDomains +
           ", denyListDomains=" + denyListDomains +
           ", allowedProtocols=" + allowedProtocols +
           '}';
  }
}


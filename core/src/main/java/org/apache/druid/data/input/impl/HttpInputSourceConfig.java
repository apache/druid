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
import com.google.common.base.Preconditions;

import java.net.URI;
import java.util.List;
import java.util.Objects;

public class HttpInputSourceConfig
{
  @JsonProperty
  private final List<String> allowListDomains;
  @JsonProperty
  private final List<String> denyListDomains;

  @JsonCreator
  public HttpInputSourceConfig(
      @JsonProperty("allowListDomains") List<String> allowListDomains,
      @JsonProperty("denyListDomains") List<String> denyListDomains
  )
  {
    this.allowListDomains = allowListDomains;
    this.denyListDomains = denyListDomains;
    Preconditions.checkArgument(
        this.denyListDomains == null || this.allowListDomains == null,
        "Can only use one of allowList or blackList"
    );
  }

  public List<String> getAllowListDomains()
  {
    return allowListDomains;
  }

  public List<String> getDenyListDomains()
  {
    return denyListDomains;
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
  public String toString()
  {
    return "HttpInputSourceConfig{" +
           "allowListDomains=" + allowListDomains +
           ", denyListDomains=" + denyListDomains +
           '}';
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
    HttpInputSourceConfig config = (HttpInputSourceConfig) o;
    return Objects.equals(allowListDomains, config.allowListDomains) &&
           Objects.equals(denyListDomains, config.denyListDomains);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(allowListDomains, denyListDomains);
  }
}


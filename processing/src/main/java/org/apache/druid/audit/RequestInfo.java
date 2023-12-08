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

package org.apache.druid.audit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Contains information about a REST API request that was audited.
 */
public class RequestInfo
{
  private final String service;
  private final String method;
  private final String uri;
  private final String queryParams;

  @JsonCreator
  public RequestInfo(
      @JsonProperty("service") String service,
      @JsonProperty("method") String method,
      @JsonProperty("uri") String uri,
      @JsonProperty("queryParams") String queryParams
  )
  {
    this.service = service;
    this.method = method;
    this.uri = uri;
    this.queryParams = queryParams;
  }

  @JsonProperty
  public String getService()
  {
    return service;
  }

  @JsonProperty
  public String getMethod()
  {
    return method;
  }

  @JsonProperty
  public String getUri()
  {
    return uri;
  }

  @JsonProperty
  public String getQueryParams()
  {
    return queryParams;
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
    RequestInfo that = (RequestInfo) o;
    return Objects.equals(this.service, that.service)
           && Objects.equals(this.method, that.method)
           && Objects.equals(this.uri, that.uri)
           && Objects.equals(this.queryParams, that.queryParams);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(service, method, uri, queryParams);
  }

  @Override
  public String toString()
  {
    return "RequestInfo{" +
           "service='" + service + '\'' +
           ", method='" + method + '\'' +
           ", path='" + uri + '\'' +
           ", queryParams='" + queryParams + '\'' +
           '}';
  }
}

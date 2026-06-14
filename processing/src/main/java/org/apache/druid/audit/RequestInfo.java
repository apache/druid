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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
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
  @Nullable
  private final String traceId;

  public RequestInfo(String service, String method, String uri, String queryParams)
  {
    this(service, method, uri, queryParams, null);
  }

  @JsonCreator
  public RequestInfo(
      @JsonProperty("service") String service,
      @JsonProperty("method") String method,
      @JsonProperty("uri") String uri,
      @JsonProperty("queryParams") String queryParams,
      @JsonProperty("traceId") @Nullable String traceId
  )
  {
    this.service = service;
    this.method = method;
    this.uri = uri;
    this.queryParams = queryParams;
    this.traceId = traceId;
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

  /**
   * Value of the {@link AuditManager#X_DRUID_TRACE_ID} header on the inbound HTTP request,
   * if present. Null otherwise. Carried through so audit consumers can correlate audit
   * events with the originating distributed-trace ID.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public String getTraceId()
  {
    return traceId;
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
           && Objects.equals(this.queryParams, that.queryParams)
           && Objects.equals(this.traceId, that.traceId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(service, method, uri, queryParams, traceId);
  }

  @Override
  public String toString()
  {
    return "RequestInfo{" +
           "service='" + service + '\'' +
           ", method='" + method + '\'' +
           ", path='" + uri + '\'' +
           ", queryParams='" + queryParams + '\'' +
           ", traceId='" + traceId + '\'' +
           '}';
  }
}

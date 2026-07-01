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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.BaseQuery;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Configures which inbound HTTP headers are captured and where they're stored in the
 * {@link org.apache.druid.query.Query} context. The map is keyed by header name (case as
 * sent on the wire) and the value is the reserved context key.
 *
 * <p>Defaults: {@code X-Druid-Trace-Id → traceId} so trace propagation works out of the
 * box. Operators can extend by setting:
 * <pre>
 *   druid.audit.requestHeaders.headerToContextKey={"X-Druid-Trace-Id": "traceId",
 *                                                  "X-Custom-Foo": "foo"}
 * </pre>
 *
 * <p>The values in this map are reserved context keys: user-supplied values for these keys
 * are stripped from the query context before the filter-captured value is merged
 * (anti-spoof). Setting overlapping keys to existing query-context keys is therefore
 * discouraged — pick context-key names that don't collide with standard Druid context.
 */
public class RequestHeaderContextConfig
{
  private static final Map<String, String> DEFAULT_HEADER_TO_CONTEXT_KEY = ImmutableMap.of(
      AuditManager.X_DRUID_TRACE_ID, "traceId"
  );

  /**
   * Context keys that are server-managed by Druid and must not be configurable as targets
   * for header propagation. Mapping a header to one of these would let a client overwrite
   * the server-assigned value (e.g. the random per-request {@code queryId}), breaking
   * query identification, cancellation, and metric correlation.
   */
  private static final Set<String> RESERVED_DRUID_CONTEXT_KEYS = ImmutableSet.of(
      BaseQuery.QUERY_ID,
      BaseQuery.SUB_QUERY_ID,
      BaseQuery.SQL_QUERY_ID
  );

  // @JsonProperty on the field is REQUIRED: JsonConfigurator.verifyClazzIsConfigurable()
  // rejects config classes whose bean-property fields lack a Jackson field annotation, which
  // would fail server startup (JettyServerModule binds this via JsonConfigProvider).
  // Declared as ImmutableMap (not Map) so getHeaderToContextKey() can return the field
  // reference directly without exposing a mutable internal representation (CodeQL).
  @JsonProperty
  private final ImmutableMap<String, String> headerToContextKey;

  public RequestHeaderContextConfig()
  {
    this(DEFAULT_HEADER_TO_CONTEXT_KEY);
  }

  /**
   * @param headerToContextKey explicit mapping; null means "use defaults" (the typical case
   *     when {@code JsonConfigProvider} deserializes from an empty config block);
   *     {@link java.util.Collections#emptyMap()} explicitly disables propagation.
   */
  @JsonCreator
  public RequestHeaderContextConfig(
      @JsonProperty("headerToContextKey") Map<String, String> headerToContextKey
  )
  {
    final Map<String, String> raw = headerToContextKey == null
                                    ? DEFAULT_HEADER_TO_CONTEXT_KEY
                                    : headerToContextKey;
    for (String contextKey : raw.values()) {
      if (RESERVED_DRUID_CONTEXT_KEYS.contains(contextKey)) {
        throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                            .ofCategory(DruidException.Category.INVALID_INPUT)
                            .build(
                                "druid.audit.requestHeaders.headerToContextKey maps a header to "
                                + "reserved Druid context key[%s]; this would let a client overwrite "
                                + "the server-assigned value. Reserved keys are %s.",
                                contextKey, RESERVED_DRUID_CONTEXT_KEYS
                            );
      }
    }
    this.headerToContextKey = ImmutableMap.copyOf(raw);
  }

  /**
   * Returns the configured map of {@code header → context-key}. Immutable. Empty means
   * no header propagation is performed (the feature is effectively disabled).
   */
  @JsonProperty
  public Map<String, String> getHeaderToContextKey()
  {
    return headerToContextKey;
  }

  /**
   * For each configured {@code header → contextKey}, if the supplied query context
   * carries a value for {@code contextKey}, invokes {@code headerSetter} with
   * {@code (headerName, stringifiedValue)}. Used by internal RPC clients
   * (broker → historical, etc.) to attach propagated headers to outbound requests so
   * the receiving node's filter captures them just as it would for a client-originated
   * request.
   */
  public void applyToOutboundRequest(Map<String, Object> queryContext, BiConsumer<String, String> headerSetter)
  {
    if (queryContext == null || queryContext.isEmpty() || headerToContextKey.isEmpty()) {
      return;
    }
    for (Map.Entry<String, String> entry : headerToContextKey.entrySet()) {
      final Object value = queryContext.get(entry.getValue());
      if (value != null) {
        headerSetter.accept(entry.getKey(), value.toString());
      }
    }
  }

  /**
   * For each configured {@code header → contextKey}, if the supplied captured-header map (keyed
   * by context key, as held by {@code RequestHeaderContext}) carries a value for {@code contextKey},
   * invokes {@code headerSetter} with {@code (headerName, value)}. Used by the shared outbound
   * HTTP client to forward configured headers on ALL inter-service calls made on the request
   * thread (not just queries), so the chain of internal calls carries the same header values.
   */
  public void applyCapturedHeaders(Map<String, String> capturedByContextKey, BiConsumer<String, String> headerSetter)
  {
    if (capturedByContextKey == null || capturedByContextKey.isEmpty() || headerToContextKey.isEmpty()) {
      return;
    }
    for (Map.Entry<String, String> entry : headerToContextKey.entrySet()) {
      final String value = capturedByContextKey.get(entry.getValue());
      if (value != null) {
        headerSetter.accept(entry.getKey(), value);
      }
    }
  }
}

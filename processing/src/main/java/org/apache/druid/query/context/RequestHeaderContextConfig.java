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

package org.apache.druid.query.context;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.IAE;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

/** Mapping from HTTP header names to application-specific query and task context keys. */
public class RequestHeaderContextConfig
{
  private static final Pattern HEADER_NAME = Pattern.compile("[!#$%&'*+.^_`|~0-9A-Za-z-]+");

  private static final Map<String, String> DEFAULT_HEADER_TO_CONTEXT_KEY =
      ImmutableMap.of("X-Druid-Trace-Id", "traceId");

  @JsonProperty
  private final ImmutableMap<String, String> headerToContextKey;

  public RequestHeaderContextConfig()
  {
    this(DEFAULT_HEADER_TO_CONTEXT_KEY);
  }

  @JsonCreator
  public RequestHeaderContextConfig(@JsonProperty("headerToContextKey") final Map<String, String> mapping)
  {
    final Map<String, String> configured = mapping == null ? DEFAULT_HEADER_TO_CONTEXT_KEY : mapping;
    final Set<String> headers = new HashSet<>();
    final Set<String> keys = new HashSet<>();
    configured.forEach((header, key) -> {
      if (header == null || !HEADER_NAME.matcher(header).matches()
          || !headers.add(header.toLowerCase(Locale.ROOT))) {
        throw new IAE("Invalid or duplicate request header name[%s]", header);
      }
      if (key == null || key.isBlank() || !keys.add(key) || QueryContextParameters.ALL.get().containsKey(key)) {
        throw new IAE("Invalid, duplicate, or built-in request header context key[%s]", key);
      }
    });
    this.headerToContextKey = ImmutableMap.copyOf(configured);
  }

  @JsonProperty
  public Map<String, String> getHeaderToContextKey()
  {
    return headerToContextKey;
  }

  public Map<String, String> capture(final Function<String, String> headerGetter)
  {
    final Map<String, String> values = new HashMap<>();
    headerToContextKey.forEach((header, key) -> {
      final String value = headerGetter.apply(header);
      if (isValidValue(value)) {
        values.put(key, value);
      }
    });
    return ImmutableMap.copyOf(values);
  }

  /** Reconstruct only configured headers from serialized query or task context. */
  public Map<String, String> fromContext(final Map<String, Object> context)
  {
    final Map<String, String> headers = new HashMap<>();
    headerToContextKey.forEach((header, key) -> {
      final Object value = context.get(key);
      if (value instanceof String && isValidValue((String) value)) {
        headers.put(header, (String) value);
      }
    });
    return ImmutableMap.copyOf(headers);
  }

  private static boolean isValidValue(final String value)
  {
    return value != null && !value.isBlank()
           && value.chars().allMatch(c -> c == '\t' || (c >= 0x20 && c != 0x7f));
  }
}

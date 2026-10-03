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

package org.apache.druid.query;

import org.apache.druid.query.context.QueryContextParameter;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Builds an immutable query context map using typed parameter descriptors.
 * Existing string-keyed maps can be copied with {@link #putAll(Map)} for backward compatibility.
 */
public final class QueryContextBuilder
{
  private final Map<String, Object> values = new LinkedHashMap<>();

  /**
   * Adds a context value using a raw string key.
   *
   * <p>This is an explicit escape hatch for keys that do not yet have a declared
   * {@link QueryContextParameter} descriptor.</p>
   */
  public QueryContextBuilder putRaw(final String name, @Nullable final Object value)
  {
    values.put(Objects.requireNonNull(name, "name"), value);
    return this;
  }

  /**
   * Adds all values from an existing query context map.
   */
  public QueryContextBuilder putAll(final Map<? extends String, ?> values)
  {
    values.forEach((name, value) -> this.values.put(Objects.requireNonNull(name, "name"), value));
    return this;
  }

  /**
   * Adds a context value using a typed parameter descriptor.
   */
  public <V> QueryContextBuilder put(final QueryContextParameter<V> parameter, @Nullable final V value)
  {
    values.put(parameter.getName(), parameter.validate(value));
    return this;
  }

  /**
   * Converts the current values to an immutable context map snapshot.
   */
  public Map<String, Object> toMap()
  {
    return Collections.unmodifiableMap(new LinkedHashMap<>(values));
  }

  /**
   * Converts the current values to an immutable {@link QueryContext} snapshot.
   */
  public QueryContext toContext()
  {
    return QueryContext.of(toMap());
  }
}

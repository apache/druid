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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Builds an immutable query context map using either string keys or typed parameter descriptors.
 */
public final class QueryContextBuilder
{
  private final Map<String, Object> values = new LinkedHashMap<>();

  /**
   * Adds a context value using its string key.
   */
  public QueryContextBuilder put(final String name, final Object value)
  {
    final String nonNullName = Objects.requireNonNull(name, "name");
    values.put(nonNullName, value);
    return this;
  }

  /**
   * Adds a context value using a typed parameter descriptor.
   */
  public <V> QueryContextBuilder put(final QueryContextParameter<V> parameter, final V value)
  {
    return put(parameter.getName(), parameter.validate(value));
  }

  /**
   * Builds the immutable context map.
   */
  public Map<String, Object> build()
  {
    return Collections.unmodifiableMap(new LinkedHashMap<>(values));
  }
}

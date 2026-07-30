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

package org.apache.druid.server.audit;

import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.Map;

/**
 * Holds the per-request map of headers captured from the inbound HTTP request, keyed by
 * the operator-configured context key (see {@link RequestHeaderContextConfig}). Bound to
 * the request-handling thread by a servlet filter, then read during query construction so
 * the captured values can be merged into {@link org.apache.druid.query.Query}'s context.
 *
 * <p>From there, Druid's existing native sub-query context propagation carries the headers
 * to historicals and peons. Consumers that already read the query context (request logger,
 * audit pipeline, lineage emitters, metric dimensions) pick them up automatically.
 *
 * <p>Only valid for the lifetime of the request thread. Background tasks and async work
 * spawned from the request thread will not see these values unless they explicitly copy them.
 */
public final class RequestHeaderContext
{
  private static final ThreadLocal<Map<String, String>> CURRENT = new ThreadLocal<>();

  private RequestHeaderContext()
  {
  }

  /**
   * Returns the captured headers for the current thread (keyed by context-key, not header
   * name). Empty if none bound. Never null.
   */
  public static Map<String, String> current()
  {
    final Map<String, String> map = CURRENT.get();
    return map == null ? Collections.emptyMap() : map;
  }

  /**
   * Binds the given map of context-key → value to the current thread. Should be paired with
   * {@link #clear()} via try/finally so values don't leak across threads in a pool.
   * Null or empty maps are treated as a clear.
   */
  public static void bind(Map<String, String> values)
  {
    if (values == null || values.isEmpty()) {
      CURRENT.remove();
    } else {
      CURRENT.set(ImmutableMap.copyOf(values));
    }
  }

  /**
   * Clears any captured headers bound to the current thread.
   */
  public static void clear()
  {
    CURRENT.remove();
  }
}

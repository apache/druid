/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.avatica;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Connection tracking for {@link DruidMeta}. Not thread-safe.
 */
public class DruidConnection
{
  private final Map<String, Object> context;
  private final Map<Integer, DruidStatement> statements;
  private Future<?> timeoutFuture;

  public DruidConnection(final Map<String, Object> context)
  {
    this.context = ImmutableMap.copyOf(context);
    this.statements = new HashMap<>();
  }

  public Map<String, Object> context()
  {
    return context;
  }

  public Map<Integer, DruidStatement> statements()
  {
    return statements;
  }

  public DruidConnection sync(final Future<?> newTimeoutFuture)
  {
    if (timeoutFuture != null) {
      timeoutFuture.cancel(false);
    }
    timeoutFuture = newTimeoutFuture;
    return this;
  }
}

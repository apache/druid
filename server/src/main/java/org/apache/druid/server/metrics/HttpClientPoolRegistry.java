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

package org.apache.druid.server.metrics;

import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.http.client.pool.ResourcePool;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The connection pool of every HTTP client of this process, under the name of the client that owns it. A process runs
 * several clients, so {@link HttpClientPoolMonitor} needs the name to keep their metrics apart.
 */
@LazySingleton
public class HttpClientPoolRegistry
{
  private final Map<String, ResourcePool.Counters> pools = new ConcurrentHashMap<>();

  public void register(String clientName, ResourcePool.Counters counters)
  {
    if (pools.putIfAbsent(clientName, counters) != null) {
      throw new ISE("Client[%s] is already registered", clientName);
    }
  }

  public Map<String, ResourcePool.Counters> getPools()
  {
    return Collections.unmodifiableMap(pools);
  }
}

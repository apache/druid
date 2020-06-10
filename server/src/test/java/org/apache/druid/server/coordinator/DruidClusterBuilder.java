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

package org.apache.druid.server.coordinator;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class DruidClusterBuilder
{
  public static DruidClusterBuilder newBuilder()
  {
    return new DruidClusterBuilder();
  }

  private @Nullable Set<ServerHolder> realtimes = null;
  private final Map<String, Iterable<ServerHolder>> historicals = new HashMap<>();
  private @Nullable Set<ServerHolder> brokers = null;

  private DruidClusterBuilder()
  {
  }

  public DruidClusterBuilder withRealtimes(ServerHolder... realtimes)
  {
    this.realtimes = new HashSet<>(Arrays.asList(realtimes));
    return this;
  }

  public DruidClusterBuilder withBrokers(ServerHolder... brokers)
  {
    this.brokers = new HashSet<>(Arrays.asList(brokers));
    return this;
  }

  public DruidClusterBuilder addTier(String tierName, ServerHolder... historicals)
  {
    if (this.historicals.putIfAbsent(tierName, Arrays.asList(historicals)) != null) {
      throw new IllegalArgumentException("Duplicate tier: " + tierName);
    }
    return this;
  }

  public DruidCluster build()
  {
    return DruidCluster.createDruidClusterFromBuilderInTest(realtimes, historicals, brokers);
  }
}

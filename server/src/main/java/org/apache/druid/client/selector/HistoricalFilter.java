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

package org.apache.druid.client.selector;

import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.client.BrokerViewOfCoordinatorConfig;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.query.CloneQueryMode;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class HistoricalFilter implements Function<Int2ObjectRBTreeMap<Set<QueryableDruidServer>>, Int2ObjectRBTreeMap<Set<QueryableDruidServer>>>
{
  public static final HistoricalFilter IDENTITY_FILTER = new HistoricalFilter(ImmutableSet::of);
  private final Supplier<Set<String>> serversToFilter;

  public HistoricalFilter(Supplier<Set<String>> serversToFilter)
  {
    this.serversToFilter = serversToFilter;
  }

  public HistoricalFilter(BrokerViewOfCoordinatorConfig configView, CloneQueryMode cloneQueryMode)
  {
    this.serversToFilter = () -> {
      switch (cloneQueryMode) {
        case CLONE_PREFERRED:
          // Remove servers being cloned targets, so that clones are queried.
          return configView.getServersBeingCloned();
        case EXCLUDE:
          // Remove clones, so that targets are queried.
          return configView.getClones();
        case INCLUDE:
          // Don't remove either
          return ImmutableSet.of();
        default:
          throw new IllegalStateException("Unexpected value: " + cloneQueryMode);
      }
    };
  }

  @Override
  public Int2ObjectRBTreeMap<Set<QueryableDruidServer>> apply(Int2ObjectRBTreeMap<Set<QueryableDruidServer>> historicalServers)
  {
    Set<String> serversToIgnore = serversToFilter.get();

    if (serversToIgnore.isEmpty()) {
      return historicalServers;
    }

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> filteredHistoricals = new Int2ObjectRBTreeMap<>();
    for (int priority : historicalServers.keySet()) {
      Set<QueryableDruidServer> servers = historicalServers.get(priority);
      filteredHistoricals.put(priority,
                              servers.stream()
                                     .filter(server -> !serversToIgnore.contains(server.getServer().getHost()))
                                     .collect(Collectors.toSet())
      );
    }

    return filteredHistoricals;
  }
}

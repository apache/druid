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
import org.apache.druid.client.CoordinatorDynamicConfigView;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.query.CloneQueryMode;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class HistoricalFilter implements Function<Int2ObjectRBTreeMap<Set<QueryableDruidServer>>, Int2ObjectRBTreeMap<Set<QueryableDruidServer>>>
{
  public static final HistoricalFilter IDENTITY_FILTER = new HistoricalFilter(ImmutableSet::of);

  public HistoricalFilter(Supplier<Set<String>> serversToIgnoreSupplier)
  {
    this.serversToIgnoreSupplier = serversToIgnoreSupplier;
  }

  public HistoricalFilter(CoordinatorDynamicConfigView configView, CloneQueryMode cloneQueryMode)
  {
    this.serversToIgnoreSupplier = () -> {
      final Set<String> serversToIgnore = new HashSet<>();

      switch (cloneQueryMode) {
        case ONLY:
          // Remove clone sources, so that targets are queried.
          serversToIgnore.addAll(configView.getSourceClusterServers());
          break;
        case EXCLUDE:
          // Remove clone sources, so that targets are queried.
          serversToIgnore.addAll(configView.getTargetCloneServers());
          break;
        case INCLUDE:
          // Don't remove either
          break;
      }
      return serversToIgnore;
    };
  }

  private final Supplier<Set<String>> serversToIgnoreSupplier;

  @Override
  public Int2ObjectRBTreeMap<Set<QueryableDruidServer>> apply(Int2ObjectRBTreeMap<Set<QueryableDruidServer>> historicalServers)
  {
    final Set<String> serversToIgnore = serversToIgnoreSupplier.get();
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

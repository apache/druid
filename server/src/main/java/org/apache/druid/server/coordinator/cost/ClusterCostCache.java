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

package org.apache.druid.server.coordinator.cost;

import com.google.common.base.Preconditions;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CollectionUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ClusterCostCache
{
  private final Map<String, ServerCostCache> serversCostCache;

  ClusterCostCache(Map<String, ServerCostCache> serversCostCache)
  {
    this.serversCostCache = Preconditions.checkNotNull(serversCostCache);
  }

  public double computeCost(String serverName, DataSegment dataSegment)
  {
    ServerCostCache serverCostCache = serversCostCache.get(serverName);
    return (serverCostCache != null) ? serverCostCache.computeCost(dataSegment) : 0.0;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builder(Map<String, Set<DataSegment>> segmentsByServerName)
  {
    Builder builder = builder();
    segmentsByServerName.forEach(
        (serverName, segments) -> segments.forEach(segment -> builder.addSegment(serverName, segment))
    );
    return builder;
  }

  public static class Builder
  {
    private final Map<String, ServerCostCache.Builder> serversCostCache = new HashMap<>();

    public void addSegment(String serverName, DataSegment dataSegment)
    {
      ServerCostCache.Builder builder = serversCostCache.computeIfAbsent(serverName, s -> ServerCostCache.builder());
      builder.addSegment(dataSegment);
    }

    public void removeSegment(String serverName, DataSegment dataSegment)
    {
      serversCostCache.computeIfPresent(
          serverName,
          (s, builder) -> builder.removeSegment(dataSegment).isEmpty() ? null : builder
      );
    }

    public void removeServer(String serverName)
    {
      serversCostCache.remove(serverName);
    }

    public ClusterCostCache build()
    {
      return new ClusterCostCache(
          CollectionUtils.mapValues(serversCostCache, ServerCostCache.Builder::build)
      );
    }
  }
}

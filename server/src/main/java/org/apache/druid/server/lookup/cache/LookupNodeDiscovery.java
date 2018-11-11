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

package org.apache.druid.server.lookup.cache;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.server.http.HostAndPortWithScheme;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;

/**
 * A Helper class that uses DruidNodeDiscovery to discover lookup nodes and tiers.
 */
public class LookupNodeDiscovery
{
  private final DruidNodeDiscovery druidNodeDiscovery;

  LookupNodeDiscovery(DruidNodeDiscoveryProvider druidNodeDiscoveryProvider)
  {
    this.druidNodeDiscovery = druidNodeDiscoveryProvider.getForService(LookupNodeService.DISCOVERY_SERVICE_KEY);
  }

  public Collection<HostAndPortWithScheme> getNodesInTier(String tier)
  {
    return Collections2.transform(
        Collections2.filter(
            druidNodeDiscovery.getAllNodes(),
            new Predicate<DiscoveryDruidNode>()
            {
              @Override
              public boolean apply(@Nullable DiscoveryDruidNode node)
              {
                return tier.equals(((LookupNodeService) node.getServices()
                                                            .get(LookupNodeService.DISCOVERY_SERVICE_KEY)).getLookupTier());
              }
            }
        ),
        new Function<DiscoveryDruidNode, HostAndPortWithScheme>()
        {
          @Override
          public HostAndPortWithScheme apply(@Nullable DiscoveryDruidNode input)
          {
            return HostAndPortWithScheme.fromString(
                input.getDruidNode().getServiceScheme(),
                input.getDruidNode().getHostAndPortToUse()
            );
          }
        }
    );
  }

  public Set<String> getAllTiers()
  {
    ImmutableSet.Builder<String> builder = new ImmutableSet.Builder<>();

    druidNodeDiscovery.getAllNodes().forEach(
        node -> builder.add(((LookupNodeService) node.getServices()
                                                     .get(LookupNodeService.DISCOVERY_SERVICE_KEY)).getLookupTier())
    );

    return builder.build();
  }
}

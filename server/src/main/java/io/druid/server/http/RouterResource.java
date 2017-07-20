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

package io.druid.server.http;

import com.google.inject.Inject;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.server.router.TieredBrokerHostSelector;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 */
@Path("/druid/router/v1")
public class RouterResource
{
  private final TieredBrokerHostSelector tieredBrokerHostSelector;

  @Inject
  public RouterResource(TieredBrokerHostSelector tieredBrokerHostSelector)
  {
    this.tieredBrokerHostSelector = tieredBrokerHostSelector;
  }

  @GET
  @Path("/brokers")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, List<String>> getBrokers()
  {
    Map<String, ServerDiscoverySelector> brokerSelectorMap = tieredBrokerHostSelector.getAllBrokers();

    Map<String, List<String>> brokersMap = new HashMap<>(brokerSelectorMap.size());

    for (Map.Entry<String, ServerDiscoverySelector> e : brokerSelectorMap.entrySet()) {
      brokersMap.put(e.getKey(), e.getValue().getAll().stream().map(s -> s.getHost()).collect(Collectors.toList()));
    }

    return brokersMap;
  }
}

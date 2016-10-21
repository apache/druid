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

package io.druid.server.router;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.query.Query;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class QueryHostFinder
{
  private static EmittingLogger log = new EmittingLogger(QueryHostFinder.class);

  private final TieredBrokerHostSelector hostSelector;

  private final ConcurrentHashMap<String, Server> serverBackup = new ConcurrentHashMap<>();

  @Inject
  public QueryHostFinder(
      TieredBrokerHostSelector hostSelector
  )
  {
    this.hostSelector = hostSelector;
  }

  public <T> Server findServer(Query<T> query)
  {
    final Pair<String, ServerDiscoverySelector> selected = hostSelector.select(query);
    return findServerInner(selected);
  }

  public Server findDefaultServer()
  {
    final Pair<String, ServerDiscoverySelector> selected = hostSelector.getDefaultLookup();
    return findServerInner(selected);
  }

  public Collection<String> getAllHosts()
  {
    return FluentIterable
        .from((Collection<ServerDiscoverySelector>) hostSelector.getAllBrokers().values())
        .transformAndConcat(
            new Function<ServerDiscoverySelector, Iterable<Server>>()
            {
              @Override
              public Iterable<Server> apply(ServerDiscoverySelector input)
              {
                return input.getAll();
              }
            }
        ).transform(new Function<Server, String>()
        {
          @Override
          public String apply(Server input)
          {
            return input.getHost();
          }
        }).toList();
  }

  public <T> String getHost(Query<T> query)
  {
    Server server = findServer(query);

    if (server == null) {
      log.makeAlert(
          "Catastrophic failure! No servers found at all! Failing request!"
      ).emit();

      throw new ISE("No server found for query[%s]", query);
    }

    final String host = server.getHost();
    log.debug("Selected [%s]", host);

    return host;
  }

  public String getDefaultHost()
  {
    Server server = findDefaultServer();

    if (server == null) {
      log.makeAlert(
          "Catastrophic failure! No servers found at all! Failing request!"
      ).emit();

      throw new ISE("No default server found!");
    }

    return server.getHost();
  }

  private Server findServerInner(final Pair<String, ServerDiscoverySelector> selected)
  {
    if (selected == null) {
      log.error("Danger, Will Robinson! Unable to find any brokers!");
    }

    final String serviceName = selected == null ? hostSelector.getDefaultServiceName() : selected.lhs;
    final ServerDiscoverySelector selector = selected == null ? null : selected.rhs;

    Server server = selector == null ? null : selector.pick();
    if (server == null) {
      log.error(
          "WTF?! No server found for serviceName[%s]. Using backup",
          serviceName
      );

      server = serverBackup.get(serviceName);

      if (server == null) {
        log.error(
            "WTF?! No backup found for serviceName[%s]. Using default[%s]",
            serviceName,
            hostSelector.getDefaultServiceName()
        );

        server = serverBackup.get(hostSelector.getDefaultServiceName());
      }
    }
    if (server != null) {
      serverBackup.put(serviceName, server);
    }

    return server;
  }
}

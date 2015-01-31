/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.router;

import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.query.Query;

import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class QueryHostFinder<T>
{
  private static EmittingLogger log = new EmittingLogger(QueryHostFinder.class);

  private final TieredBrokerHostSelector hostSelector;

  private final ConcurrentHashMap<String, Server> serverBackup = new ConcurrentHashMap<String, Server>();

  @Inject
  public QueryHostFinder(
      TieredBrokerHostSelector hostSelector
  )
  {
    this.hostSelector = hostSelector;
  }

  public Server findServer(Query<T> query)
  {
    final Pair<String, ServerDiscoverySelector> selected = hostSelector.select(query);
    return findServerInner(selected);
  }

  public Server findDefaultServer()
  {
    final Pair<String, ServerDiscoverySelector> selected = hostSelector.getDefaultLookup();
    return findServerInner(selected);
  }

  public String getHost(Query<T> query)
  {
    Server server = findServer(query);

    if (server == null) {
      log.makeAlert(
          "Catastrophic failure! No servers found at all! Failing request!"
      ).emit();

      throw new ISE("No server found for query[%s]", query);
    }

    log.debug("Selected [%s]", server.getHost());

    return server.getHost();
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

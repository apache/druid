/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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

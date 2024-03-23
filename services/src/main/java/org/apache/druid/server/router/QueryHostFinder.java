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

package org.apache.druid.server.router;

import com.google.inject.Inject;
import org.apache.druid.client.selector.Server;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.Query;
import org.apache.druid.sql.http.SqlQuery;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 */
public class QueryHostFinder
{
  private static EmittingLogger log = new EmittingLogger(QueryHostFinder.class);

  private final TieredBrokerHostSelector hostSelector;
  private final AvaticaConnectionBalancer avaticaConnectionBalancer;

  private final ConcurrentHashMap<String, Server> serverBackup = new ConcurrentHashMap<>();

  @Inject
  public QueryHostFinder(
      TieredBrokerHostSelector hostSelector,
      AvaticaConnectionBalancer avaticaConnectionBalancer
  )
  {
    this.hostSelector = hostSelector;
    this.avaticaConnectionBalancer = avaticaConnectionBalancer;
  }

  public <T> Server findServer(Query<T> query)
  {
    final Pair<String, Server> selected = hostSelector.select(query);
    return findServerInner(selected);
  }

  public Server findDefaultServer()
  {
    final Pair<String, Server> selected = hostSelector.getDefaultLookup();
    return findServerInner(selected);
  }

  public Collection<Server> getAllServers()
  {
    return hostSelector.getAllBrokers().values().stream()
                       .flatMap(Collection::stream)
                       .collect(Collectors.toList());
  }

  public Server findServerAvatica(String connectionId)
  {
    Server chosenServer = avaticaConnectionBalancer.pickServer(getAllServers(), connectionId);
    assertServerFound(
        chosenServer,
        "No server found for Avatica request with connectionId[%s]",
        connectionId
    );

    log.debug(
        "Balancer class[%s] sending request with connectionId[%s] to server[%s]",
        avaticaConnectionBalancer.getClass(),
        connectionId,
        chosenServer.getHost()
    );
    return chosenServer;
  }

  public Server findServerSql(SqlQuery sqlQuery)
  {
    Server server = findServerInner(hostSelector.selectForSql(sqlQuery));
    assertServerFound(
        server,
        "There are no available brokers for SQL query[%s]."
        + "Please check that your brokers are "
        + "running and healthy.",
        sqlQuery
    );
    return server;
  }

  public <T> Server pickServer(Query<T> query)
  {
    Server server = findServer(query);
    assertServerFound(
        server,
        "There are no available brokers for query[%s]."
        + "Please check that your brokers are "
        + "running and healthy.",
        query
    );
    return server;
  }

  public Server pickDefaultServer()
  {
    Server server = findDefaultServer();
    assertServerFound(
        server,
        "There are no available brokers. Please check that your brokers are running and healthy."
    );
    return server;
  }

  private Server findServerInner(final Pair<String, Server> selected)
  {
    if (selected == null) {
      log.error("Unable to find any brokers!");
    }

    final String serviceName = selected == null ? hostSelector.getDefaultServiceName() : selected.lhs;
    Server server = selected == null ? null : selected.rhs;

    if (server == null) {
      log.error(
          "No server found for serviceName[%s]. Using backup",
          serviceName
      );

      server = serverBackup.get(serviceName);

      if (server == null) {
        log.error(
            "No backup found for serviceName[%s]. Using default[%s]",
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

  private void assertServerFound(Server server, String messageFormat, Object... args)
  {
    if (server != null) {
      log.debug("Selected server[%s]", server.getHost());
      return;
    }

    log.makeAlert(
        "Catastrophic failure! No brokers found at all! Failing request!"
    ).emit();

    throw new ISE(messageFormat, args);
  }
}

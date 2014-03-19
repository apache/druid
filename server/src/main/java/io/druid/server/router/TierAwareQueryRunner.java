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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import io.druid.client.DirectDruidClient;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChestWarehouse;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class TierAwareQueryRunner<T> implements QueryRunner<T>
{
  private static EmittingLogger log = new EmittingLogger(TierAwareQueryRunner.class);

  private final QueryToolChestWarehouse warehouse;
  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;
  private final BrokerSelector<T> brokerSelector;
  private final TierConfig tierConfig;

  private final ConcurrentHashMap<String, ServerDiscoverySelector> selectorMap = new ConcurrentHashMap<String, ServerDiscoverySelector>();
  private final ConcurrentHashMap<String, Server> serverBackup = new ConcurrentHashMap<String, Server>();

  public TierAwareQueryRunner(
      QueryToolChestWarehouse warehouse,
      ObjectMapper objectMapper,
      HttpClient httpClient,
      BrokerSelector<T> brokerSelector,
      TierConfig tierConfig,
      ServerDiscoveryFactory serverDiscoveryFactory
  )
  {
    this.warehouse = warehouse;
    this.objectMapper = objectMapper;
    this.httpClient = httpClient;
    this.brokerSelector = brokerSelector;
    this.tierConfig = tierConfig;

    try {
      for (Map.Entry<String, String> entry : tierConfig.getTierToBrokerMap().entrySet()) {
        ServerDiscoverySelector selector = serverDiscoveryFactory.createSelector(entry.getValue());
        selector.start();
        // TODO: stop?
        selectorMap.put(entry.getValue(), selector);
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public Server findServer(Query<T> query)
  {
    String brokerServiceName = brokerSelector.select(query);

    if (brokerServiceName == null) {
      log.makeAlert(
          "WTF?! No brokerServiceName found for datasource[%s], intervals[%s]. Using default[%s].",
          query.getDataSource(),
          query.getIntervals(),
          tierConfig.getDefaultBrokerServiceName()
      ).emit();
      brokerServiceName = tierConfig.getDefaultBrokerServiceName();
    }

    ServerDiscoverySelector selector = selectorMap.get(brokerServiceName);

    Server server;
    if (selector == null) {
      log.makeAlert(
          "WTF?! No selector found for brokerServiceName[%s]. Using default selector for[%s]",
          brokerServiceName,
          tierConfig.getDefaultBrokerServiceName()
      ).emit();
      selector = selectorMap.get(tierConfig.getDefaultBrokerServiceName());

      if (selector != null) {
        server = selector.pick();
      } else {
        return null;
      }
    } else {
      server = selector.pick();
    }

    if (server == null) {
      log.error(
          "WTF?! No server found for brokerServiceName[%s]. Using backup",
          brokerServiceName
      );

      server = serverBackup.get(brokerServiceName);

      if (server == null) {
        log.makeAlert(
            "WTF?! No backup found for brokerServiceName[%s]. Using default[%s]",
            brokerServiceName,
            tierConfig.getDefaultBrokerServiceName()
        ).emit();

        server = serverBackup.get(tierConfig.getDefaultBrokerServiceName());
      }
    } else {
      serverBackup.put(brokerServiceName, server);
    }

    return server;
  }

  @Override
  public Sequence<T> run(Query<T> query)
  {
    Server server = findServer(query);

    if (server == null) {
      log.makeAlert(
          "Catastrophic failure! No servers found at all! Failing request!"
      ).emit();
      return Sequences.empty();
    }

    QueryRunner<T> client = new DirectDruidClient<T>(
        warehouse,
        objectMapper,
        httpClient,
        server.getHost()
    );

    return client.run(query);
  }
}

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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Charsets;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.client.selector.Server;
import io.druid.concurrent.Execs;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Json;
import io.druid.server.coordinator.rules.Rule;
import org.joda.time.Duration;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
@ManageLifecycle
public class CoordinatorRuleManager
{
  private static final Logger log = new Logger(CoordinatorRuleManager.class);

  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;
  private final Supplier<TieredBrokerConfig> config;
  private final ServerDiscoverySelector selector;

  private final StatusResponseHandler responseHandler;
  private final AtomicReference<ConcurrentHashMap<String, List<Rule>>> rules;

  private volatile ScheduledExecutorService exec;

  private final Object lock = new Object();

  private volatile boolean started = false;

  @Inject
  public CoordinatorRuleManager(
      @Global HttpClient httpClient,
      @Json ObjectMapper jsonMapper,
      Supplier<TieredBrokerConfig> config,
      ServerDiscoverySelector selector
  )
  {
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.selector = selector;

    this.responseHandler = new StatusResponseHandler(Charsets.UTF_8);
    this.rules = new AtomicReference<>(
        new ConcurrentHashMap<String, List<Rule>>()
    );
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      this.exec = Execs.scheduledSingleThreaded("CoordinatorRuleManager-Exec--%d");

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(0),
          config.get().getPollPeriod().toStandardDuration(),
          new Runnable()
          {
            @Override
            public void run()
            {
              poll();
            }
          }
      );

      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      rules.set(new ConcurrentHashMap<String, List<Rule>>());

      started = false;
      exec.shutdownNow();
      exec = null;
    }
  }

  public boolean isStarted()
  {
    return started;
  }

  public void poll()
  {
    try {
      String url = getRuleURL();
      if (url == null) {
        return;
      }

      StatusResponseHolder response = httpClient.get(new URL(url))
                                                .go(responseHandler)
                                                .get();

      ConcurrentHashMap<String, List<Rule>> newRules = new ConcurrentHashMap<String, List<Rule>>(
          (Map<String, List<Rule>>) jsonMapper.readValue(
              response.getContent(), new TypeReference<Map<String, List<Rule>>>()
          {
          }
          )
      );

      log.info("Got [%,d] rules", newRules.keySet().size());

      rules.set(newRules);
    }
    catch (Exception e) {
      log.error(e, "Exception while polling for rules");
    }
  }

  public List<Rule> getRulesWithDefault(final String dataSource)
  {
    List<Rule> retVal = Lists.newArrayList();
    Map<String, List<Rule>> theRules = rules.get();
    if (theRules.get(dataSource) != null) {
      retVal.addAll(theRules.get(dataSource));
    }
    if (theRules.get(config.get().getDefaultRule()) != null) {
      retVal.addAll(theRules.get(config.get().getDefaultRule()));
    }
    return retVal;
  }

  private String getRuleURL()
  {
    Server server = selector.pick();

    if (server == null) {
      log.error("No instances found for [%s]!", config.get().getCoordinatorServiceName());
      return null;
    }

    return String.format("http://%s%s", server.getHost(), config.get().getRulesEndpoint());
  }
}

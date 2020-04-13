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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.http.RulesResource;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
@ManageLifecycle
public class CoordinatorRuleManager
{
  private static final Logger LOG = new Logger(CoordinatorRuleManager.class);

  private static final TypeReference<Map<String, List<Rule>>> TYPE_REFERENCE =
      new TypeReference<Map<String, List<Rule>>>()
      {
      };

  private final ObjectMapper jsonMapper;
  private final Supplier<TieredBrokerConfig> config;
  private final AtomicReference<Map<String, List<Rule>>> rules;
  private final DruidLeaderClient druidLeaderClient;

  private final Object lock = new Object();

  private volatile boolean started = false;

  @GuardedBy("lock")
  private ScheduledExecutorService exec;

  @Inject
  public CoordinatorRuleManager(
      @Json ObjectMapper jsonMapper,
      Supplier<TieredBrokerConfig> config,
      @Coordinator DruidLeaderClient druidLeaderClient
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.druidLeaderClient = druidLeaderClient;

    this.rules = new AtomicReference<>(Collections.emptyMap());
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
          this::poll
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

      rules.set(Collections.emptyMap());

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
      StringFullResponseHolder response = druidLeaderClient.go(
          druidLeaderClient.makeRequest(HttpMethod.GET, RulesResource.RULES_ENDPOINT)
      );

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while polling rules, status[%s] content[%s]",
            response.getStatus(),
            response.getContent()
        );
      }

      final Map<String, List<Rule>> map = jsonMapper.readValue(response.getContent(), TYPE_REFERENCE);
      final Map<String, List<Rule>> immutableMapBuilder = Maps.newHashMapWithExpectedSize(map.size());
      map.forEach((k, list) -> immutableMapBuilder.put(k, Collections.unmodifiableList(list)));
      rules.set(Collections.unmodifiableMap(immutableMapBuilder));
    }
    catch (Exception e) {
      LOG.error(e, "Exception while polling for rules");
    }
  }

  public List<Rule> getRulesWithDefault(final String dataSource)
  {
    List<Rule> rulesWithDefault = new ArrayList<>();
    Map<String, List<Rule>> theRules = rules.get();
    List<Rule> dataSourceRules = theRules.get(dataSource);
    if (dataSourceRules != null) {
      rulesWithDefault.addAll(dataSourceRules);
    }
    List<Rule> defaultRules = theRules.get(config.get().getDefaultRule());
    if (defaultRules != null) {
      rulesWithDefault.addAll(defaultRules);
    }
    return rulesWithDefault;
  }

  /**
   * Returns the current snapshot of the rules.
   * This method should be used for only testing.
   */
  @VisibleForTesting
  Map<String, List<Rule>> getRules()
  {
    return rules.get();
  }
}

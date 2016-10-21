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

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.selector.HostSelector;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.query.Query;
import io.druid.server.coordinator.rules.LoadRule;
import io.druid.server.coordinator.rules.Rule;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class TieredBrokerHostSelector<T> implements HostSelector<T>
{
  private static EmittingLogger log = new EmittingLogger(TieredBrokerHostSelector.class);

  private final CoordinatorRuleManager ruleManager;
  private final TieredBrokerConfig tierConfig;
  private final ServerDiscoveryFactory serverDiscoveryFactory;
  private final ConcurrentHashMap<String, ServerDiscoverySelector> selectorMap = new ConcurrentHashMap<>();
  private final List<TieredBrokerSelectorStrategy> strategies;

  private final Object lock = new Object();

  private volatile boolean started = false;

  @Inject
  public TieredBrokerHostSelector(
      CoordinatorRuleManager ruleManager,
      TieredBrokerConfig tierConfig,
      ServerDiscoveryFactory serverDiscoveryFactory,
      List<TieredBrokerSelectorStrategy> strategies
  )
  {
    this.ruleManager = ruleManager;
    this.tierConfig = tierConfig;
    this.serverDiscoveryFactory = serverDiscoveryFactory;
    this.strategies = strategies;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      try {
        for (Map.Entry<String, String> entry : tierConfig.getTierToBrokerMap().entrySet()) {
          ServerDiscoverySelector selector = serverDiscoveryFactory.createSelector(entry.getValue());
          selector.start();
          selectorMap.put(entry.getValue(), selector);
        }
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }

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

      try {
        for (ServerDiscoverySelector selector : selectorMap.values()) {
          selector.stop();
        }
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }

      started = false;
    }
  }

  @Override
  public String getDefaultServiceName()
  {
    return tierConfig.getDefaultBrokerServiceName();
  }

  public Pair<String, ServerDiscoverySelector> select(final Query<T> query)
  {
    synchronized (lock) {
      if (!ruleManager.isStarted() || !started) {
        return getDefaultLookup();
      }
    }

    String brokerServiceName = null;

    for (TieredBrokerSelectorStrategy strategy : strategies) {
      final Optional<String> optionalName = strategy.getBrokerServiceName(tierConfig, query);
      if (optionalName.isPresent()) {
        brokerServiceName = optionalName.get();
        break;
      }
    }

    if (brokerServiceName == null) {
      // For Union Queries tier will be selected on the rules for first dataSource.
      List<Rule> rules = ruleManager.getRulesWithDefault(Iterables.getFirst(query.getDataSource().getNames(), null));

      // find the rule that can apply to the entire set of intervals
      DateTime now = new DateTime();
      int lastRulePosition = -1;
      LoadRule baseRule = null;

      for (Interval interval : query.getIntervals()) {
        int currRulePosition = 0;
        for (Rule rule : rules) {
          if (rule instanceof LoadRule && currRulePosition > lastRulePosition && rule.appliesTo(interval, now)) {
            lastRulePosition = currRulePosition;
            baseRule = (LoadRule) rule;
            break;
          }
          currRulePosition++;
        }
      }

      if (baseRule == null) {
        return getDefaultLookup();
      }

      // in the baseRule, find the broker of highest priority
      for (Map.Entry<String, String> entry : tierConfig.getTierToBrokerMap().entrySet()) {
        if (baseRule.getTieredReplicants().containsKey(entry.getKey())) {
          brokerServiceName = entry.getValue();
          break;
        }
      }
    }

    if (brokerServiceName == null) {
      log.error(
          "WTF?! No brokerServiceName found for datasource[%s], intervals[%s]. Using default[%s].",
          query.getDataSource(),
          query.getIntervals(),
          tierConfig.getDefaultBrokerServiceName()
      );
      brokerServiceName = tierConfig.getDefaultBrokerServiceName();
    }

    ServerDiscoverySelector retVal = selectorMap.get(brokerServiceName);

    if (retVal == null) {
      log.error(
          "WTF?! No selector found for brokerServiceName[%s]. Using default selector for[%s]",
          brokerServiceName,
          tierConfig.getDefaultBrokerServiceName()
      );
      retVal = selectorMap.get(tierConfig.getDefaultBrokerServiceName());
    }

    return new Pair<>(brokerServiceName, retVal);
  }

  public Pair<String, ServerDiscoverySelector> getDefaultLookup()
  {
    final String brokerServiceName = tierConfig.getDefaultBrokerServiceName();
    final ServerDiscoverySelector retVal = selectorMap.get(brokerServiceName);
    return new Pair<>(brokerServiceName, retVal);
  }

  public Map<String, ServerDiscoverySelector> getAllBrokers()
  {
    return Collections.unmodifiableMap(selectorMap);
  }
}

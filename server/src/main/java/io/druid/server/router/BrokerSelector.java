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

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.query.Query;
import io.druid.server.coordinator.rules.LoadRule;
import io.druid.server.coordinator.rules.Rule;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class BrokerSelector<T>
{
  private static EmittingLogger log = new EmittingLogger(BrokerSelector.class);

  private final CoordinatorRuleManager ruleManager;
  private final TierConfig tierConfig;
  private final ServerDiscoveryFactory serverDiscoveryFactory;
  private final ConcurrentHashMap<String, ServerDiscoverySelector> selectorMap = new ConcurrentHashMap<String, ServerDiscoverySelector>();

  private final Object lock = new Object();

  private volatile boolean started = false;

  @Inject
  public BrokerSelector(
      CoordinatorRuleManager ruleManager,
      TierConfig tierConfig,
      ServerDiscoveryFactory serverDiscoveryFactory
  )
  {
    this.ruleManager = ruleManager;
    this.tierConfig = tierConfig;
    this.serverDiscoveryFactory = serverDiscoveryFactory;
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

  public Pair<String, ServerDiscoverySelector> select(final Query<T> query)
  {
    synchronized (lock) {
      if (!ruleManager.isStarted() || !started) {
        return null;
      }
    }

    List<Rule> rules = ruleManager.getRulesWithDefault((query.getDataSource()).getName());

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
      return null;
    }

    // in the baseRule, find the broker of highest priority
    String brokerServiceName = null;
    for (Map.Entry<String, String> entry : tierConfig.getTierToBrokerMap().entrySet()) {
      if (baseRule.getTieredReplicants().containsKey(entry.getKey())) {
        brokerServiceName = entry.getValue();
        break;
      }
    }

    if (brokerServiceName == null) {
      log.makeAlert(
          "WTF?! No brokerServiceName found for datasource[%s], intervals[%s]. Using default[%s].",
          query.getDataSource(),
          query.getIntervals(),
          tierConfig.getDefaultBrokerServiceName()
      ).emit();
      brokerServiceName = tierConfig.getDefaultBrokerServiceName();
    }

    ServerDiscoverySelector retVal = selectorMap.get(brokerServiceName);

    if (retVal == null) {
      log.makeAlert(
          "WTF?! No selector found for brokerServiceName[%s]. Using default selector for[%s]",
          brokerServiceName,
          tierConfig.getDefaultBrokerServiceName()
      ).emit();
      retVal = selectorMap.get(tierConfig.getDefaultBrokerServiceName());
    }

    return new Pair<>(brokerServiceName, retVal);
  }
}

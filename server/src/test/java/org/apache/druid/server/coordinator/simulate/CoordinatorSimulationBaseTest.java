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

package org.apache.druid.server.coordinator.simulate;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Base test for coordinator simulations.
 * <p>
 * Each test must call {@link #startSimulation(CoordinatorSimulation)} to start
 * the simulation. {@link CoordinatorSimulation#stop()} should not be called as
 * the simulation is stopped when cleaning up after the test in {@link #tearDown()}.
 */
public class CoordinatorSimulationBaseTest
    implements CoordinatorSimulation.CoordinatorState, CoordinatorSimulation.ClusterState
{
  static final double DOUBLE_DELTA = 10e-9;

  static class Rules
  {
    static final List<Rule> T1_X1 = Collections.singletonList(
        new ForeverLoadRule(Collections.singletonMap(Tier.T1, 1))
    );
  }

  static class Tier
  {
    static final String T1 = "_default_tier";
  }

  private CoordinatorSimulation sim;

  @Before
  public void setUp()
  {
    sim = null;
  }

  @After
  public void tearDown()
  {
    if (sim != null) {
      sim.stop();
    }
  }

  void startSimulation(CoordinatorSimulation simulation)
  {
    this.sim = simulation;
    simulation.start();
  }

  @Override
  public void runCycle()
  {
    sim.coordinator().runCycle();
  }

  @Override
  public List<Event> getMetricEvents()
  {
    return sim.coordinator().getMetricEvents();
  }

  @Override
  public DruidServer getInventoryView(String serverName)
  {
    return sim.coordinator().getInventoryView(serverName);
  }

  @Override
  public void syncInventoryView()
  {
    sim.coordinator().syncInventoryView();
  }

  @Override
  public void loadQueuedSegments()
  {
    sim.cluster().loadQueuedSegments();
  }

  @Override
  public double getLoadPercentage(String datasource)
  {
    return sim.coordinator().getLoadPercentage(datasource);
  }

  // Verification methods
  void verifyDatasourceIsFullyLoaded(String datasource)
  {
    Assert.assertEquals(100.0, getLoadPercentage(datasource), DOUBLE_DELTA);
  }

  void verifyLatestMetricValue(String metricName, Number expectedValue)
  {
    final List<Number> observedValues = getMetricValues(metricName);
    Number latestValue = observedValues.get(observedValues.size() - 1);
    Assert.assertEquals(expectedValue, latestValue);
  }

  private List<Number> getMetricValues(String metricName)
  {
    final List<Number> metricValues = new ArrayList<>();

    for (Event event : sim.coordinator().getMetricEvents()) {
      final Map<String, Object> map = event.toMap();
      final String eventMetricName = (String) map.get("metric");
      if (eventMetricName != null && eventMetricName.equals(metricName)) {
        metricValues.add((Number) map.get("value"));
      }
    }

    return metricValues;
  }

  // Utility methods

  /**
   * Creates a tier of historicals.
   */
  static List<DruidServer> createHistoricalTier(String tier, int numServers, long serverSizeMb)
  {
    List<DruidServer> servers = new ArrayList<>();
    for (int i = 0; i < numServers; ++i) {
      servers.add(createHistorical(i, tier, serverSizeMb));
    }
    return servers;
  }

  /**
   * Creates a historical.
   */
  static DruidServer createHistorical(int uniqueIdInTier, String tier, long serverSizeMb)
  {
    final String name = tier + "__" + "hist__" + uniqueIdInTier;
    return new DruidServer(name, name, name, serverSizeMb, ServerType.HISTORICAL, tier, 1);
  }

}

package org.apache.druid.server.coordinator.simulate;

import org.apache.druid.client.DruidServer;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HistoricalCloningTest extends CoordinatorSimulationBaseTest
{
  private DruidServer historicalT11;
  private DruidServer historicalT12;

  private final String datasource = TestDataSource.WIKI;
  private final List<DataSegment> segments = Segments.WIKI_10X1D;

  @Override
  public void setUp()
  {
    // Setup historicals for 2 tiers, size 10 GB each
    historicalT11 = createHistorical(1, Tier.T1, 10_000);
    historicalT12 = createHistorical(2, Tier.T1, 10_000);
  }

  @Test
  public void testCloningHistorical()
  {
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12)
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .withDynamicConfig(
                                 withCloneServers(
                                     Map.of(
                                         historicalT11.getHost(), historicalT12.getHost()
                                     )
                                 ))
                             .build();

    startSimulation(sim);
    runCoordinatorCycle();
    loadQueuedSegments();

    verifyValue(Metric.ASSIGNED_COUNT, 10L);
    verifyValue(
        Stats.CoordinatorRun.CLONE_LOAD.getMetricName(),
        Map.of("server", historicalT12.getName()),
        10L
    );

    runCoordinatorCycle();
    verifyValue(
        Metric.SUCCESS_ACTIONS,
        Map.of("server", historicalT11.getName(), "description", "LOAD: NORMAL"),
        10L
    );
    verifyValue(
        Metric.SUCCESS_ACTIONS,
        Map.of("server", historicalT12.getName(), "description", "LOAD: TURBO"),
        10L
    );

    loadQueuedSegments();
    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(10, historicalT12.getTotalSegments());
  }

  /**
   * Creates a dynamic config with unlimited load queue, balancing disabled and
   * the given {@code replicationThrottleLimit}.
   */
  private CoordinatorDynamicConfig withCloneServers(Map<String, String> cloneServers)
  {
    final Set<String> unmanagedServers = new HashSet<>(cloneServers.values());

    return CoordinatorDynamicConfig.builder()
                                   .withSmartSegmentLoading(true)
                                   .withCloneServers(cloneServers)
                                   .withUnmanagedNodes(unmanagedServers)
                                   .withTurboLoadingNodes(unmanagedServers)
                                   .build();
  }
}

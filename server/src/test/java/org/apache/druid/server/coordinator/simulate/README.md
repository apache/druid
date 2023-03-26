<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Coordinator simulations

The simulation framework allows developers to recreate arbitrary cluster setups and verify coordinator behaviour. Tests
written using the framework can also help identify performance bottlenecks or potential bugs in the system and even
compare different balancing strategies.

As opposed to unit tests, simulations are meant to test the coordinator as a whole and verify the interactions of all
the underlying parts. In that regard, these simulations resemble integration tests more closely.

## Test targets

The primary test target is the `DruidCoordinator` itself. The behaviour of the following entities can also be verified
using simulations:

- `HttpLoadQueuePeon`, `LoadQueueTaskMaster`
- All coordinator duties, e.g. `BalanceSegments`, `RunRules`
- All retention rules

## Capabilities

The framework provides control over the following aspects of the setup:

| Input | Details | Actions |
|-------|---------|---------|
|cluster | server name, type, tier, size | add a server, remove a server|
|segment |datasource, interval, version, partition num, size | add/remove from server, mark used/unused, publish new segments|
|rules | type (foreverLoad, drop, etc), replica count per tier | set rules for a datasource| 
|configs |coordinator period, load queue type, load queue size, max segments to balance | set or update a config |

The above actions can be performed at any point after building the simulation. So, you could even recreate scenarios
where during a coordinator run, a server crashes or the retention rules of a datasource change, and verify the behaviour
of the coordinator in these situations.

## Design

1. __Execution__: A tight dependency on time durations such as the period of a repeating task or the delay before a
   scheduled task makes it difficult to reliably reproduce a test scenario. As a result, the tests become flaky. Thus,
   all the executors required for coordinator operations have been allowed only two possible modes of execution:
    - __immediate__: Execute tasks on the calling thread itself.
    - __blocked__: Keep tasks in a queue until explicitly invoked.
2. __Internal dependencies__: In order to allow realistic reproductions of the coordinator behaviour, none of the
   internal parts of the coordinator have been mocked in the framework and new tests need not mock anything at all.
3. __External dependencies__: Since these tests are meant to verify the behaviour of only the coordinator, the
   interfaces to communicate with external dependencies have been provided as simple in-memory implementations:
    - communication with metadata store: `SegmentMetadataManager`, `MetadataRuleManager`
    - communication with historicals: `HttpClient`, `ServerInventoryView`
    - `CuratorFramework`: provided as a mock as simulations of `CuratorLoadQueuePeon` are not supported yet
4. __Inventory__: The coordinator maintains an inventory view of the cluster state. Simulations can choose from two
   modes of inventory update - auto and manual. In auto update mode, any change made to the cluster is immediately
   reflected in the inventory view. In manual update mode, the inventory must be explicitly synchronized with the
   cluster state.

## Limitations

- The framework does not expose the coordinator HTTP endpoints.
- It should not be used to verify the absolute values of execution latencies, e.g. the time taken to compute the
  balancing cost of a segment. But the relative values can still be a good indicator while doing comparisons between,
  say two balancing strategies.
- It does not support simulation of the zk-based `CuratorLoadQueuePeon`.

## Usage

Writing a test class:

- Extend `CoordinatorSimulationBaseTest`. This base test exposes methods to get or set the state of the cluster and
  coordinator during a simulation.
- Build a simulation using `CoordinatorSimulation.builder()` with specified segments, servers, rules and configs.
- Start the simulation with `startSimulation(simulation)`.
- Invoke coordinator runs with `runCoordinatorCycle()`
- Verify emitted metrics and current cluster state

Example:

```java
public class SimpleSimulationTest extends CoordinatorSimulationBaseTest
{
  @Test
  public void testShiftSegmentsToDifferentTier()
  {
    // Create segments
    List<DataSegment> segments =
        CreateDataSegments.ofDatasource("wiki")
                          .forIntervals(30, Granularities.DAY)
                          .startingAt("2022-01-01")
                          .withNumPartitions(10)
                          .eachOfSizeInMb(500);

    // Create servers
    DruidServer historicalTier1 = createHistoricalTier(1, "tier_1", 10000);
    DruidServer historicalTier2 = createHistoricalTier(1, "tier_2", 20000);

    // Build simulation
    CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withServers(historicalTier1, historicalTier2)
                             .withSegments(segments)
                             .withRules("wiki".Load.on("tier_2", 1).forever())
                             .build();

    // Start the simulation with all segments loaded on tier_1
    segments.forEach(historicalTier1::addSegment);
    startSimulation(sim);

    // Run a few coordinator cycles
    int totalLoadedOnT2 = 0;
    int totalDroppedFromT1 = 0;
    for (int i = 0; i < 10; ++i) {
      runCoordinatorCycle();
      loadQueuedSegments();
      totalLoadedOnT2 += getValue("segment/assigned/count", filter("tier", "tier_2"));
      totalDroppedFromT1 += getValue("segment/dropped/count", filter("tier", "tier_1"));
    }

    // Verify that some segments have been loaded/dropped
    Assert.assertTrue(totalLoadedOnT2 > 0 && totalLoadedOnT2 <= segments.size());
    Assert.assertTrue(totalDroppedFromT1 > 0 && totalDroppedFromT1 <= segments.size());
    Assert.assertTrue(totalDroppedFromT1 <= totalLoadedOnT2);
  }
}
```

## More examples

See `org.apache.druid.server.coordinator.simulate.SegmentLoadingTest`

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

package org.apache.druid.server.coordinator.balancer;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class IntervalAwareBalancerStrategyTest
{
  private static final String DS_WIKI = "wiki";
  private static final String DS_KOALA = "koala";
  private static final String MINUTE_START = "2024-01-01T00:00:00.000Z";

  private IntervalAwareBalancerStrategy strategy;
  private int uniqueServerId;

  @Before
  public void setUp()
  {
    // Default tests use per-datasource scoping; the cross-datasource behaviour
    // is covered explicitly in the dedicated tests below.
    strategy = new IntervalAwareBalancerStrategy(true);
    uniqueServerId = 0;
  }

  @Test
  public void testLeastLoadedServerForIntervalIsPreferred()
  {
    // Segments for the target 1-minute interval
    final List<DataSegment> intervalSegments = minuteSegments(DS_WIKI, 6);

    // serverA already holds 3 of them, serverB holds 1, serverC holds none
    final ServerHolder serverA = createServerWith(intervalSegments.subList(0, 3));
    final ServerHolder serverB = createServerWith(intervalSegments.subList(3, 4));
    final ServerHolder serverC = createServerWith(new ArrayList<>());

    final DataSegment newSegment = intervalSegments.get(5);
    final Iterator<ServerHolder> ordered =
        strategy.findServersToLoadSegment(newSegment, Arrays.asList(serverA, serverB, serverC));

    // Emptiest server for this interval comes first, fullest last
    Assert.assertSame(serverC, ordered.next());
    Assert.assertSame(serverB, ordered.next());
    Assert.assertSame(serverA, ordered.next());
    Assert.assertFalse(ordered.hasNext());
  }

  @Test
  public void testPerDatasourceCountIgnoresOtherDatasources()
  {
    final IntervalAwareBalancerStrategy perDsStrategy = new IntervalAwareBalancerStrategy(true);

    final List<DataSegment> wikiSegments = minuteSegments(DS_WIKI, 3);
    final List<DataSegment> koalaSegments = minuteSegments(DS_KOALA, 3);

    // serverA holds 2 koala + 0 wiki (wiki count = 0 for the interval)
    // serverB holds 2 wiki (wiki count = 2 for the interval)
    final ServerHolder serverA = createServerWith(koalaSegments.subList(0, 2));
    final ServerHolder serverB = createServerWith(wikiSegments.subList(0, 2));

    // A new wiki segment should prefer serverA, because only the wiki count
    // matters in per-datasource mode (serverA has 0 wiki despite holding koala).
    final DataSegment newWiki = wikiSegments.get(2);
    final Iterator<ServerHolder> ordered =
        perDsStrategy.findServersToLoadSegment(newWiki, Arrays.asList(serverA, serverB));

    Assert.assertSame(serverA, ordered.next());
    Assert.assertSame(serverB, ordered.next());
  }

  @Test
  public void testTotalCountIsAcrossAllDatasources()
  {
    final IntervalAwareBalancerStrategy totalStrategy = new IntervalAwareBalancerStrategy(false);

    final List<DataSegment> wikiSegments = minuteSegments(DS_WIKI, 3);
    final List<DataSegment> koalaSegments = minuteSegments(DS_KOALA, 3);

    // serverA holds 2 wiki + 1 koala (total 3 for the interval)
    // serverB holds 1 wiki only (total 1 for the interval)
    final List<DataSegment> serverASegments = new ArrayList<>(wikiSegments.subList(0, 2));
    serverASegments.add(koalaSegments.get(0));
    final ServerHolder serverA = createServerWith(serverASegments);
    final ServerHolder serverB = createServerWith(wikiSegments.subList(2, 3));

    // A new koala segment should prefer serverB, because in total mode the count
    // is across all datasources, not just koala.
    final DataSegment newKoala = koalaSegments.get(1);
    final Iterator<ServerHolder> ordered =
        totalStrategy.findServersToLoadSegment(newKoala, Arrays.asList(serverA, serverB));

    Assert.assertSame(serverB, ordered.next());
    Assert.assertSame(serverA, ordered.next());
  }

  @Test
  public void testMovePrefersEmptierServerAndAvoidsOscillation()
  {
    final List<DataSegment> intervalSegments = minuteSegments(DS_WIKI, 4);

    // source holds 3 segments for the interval, dest holds 0
    final ServerHolder source = createServerWith(intervalSegments.subList(0, 3));
    final ServerHolder dest = createServerWith(new ArrayList<>());

    final DataSegment toMove = intervalSegments.get(0);
    final ServerHolder chosen =
        strategy.findDestinationServerToMoveSegment(toMove, source, Arrays.asList(source, dest));
    Assert.assertSame(dest, chosen);

    // When source has 2 and dest has 1 (differ by one), no move should happen
    // to avoid oscillation.
    final ServerHolder source2 = createServerWith(intervalSegments.subList(0, 2));
    final ServerHolder dest2 = createServerWith(intervalSegments.subList(2, 3));
    final ServerHolder chosen2 =
        strategy.findDestinationServerToMoveSegment(
            intervalSegments.get(0),
            source2,
            Arrays.asList(source2, dest2)
        );
    Assert.assertNull(chosen2);
  }

  @Test
  public void testMoveTieBreakIsNotBiasedTowardListOrder()
  {
    final List<DataSegment> intervalSegments = minuteSegments(DS_WIKI, 4);

    // Source holds 3 segments for the interval; two candidate destinations both
    // hold 0 (a tie at the minimum). Over many runs, both should be chosen — a
    // first-wins linear scan would always return destA.
    final ServerHolder source = createServerWith(intervalSegments.subList(0, 3));
    final ServerHolder destA = createServerWith(new ArrayList<>());
    final ServerHolder destB = createServerWith(new ArrayList<>());
    final DataSegment toMove = intervalSegments.get(0);

    int chosenA = 0;
    int chosenB = 0;
    for (int i = 0; i < 2000; i++) {
      final ServerHolder chosen =
          strategy.findDestinationServerToMoveSegment(toMove, source, Arrays.asList(destA, destB));
      if (chosen == destA) {
        chosenA++;
      } else if (chosen == destB) {
        chosenB++;
      }
    }

    // Both tied servers must be selected a meaningful fraction of the time.
    Assert.assertTrue("destA never chosen: " + chosenA, chosenA > 700);
    Assert.assertTrue("destB never chosen: " + chosenB, chosenB > 700);
    Assert.assertEquals(2000, chosenA + chosenB);
  }

  @Test
  public void testNodeLossReplicatesToLeastLoadedServerForInterval()
  {
    // Simulates a historical being deleted (e.g. pod delete): its replicas are
    // gone from the cluster view, so the segment is under-replicated and must be
    // re-loaded onto one of the surviving servers via the load path. The strategy
    // must pick the survivor holding the fewest segments for this interval.
    final List<DataSegment> intervalSegments = minuteSegments(DS_WIKI, 5);

    // The lost node held intervalSegments 0..2; these are no longer on any server.
    final DataSegment lostReplica = intervalSegments.get(0);

    // Surviving servers with differing load for the interval.
    final ServerHolder survivorHeavy = createServerWith(intervalSegments.subList(3, 5)); // count = 2
    final ServerHolder survivorLight = createServerWith(intervalSegments.subList(3, 4)); // count = 1
    final ServerHolder survivorEmpty = createServerWith(new ArrayList<>());              // count = 0

    final Iterator<ServerHolder> ordered = strategy.findServersToLoadSegment(
        lostReplica,
        Arrays.asList(survivorHeavy, survivorLight, survivorEmpty)
    );

    // Re-replication targets the emptiest survivor for the interval first.
    Assert.assertSame(survivorEmpty, ordered.next());
    Assert.assertSame(survivorLight, ordered.next());
    Assert.assertSame(survivorHeavy, ordered.next());
    Assert.assertFalse(ordered.hasNext());
  }

  @Test
  public void testDecommissioningSourceIsEvacuatedEvenWhenGuardWouldBlock()
  {
    // Simulates a historical being drained (e.g. pod drain / marked
    // decommissioning): every segment must be moved off it regardless of interval
    // balance. Here the source holds a single replica for the interval
    // (sourceCount = 1) and the only destination already holds one segment for the
    // same interval, so the anti-oscillation guard (bestCount + 1 < sourceCount)
    // would otherwise block the move and strand the segment on the draining node.
    final List<DataSegment> intervalSegments = minuteSegments(DS_WIKI, 3);

    final ServerHolder decommissioningSource = createDecommissioningServerWith(intervalSegments.subList(0, 1));
    final ServerHolder activeDest = createServerWith(intervalSegments.subList(1, 2));

    final DataSegment toMove = intervalSegments.get(0);
    // The caller excludes a decommissioning source from the destination list.
    final ServerHolder chosen = strategy.findDestinationServerToMoveSegment(
        toMove,
        decommissioningSource,
        Arrays.asList(activeDest)
    );

    Assert.assertSame(activeDest, chosen);
  }

  @Test
  public void testDecommissioningSourceEvacuatesToLeastLoadedDestination()
  {
    // When draining, the segment must still go to the emptiest destination for the
    // interval among the active servers.
    final List<DataSegment> intervalSegments = minuteSegments(DS_WIKI, 5);

    final ServerHolder decommissioningSource = createDecommissioningServerWith(intervalSegments.subList(0, 1));
    final ServerHolder destHeavy = createServerWith(intervalSegments.subList(1, 4)); // count = 3
    final ServerHolder destLight = createServerWith(intervalSegments.subList(4, 5)); // count = 1

    final DataSegment toMove = intervalSegments.get(0);
    final ServerHolder chosen = strategy.findDestinationServerToMoveSegment(
        toMove,
        decommissioningSource,
        Arrays.asList(destHeavy, destLight)
    );

    Assert.assertSame(destLight, chosen);
  }

  @Test
  public void testActiveSourceStillHonoursOscillationGuard()
  {
    // The evacuation fast-path must not affect a normal (non-decommissioning)
    // source: a move that does not strictly reduce the max per-interval count is
    // still skipped. Source has 1 for the interval, destination also has 1, so no
    // move should happen.
    final List<DataSegment> intervalSegments = minuteSegments(DS_WIKI, 3);

    final ServerHolder activeSource = createServerWith(intervalSegments.subList(0, 1));
    final ServerHolder activeDest = createServerWith(intervalSegments.subList(1, 2));

    final ServerHolder chosen = strategy.findDestinationServerToMoveSegment(
        intervalSegments.get(0),
        activeSource,
        Arrays.asList(activeDest)
    );

    Assert.assertNull(chosen);
  }

  @Test
  public void testDropPrefersMostLoadedServerForInterval()
  {
    final List<DataSegment> intervalSegments = minuteSegments(DS_WIKI, 5);

    final ServerHolder serverA = createServerWith(intervalSegments.subList(0, 1));
    final ServerHolder serverB = createServerWith(intervalSegments.subList(1, 4));

    final DataSegment segmentToDrop = intervalSegments.get(0);
    final Iterator<ServerHolder> ordered =
        strategy.findServersToDropSegment(segmentToDrop, Arrays.asList(serverA, serverB));

    // Most heavily loaded server for the interval should be dropped from first
    Assert.assertSame(serverB, ordered.next());
    Assert.assertSame(serverA, ordered.next());
  }

  @Test
  public void testFullServerIsExcludedFromLoadCandidates()
  {
    final List<DataSegment> targetInterval = minuteSegments(DS_WIKI, 4);
    // A different interval, used only to fill up the "full" server.
    final List<DataSegment> otherInterval =
        CreateDataSegments.ofDatasource(DS_WIKI)
                          .forIntervals(1, Granularities.MINUTE)
                          .startingAt("2024-01-01T01:00:00.000Z")
                          .withNumPartitions(2)
                          .eachOfSizeInMb(100);

    // Full server: 200 MB capacity holding 2 x 100 MB segments of another interval.
    // It therefore has 0 segments for the target interval (so by count alone it
    // would rank first), but it cannot fit any more data.
    final ServerHolder fullServer = createServerWith(otherInterval, 200L << 20);
    // Normal server holds 1 segment of the target interval (count = 1).
    final ServerHolder normalServer = createServerWith(targetInterval.subList(0, 1));

    final DataSegment newSegment = targetInterval.get(1);
    final Iterator<ServerHolder> ordered =
        strategy.findServersToLoadSegment(newSegment, Arrays.asList(fullServer, normalServer));

    final List<ServerHolder> result = new ArrayList<>();
    ordered.forEachRemaining(result::add);

    // Despite having the lowest interval count, the full server must be filtered out.
    Assert.assertFalse("full server must be excluded", result.contains(fullServer));
    Assert.assertTrue(result.contains(normalServer));
    Assert.assertEquals(1, result.size());
  }

  @Test
  public void testGetStatsTracksComputation()
  {
    final List<DataSegment> intervalSegments = minuteSegments(DS_WIKI, 2);
    final ServerHolder serverA = createServerWith(new ArrayList<>());
    final ServerHolder serverB = createServerWith(intervalSegments.subList(0, 1));

    strategy.findServersToLoadSegment(intervalSegments.get(1), Arrays.asList(serverA, serverB));

    final CoordinatorRunStats computeStats = strategy.getStats();
    Assert.assertEquals(1L, computeStats.get(Stats.Balancer.COMPUTATION_COUNT));
    Assert.assertTrue(computeStats.get(Stats.Balancer.COMPUTATION_TIME) >= 0);
  }

  private List<DataSegment> minuteSegments(String datasource, int count)
  {
    return CreateDataSegments.ofDatasource(datasource)
                             .forIntervals(1, Granularities.MINUTE)
                             .startingAt(MINUTE_START)
                             .withNumPartitions(count)
                             .eachOfSizeInMb(100);
  }

  private ServerHolder createServerWith(List<DataSegment> segments)
  {
    return createServerWith(segments, 10L << 30);
  }

  private ServerHolder createServerWith(List<DataSegment> segments, long maxSizeBytes)
  {
    return new ServerHolder(buildServer(segments, maxSizeBytes).toImmutableDruidServer(), new TestLoadQueuePeon());
  }

  private ServerHolder createDecommissioningServerWith(List<DataSegment> segments)
  {
    return new ServerHolder(
        buildServer(segments, 10L << 30).toImmutableDruidServer(),
        new TestLoadQueuePeon(),
        true
    );
  }

  private DruidServer buildServer(List<DataSegment> segments, long maxSizeBytes)
  {
    final String name = "hist_" + uniqueServerId++;
    final DruidServer server =
        new DruidServer(name, name, null, maxSizeBytes, null, ServerType.HISTORICAL, "hot", 1);
    for (DataSegment segment : segments) {
      server.addDataSegment(segment);
    }
    return server;
  }
}

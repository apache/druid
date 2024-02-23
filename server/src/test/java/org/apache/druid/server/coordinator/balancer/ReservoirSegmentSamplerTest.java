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

import com.google.common.collect.Lists;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ReservoirSegmentSamplerTest
{

  /**
   * num segments = 10 x 100 days
   */
  private final List<DataSegment> segments =
      CreateDataSegments.ofDatasource("wiki")
                        .forIntervals(100, Granularities.DAY)
                        .startingAt("2022-01-01")
                        .withNumPartitions(10)
                        .eachOfSizeInMb(100);

  @Before
  public void setUp()
  {
  }

  //checks if every segment is selected at least once out of 50 trials
  @Test
  public void testEverySegmentGetsPickedAtleastOnce()
  {
    int iterations = 50;

    final List<ServerHolder> servers = Arrays.asList(
        createHistorical("server1", segments.get(0)),
        createHistorical("server2", segments.get(1)),
        createHistorical("server3", segments.get(2)),
        createHistorical("server4", segments.get(3))
    );
    Map<DataSegment, Integer> segmentCountMap = new HashMap<>();
    for (int i = 0; i < iterations; i++) {
      // due to the pseudo-randomness of this method, we may not select a segment every single time no matter what.
      segmentCountMap.compute(
          ReservoirSegmentSampler
              .pickMovableSegmentsFrom(servers, 1, ServerHolder::getServedSegments, Collections.emptySet())
              .get(0).getSegment(),
          (segment, count) -> count == null ? 1 : count + 1
      );
    }

    // Verify that each segment has been chosen at least once
    Assert.assertEquals(4, segmentCountMap.size());
  }

  /**
   * Makes sure that the segment on server4 is never chosen in 5k iterations because it should never have its segment
   * checked due to the limit on segment candidates
   */
  @Test
  public void getRandomBalancerSegmentHolderTestSegmentsToConsiderLimit()
  {
    int iterations = 50;

    final DataSegment excludedSegment = segments.get(3);
    final List<ServerHolder> servers = Arrays.asList(
        createHistorical("server1", segments.get(0)),
        createHistorical("server2", segments.get(1)),
        createHistorical("server3", segments.get(2)),
        createHistorical("server4", excludedSegment)
    );

    Map<DataSegment, Integer> segmentCountMap = new HashMap<>();

    final double percentOfSegmentsToConsider = 75.0;
    for (int i = 0; i < iterations; i++) {
      segmentCountMap.compute(
          ReservoirSegmentSampler
              .getRandomBalancerSegmentHolder(servers, Collections.emptySet(), percentOfSegmentsToConsider)
              .getSegment(),
          (segment, count) -> count == null ? 1 : count + 1
      );
    }

    // Verify that the segment on server4 is never chosen because of limit
    Assert.assertFalse(segmentCountMap.containsKey(excludedSegment));
    Assert.assertEquals(3, segmentCountMap.size());
  }

  @Test
  public void testPickLoadingOrLoadedSegments()
  {
    final List<DataSegment> loadedSegments = Arrays.asList(segments.get(0), segments.get(1));
    final List<DataSegment> loadingSegments = Arrays.asList(segments.get(2), segments.get(3));

    final ServerHolder server1 = createHistorical("server1", loadedSegments.get(0));
    server1.startOperation(SegmentAction.LOAD, loadingSegments.get(0));

    final ServerHolder server2 = createHistorical("server2", loadedSegments.get(1));
    server2.startOperation(SegmentAction.LOAD, loadingSegments.get(1));

    // Pick only loading segments
    Set<DataSegment> pickedSegments = ReservoirSegmentSampler
        .pickMovableSegmentsFrom(
            Arrays.asList(server1, server2),
            10,
            ServerHolder::getLoadingSegments,
            Collections.emptySet()
        )
        .stream().map(BalancerSegmentHolder::getSegment).collect(Collectors.toSet());

    // Verify that only loading segments are picked
    Assert.assertEquals(loadingSegments.size(), pickedSegments.size());
    Assert.assertTrue(pickedSegments.containsAll(loadingSegments));

    // Pick only loaded segments
    List<BalancerSegmentHolder> pickedHolders = ReservoirSegmentSampler.pickMovableSegmentsFrom(
        Arrays.asList(server1, server2),
        10,
        ServerHolder::getServedSegments,
        Collections.emptySet()
    );
    pickedSegments = pickedHolders
        .stream()
        .map(BalancerSegmentHolder::getSegment)
        .collect(Collectors.toSet());

    // Verify that only loaded segments are picked
    Assert.assertEquals(loadedSegments.size(), pickedSegments.size());
    Assert.assertTrue(pickedSegments.containsAll(loadedSegments));
  }

  @Test
  public void testSegmentsOnBrokersAreIgnored()
  {
    final ServerHolder historical = createHistorical("hist1", segments.get(0), segments.get(1));

    final ServerHolder broker = new ServerHolder(
        new DruidServer("broker1", "broker1", null, 1000, ServerType.BROKER, null, 1)
            .addDataSegment(segments.get(2))
            .addDataSegment(segments.get(3))
            .toImmutableDruidServer(),
        new TestLoadQueuePeon()
    );

    // Try to pick all the segments on the servers
    List<BalancerSegmentHolder> pickedSegments = ReservoirSegmentSampler.pickMovableSegmentsFrom(
        Arrays.asList(historical, broker),
        10,
        ServerHolder::getServedSegments,
        Collections.emptySet()
    );

    // Verify that only the segments on the historical are picked
    Assert.assertEquals(2, pickedSegments.size());
    for (BalancerSegmentHolder holder : pickedSegments) {
      Assert.assertEquals(historical, holder.getServer());
    }
  }

  @Test
  public void testBroadcastSegmentsAreIgnored()
  {
    // num segments = 1 x 4 days
    final String broadcastDatasource = "ds_broadcast";
    final List<DataSegment> broadcastSegments
        = CreateDataSegments.ofDatasource(broadcastDatasource)
                            .forIntervals(4, Granularities.DAY)
                            .startingAt("2022-01-01")
                            .withNumPartitions(1)
                            .eachOfSizeInMb(100);

    final List<ServerHolder> servers = Arrays.asList(
        createHistorical("server1", broadcastSegments.toArray(new DataSegment[0])),
        createHistorical("server2", segments.get(0), segments.get(1))
    );

    // Try to pick all the segments on the servers
    List<BalancerSegmentHolder> pickedSegments = ReservoirSegmentSampler.pickMovableSegmentsFrom(
        servers,
        10,
        ServerHolder::getServedSegments,
        Collections.singleton(broadcastDatasource)
    );

    // Verify that none of the broadcast segments are picked
    Assert.assertEquals(2, pickedSegments.size());
    for (BalancerSegmentHolder holder : pickedSegments) {
      Assert.assertNotEquals(broadcastDatasource, holder.getSegment().getDataSource());
    }
  }

  @Test
  public void testSegmentsFromAllServersAreEquallyLikelyToBePicked()
  {
    // Create 4 servers, each having an equal number of segments
    final List<List<DataSegment>> subSegmentLists = Lists.partition(segments, segments.size() / 4);
    final List<ServerHolder> servers = IntStream.range(0, 4).mapToObj(
        i -> createHistorical("server_" + i, subSegmentLists.get(i).toArray(new DataSegment[0]))
    ).collect(Collectors.toList());

    // Get the distribution of picked segments for different sample percentages
    final int[] samplePercentages = {50, 20, 10, 5};
    for (int samplePercentage : samplePercentages) {
      final int[] numSegmentsPickedFromServer
          = pickSegmentsAndGetPickedCountPerServer(servers, samplePercentage, 50);

      final int totalSegmentsPicked = Arrays.stream(numSegmentsPickedFromServer).sum();

      // Number of segments picked from each server is ~25% of total
      final double expectedPickedSegments = totalSegmentsPicked * 0.25;
      final double error = totalSegmentsPicked * 0.02;
      for (int pickedSegments : numSegmentsPickedFromServer) {
        Assert.assertEquals(expectedPickedSegments, pickedSegments, error);
      }
    }
  }

  @Test
  public void testSegmentsFromMorePopulousServerAreMoreLikelyToBePicked()
  {
    // Create 4 servers, first one having twice as many segments as the rest
    final List<List<DataSegment>> subSegmentLists = Lists.partition(segments, segments.size() / 5);

    final List<ServerHolder> servers = new ArrayList<>();
    List<DataSegment> segmentsForServer0 = new ArrayList<>(subSegmentLists.get(0));
    segmentsForServer0.addAll(subSegmentLists.get(1));
    servers.add(createHistorical("server_" + 0, segmentsForServer0));

    IntStream.range(1, 4).mapToObj(
        i -> createHistorical("server_" + i, subSegmentLists.get(i + 1))
    ).forEach(servers::add);

    final int[] samplePercentages = {50, 20, 10, 5};
    for (int samplePercentage : samplePercentages) {
      final int[] numSegmentsPickedFromServer
          = pickSegmentsAndGetPickedCountPerServer(servers, samplePercentage, 50);

      final int totalSegmentsPicked = Arrays.stream(numSegmentsPickedFromServer).sum();

      // Number of segments picked from server0 are ~40% of total and
      // number of segments picked from other servers are each ~20% of total
      double error = totalSegmentsPicked * 0.02;
      Assert.assertEquals(totalSegmentsPicked * 0.40, numSegmentsPickedFromServer[0], error);

      for (int serverId = 1; serverId < servers.size(); ++serverId) {
        Assert.assertEquals(totalSegmentsPicked * 0.20, numSegmentsPickedFromServer[serverId], error);
      }
    }
  }

  @Test(timeout = 60_000)
  public void testNumberOfSamplingsRequiredToPickAllSegments()
  {
    // The number of sampling iterations required for each sample percentage
    // remains more or less fixed, even with a larger number of segments
    final int[] samplePercentages = {100, 50, 10, 5, 1};
    final int[] expectedIterations = {1, 20, 100, 200, 1000};

    final int[] totalObservedIterations = new int[5];

    // For every sample percentage, count the minimum number of required samplings
    for (int i = 0; i < 50; ++i) {
      for (int j = 0; j < samplePercentages.length; ++j) {
        totalObservedIterations[j] += countMinRunsToPickAllSegments(samplePercentages[j]);
      }
    }

    // Compute the avg value from the 50 observations for each sample percentage
    for (int j = 0; j < samplePercentages.length; ++j) {
      double avgObservedIterations = totalObservedIterations[j] / 50.0;
      Assert.assertTrue(avgObservedIterations <= expectedIterations[j]);
    }

  }

  /**
   * Returns the minimum number of iterations of the reservoir sampling required
   * to pick each segment atleast once.
   * <p>
   * {@code k = sampleSize = totalNumSegments * samplePercentage}
   */
  private int countMinRunsToPickAllSegments(int samplePercentage)
  {
    final int numSegments = segments.size();
    final List<ServerHolder> servers = Arrays.asList(
        createHistorical("server1", segments.subList(0, numSegments / 2).toArray(new DataSegment[0])),
        createHistorical("server2", segments.subList(numSegments / 2, numSegments).toArray(new DataSegment[0]))
    );

    final Set<DataSegment> pickedSegments = new HashSet<>();

    int sampleSize = (int) (numSegments * samplePercentage / 100.0);

    int numIterations = 1;
    for (; numIterations < 10000; ++numIterations) {
      ReservoirSegmentSampler
          .pickMovableSegmentsFrom(servers, sampleSize, ServerHolder::getServedSegments, Collections.emptySet())
          .forEach(holder -> pickedSegments.add(holder.getSegment()));

      if (pickedSegments.size() >= numSegments) {
        break;
      }
    }

    return numIterations;
  }

  private int[] pickSegmentsAndGetPickedCountPerServer(
      List<ServerHolder> servers,
      int samplePercentage,
      int numIterations
  )
  {
    final int numSegmentsToPick = (int) (segments.size() * samplePercentage / 100.0);
    final int[] numSegmentsPickedFromServer = new int[servers.size()];

    for (int i = 0; i < numIterations; ++i) {
      List<BalancerSegmentHolder> pickedSegments = ReservoirSegmentSampler.pickMovableSegmentsFrom(
          servers,
          numSegmentsToPick,
          ServerHolder::getServedSegments,
          Collections.emptySet()
      );

      // Get the number of segments picked from each server
      for (BalancerSegmentHolder pickedSegment : pickedSegments) {
        int serverIndex = servers.indexOf(pickedSegment.getServer());
        numSegmentsPickedFromServer[serverIndex]++;
      }
    }

    return numSegmentsPickedFromServer;
  }

  private ServerHolder createHistorical(String serverName, List<DataSegment> loadedSegments)
  {
    return createHistorical(serverName, loadedSegments.toArray(new DataSegment[0]));
  }

  private ServerHolder createHistorical(String serverName, DataSegment... loadedSegments)
  {
    final DruidServer server =
        new DruidServer(serverName, serverName, null, 100000, ServerType.HISTORICAL, "normal", 1);
    for (DataSegment segment : loadedSegments) {
      server.addDataSegment(segment);
    }
    return new ServerHolder(server.toImmutableDruidServer(), new TestLoadQueuePeon());
  }
}

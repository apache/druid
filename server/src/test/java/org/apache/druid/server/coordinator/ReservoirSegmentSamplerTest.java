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

package org.apache.druid.server.coordinator;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
              .getRandomBalancerSegmentHolders(servers, Collections.emptySet(), 1)
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
  public void testLoadingSegmentGetsPicked()
  {
    final List<DataSegment> loadedSegments = Arrays.asList(segments.get(0), segments.get(1));
    final List<DataSegment> loadingSegments = Arrays.asList(segments.get(2), segments.get(3));

    final ServerHolder server1 = createHistorical("server1", loadedSegments.get(0));
    server1.startOperation(SegmentAction.LOAD, loadingSegments.get(0));

    final ServerHolder server2 = createHistorical("server2", loadedSegments.get(1));
    server2.startOperation(SegmentAction.LOAD, loadingSegments.get(1));

    Set<DataSegment> pickedSegments = ReservoirSegmentSampler
        .getRandomBalancerSegmentHolders(Arrays.asList(server1, server2), Collections.emptySet(), 10)
        .stream().map(BalancerSegmentHolder::getSegment).collect(Collectors.toSet());

    // Verify that both loaded and loading segments are picked
    Assert.assertTrue(pickedSegments.containsAll(loadedSegments));
    Assert.assertTrue(pickedSegments.containsAll(loadingSegments));
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
        new LoadQueuePeonTester()
    );

    // Try to pick all the segments on the servers
    List<BalancerSegmentHolder> pickedSegments = ReservoirSegmentSampler.getRandomBalancerSegmentHolders(
        Arrays.asList(historical, broker),
        Collections.emptySet(),
        10
    );

    // Verify that only the segments on the historical are picked
    Assert.assertEquals(2, pickedSegments.size());
    for (BalancerSegmentHolder holder : pickedSegments) {
      Assert.assertEquals(historical, holder.getFromServer());
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
    List<BalancerSegmentHolder> pickedSegments = ReservoirSegmentSampler
        .getRandomBalancerSegmentHolders(servers, Collections.singleton(broadcastDatasource), 10);

    // Verify that none of the broadcast segments are picked
    Assert.assertEquals(2, pickedSegments.size());
    for (BalancerSegmentHolder holder : pickedSegments) {
      Assert.assertNotEquals(broadcastDatasource, holder.getSegment().getDataSource());
    }
  }

  @Test(timeout = 60_000)
  public void testNumberOfIterationsToCycleThroughAllSegments()
  {
    // The number of runs required for each sample percentage
    // remains more or less fixed, even with a larger number of segments
    final int[] samplePercentages = {100, 50, 10, 5, 1};
    final int[] expectedIterations = {1, 20, 100, 200, 1000};

    final int[] totalObservedIterations = new int[5];
    for (int i = 0; i < 50; ++i) {
      for (int j = 0; j < samplePercentages.length; ++j) {
        totalObservedIterations[j] += countMinRunsWithSamplePercent(samplePercentages[j]);
      }
    }

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
  private int countMinRunsWithSamplePercent(int samplePercentage)
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
          .getRandomBalancerSegmentHolders(servers, Collections.emptySet(), sampleSize)
          .forEach(holder -> pickedSegments.add(holder.getSegment()));

      if (pickedSegments.size() >= numSegments) {
        break;
      }
    }

    return numIterations;
  }

  private ServerHolder createHistorical(String serverName, DataSegment... loadedSegments)
  {
    final DruidServer server =
        new DruidServer(serverName, serverName, null, 100000, ServerType.HISTORICAL, "normal", 1);
    for (DataSegment segment : loadedSegments) {
      server.addDataSegment(segment);
    }
    return new ServerHolder(server.toImmutableDruidServer(), new LoadQueuePeonTester());
  }
}

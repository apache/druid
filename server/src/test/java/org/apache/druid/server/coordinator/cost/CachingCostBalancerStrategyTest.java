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

package org.apache.druid.server.coordinator.cost;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CachingCostBalancerStrategy;
import org.apache.druid.server.coordinator.CostBalancerStrategy;
import org.apache.druid.server.coordinator.LoadQueuePeonTester;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CachingCostBalancerStrategyTest
{
  private static final int DAYS_IN_MONTH = 30;
  private static final int SEGMENT_SIZE = 100;
  private static final int NUMBER_OF_SEGMENTS_ON_SERVER = 10000;
  private static final int NUMBER_OF_QUERIES = 1000;
  private static final int NUMBER_OF_SERVERS = 3;

  private List<ServerHolder> serverHolderList;
  private List<DataSegment> segmentQueries;
  private ListeningExecutorService executorService;

  @Before
  public void setUp()
  {
    Random random = new Random(0);
    DateTime referenceTime = DateTimes.of("2014-01-01T00:00:00");

    serverHolderList = IntStream
        .range(0, NUMBER_OF_SERVERS)
        .mapToObj(i ->
                      createServerHolder(
                          String.valueOf(i),
                          String.valueOf(i),
                          SEGMENT_SIZE * (NUMBER_OF_SEGMENTS_ON_SERVER + NUMBER_OF_QUERIES),
                          NUMBER_OF_SEGMENTS_ON_SERVER,
                          random,
                          referenceTime
                      )
        )
        .collect(Collectors.toList());

    segmentQueries = createDataSegments(NUMBER_OF_QUERIES, random, referenceTime);
    executorService = MoreExecutors.listeningDecorator(Execs.singleThreaded(""));
  }

  @After
  public void tearDown()
  {
    executorService.shutdownNow();
  }

  @Test
  public void decisionTest()
  {
    CachingCostBalancerStrategy cachingCostBalancerStrategy = createCachingCostBalancerStrategy(
        serverHolderList,
        executorService
    );
    CostBalancerStrategy costBalancerStrategy = createCostBalancerStrategy(executorService);
    int notEqual = segmentQueries
        .stream()
        .mapToInt(
            s -> {
              ServerHolder s1 = cachingCostBalancerStrategy.findNewSegmentHomeBalancer(s, serverHolderList);
              ServerHolder s2 = costBalancerStrategy.findNewSegmentHomeBalancer(s, serverHolderList);
              return (s1.getServer().getName().equals(s2.getServer().getName())) ? 0 : 1;
            }
        )
        .sum();
    Assert.assertTrue(((double) notEqual / (double) segmentQueries.size()) < 0.01);
  }

  private CachingCostBalancerStrategy createCachingCostBalancerStrategy(
      List<ServerHolder> serverHolders,
      ListeningExecutorService listeningExecutorService
  )
  {
    ClusterCostCache.Builder builder = ClusterCostCache.builder();
    serverHolders.forEach(
        s -> s.getServer().iterateAllSegments().forEach(segment -> builder.addSegment(s.getServer().getName(), segment))
    );
    return new CachingCostBalancerStrategy(builder.build(), listeningExecutorService);
  }

  private CostBalancerStrategy createCostBalancerStrategy(ListeningExecutorService listeningExecutorService)
  {
    return new CostBalancerStrategy(listeningExecutorService);
  }

  private ServerHolder createServerHolder(
      String name,
      String host,
      int maxSize,
      int numberOfSegments,
      Random random,
      DateTime referenceTime
  )
  {
    DruidServer druidServer = new DruidServer(name, host, null, maxSize, ServerType.HISTORICAL, "normal", 0);
    createDataSegments(numberOfSegments, random, referenceTime)
        .forEach(druidServer::addDataSegment);
    return new ServerHolder(
        druidServer.toImmutableDruidServer(),
        new LoadQueuePeonTester()
    );
  }

  private List<DataSegment> createDataSegments(
      int numberOfSegments,
      Random random,
      DateTime referenceTime
  )
  {
    return new ArrayList<>(
        IntStream
            .range(0, numberOfSegments)
            .mapToObj(i -> createRandomSegment(random, referenceTime))
            .collect(Collectors.toSet())
    );
  }

  private DataSegment createRandomSegment(Random random, DateTime referenceTime)
  {
    int timeShift = random.nextInt((int) TimeUnit.DAYS.toHours(DAYS_IN_MONTH * 12));
    return new DataSegment(
        String.valueOf(random.nextInt(50)),
        new Interval(referenceTime.plusHours(timeShift), referenceTime.plusHours(timeShift + 1)),
        "version",
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        null,
        0,
        100
    );
  }
}

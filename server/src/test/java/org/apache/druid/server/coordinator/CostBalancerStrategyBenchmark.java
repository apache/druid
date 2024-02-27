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

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategy;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

@Ignore
@RunWith(Parameterized.class)
public class CostBalancerStrategyBenchmark extends AbstractBenchmark
{
  @Parameterized.Parameters
  public static List<CostBalancerStrategy[]> factoryClasses()
  {
    return Arrays.asList(
        new CostBalancerStrategy[] {
            new CostBalancerStrategy(MoreExecutors.listeningDecorator(
                Execs.multiThreaded(1, "CostBalancerStrategyBenchmark-%d")))
        },
        new CostBalancerStrategy[] {
            new CostBalancerStrategy(MoreExecutors.listeningDecorator(
                Execs.multiThreaded(4, "CostBalancerStrategyBenchmark-%d")))
        }
    );
  }

  private final CostBalancerStrategy strategy;
  private final List<ServerHolder> serverHolderList;

  public CostBalancerStrategyBenchmark(CostBalancerStrategy costBalancerStrategy)
  {
    this.strategy = costBalancerStrategy;
    this.serverHolderList = initServers();
  }

  private List<ServerHolder> initServers()
  {
    final List<DruidServer> servers = new ArrayList<>();
    for (int i = 0; i < 6; ++i) {
      DruidServer druidServer = new DruidServer(
          "server_" + i,
          "localhost", null, 10_000_000L, ServerType.HISTORICAL, "hot", 1
      );
      servers.add(druidServer);
    }

    // Create and randomly distribute some segments amongst the servers
    final List<DataSegment> segments =
        CreateDataSegments.ofDatasource("wikipedia")
                          .forIntervals(200, Granularities.DAY)
                          .withNumPartitions(100)
                          .eachOfSizeInMb(200);
    final Random random = new Random(100);
    segments.forEach(
        segment -> servers.get(random.nextInt(servers.size()))
                          .addDataSegment(segment)
    );

    return servers.stream()
                  .map(DruidServer::toImmutableDruidServer)
                  .map(server -> new ServerHolder(server, null))
                  .collect(Collectors.toList());
  }

  volatile ServerHolder selected;

  @Test
  @BenchmarkOptions(warmupRounds = 10, benchmarkRounds = 1000)
  public void testBenchmark()
  {
    DataSegment segment = DataSegment.builder().dataSource("testds").version("1000")
                                     .interval(interval1).size(100L).build();
    Iterator<ServerHolder> candidates = strategy.findServersToLoadSegment(segment, serverHolderList);
    selected = candidates.hasNext() ? candidates.next() : null;
  }

  // Benchmark Joda Interval Gap impl vs CostBalancer.gapMillis
  private final Interval interval1 = Intervals.of("2015-01-01T01:00:00Z/2015-01-01T02:00:00Z");
  private final Interval interval2 = Intervals.of("2015-02-01T01:00:00Z/2015-02-01T02:00:00Z");
  volatile Long sum;

  @BenchmarkOptions(warmupRounds = 1000, benchmarkRounds = 1000000)
  @Test
  public void testJodaGap()
  {
    long diff = 0;
    for (int i = 0; i < 1000; i++) {
      diff = diff + interval1.gap(interval2).toDurationMillis();
    }
    sum = diff;
  }
}

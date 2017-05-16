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

package io.druid.server.coordinator;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

@Ignore
@RunWith(Parameterized.class)
public class CostBalancerStrategyBenchmark extends AbstractBenchmark
{
  @Parameterized.Parameters
  public static List<CostBalancerStrategy[]> factoryClasses()
  {
    return Arrays.asList(
        (CostBalancerStrategy[]) Arrays.asList(
            new CostBalancerStrategy(MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(1)))
        ).toArray(),
        (CostBalancerStrategy[]) Arrays.asList(
            new CostBalancerStrategy(MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(4)))
        ).toArray()
    );
  }

  private final CostBalancerStrategy strategy;

  public CostBalancerStrategyBenchmark(CostBalancerStrategy costBalancerStrategy)
  {
    this.strategy = costBalancerStrategy;
  }

  private static List<ServerHolder> serverHolderList;
  volatile ServerHolder selected;

  @BeforeClass
  public static void setup()
  {
    serverHolderList = CostBalancerStrategyTest.setupDummyCluster(5, 20000);
  }

  @AfterClass
  public static void tearDown()
  {
    serverHolderList = null;
  }

  @Test
  @BenchmarkOptions(warmupRounds = 10, benchmarkRounds = 1000)
  public void testBenchmark() throws InterruptedException
  {
    DataSegment segment = CostBalancerStrategyTest.getSegment(1000, "testds", interval1);
    selected = strategy.findNewSegmentHomeReplicator(segment, serverHolderList);
  }


  // Benchmark Joda Interval Gap impl vs CostBalancer.gapMillis
  private final Interval interval1 = new Interval("2015-01-01T01:00:00Z/2015-01-01T02:00:00Z");
  private final Interval interval2 = new Interval("2015-02-01T01:00:00Z/2015-02-01T02:00:00Z");
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

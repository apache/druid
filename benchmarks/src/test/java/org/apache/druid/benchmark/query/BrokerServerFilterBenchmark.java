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

package org.apache.druid.benchmark.query;

import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.client.BrokerViewOfCoordinatorConfig;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordination.TestCoordinatorClient;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@link BrokerViewOfCoordinatorConfig#getQueryableServers} under varying thread counts
 * to measure concurrent read throughput. This is the hot path called per-segment during
 * query planning (groupSegmentsByServer, computeResultLevelCachingEtag).
 *
 * With the old synchronized implementation, throughput at 32 threads would be roughly the same as
 * 1 thread due to monitor contention. With volatile, it should scale linearly.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "-XX:+UseG1GC")
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class BrokerServerFilterBenchmark
{
  private BrokerViewOfCoordinatorConfig config;
  private Int2ObjectRBTreeMap<Set<QueryableDruidServer>> servers;

  @Setup
  public void setup()
  {
    config = new BrokerViewOfCoordinatorConfig(new TestCoordinatorClient());
    config.start();

    servers = new Int2ObjectRBTreeMap<>();
    Set<QueryableDruidServer> serverSet = new HashSet<>();
    for (int i = 0; i < 450; i++) {
      String host = "historical-" + i;
      DruidServer druidServer = new DruidServer(host, host, null, 100, null, ServerType.HISTORICAL, "tier1", 0);
      serverSet.add(new QueryableDruidServer(druidServer, (QueryRunner) (queryPlus, responseContext) -> null));
    }
    servers.put(0, serverSet);
  }

  @Benchmark
  @Threads(1)
  public void getQueryableServers_1thread(Blackhole blackhole)
  {
    blackhole.consume(config.getQueryableServers(servers, CloneQueryMode.EXCLUDECLONES));
  }

  @Benchmark
  @Threads(8)
  public void getQueryableServers_8threads(Blackhole blackhole)
  {
    blackhole.consume(config.getQueryableServers(servers, CloneQueryMode.EXCLUDECLONES));
  }

  @Benchmark
  @Threads(32)
  public void getQueryableServers_32threads(Blackhole blackhole)
  {
    blackhole.consume(config.getQueryableServers(servers, CloneQueryMode.EXCLUDECLONES));
  }
}

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

package org.apache.druid.benchmark;

import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.router.RendezvousHasher;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 15)
@Measurement(iterations = 30)
public class RendezvousHasherBenchmark
{
  @Param({"100000"})
  int numIds;

  RendezvousHasher hasher;
  List<String> uuids;
  Set<String> servers;

  @Setup
  public void setup()
  {
    hasher = new RendezvousHasher();
    uuids = new ArrayList<>();
    servers = Sets.newHashSet(
        "localhost:1",
        "localhost:2",
        "localhost:3",
        "localhost:4",
        "localhost:5",
        "localhost:6",
        "localhost:7",
        "localhost:8",
        "localhost:9",
        "localhost:10"
    );


    for (int i = 0; i < numIds; i++) {
      UUID uuid = UUID.randomUUID();
      uuids.add(uuid.toString());
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void hash(Blackhole blackhole)
  {
    for (String uuid : uuids) {
      String server = hasher.chooseNode(servers, StringUtils.toUtf8(uuid));
      blackhole.consume(server);
    }
  }
}

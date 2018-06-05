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

package io.druid.benchmark;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.StringUtils;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 15)
@Measurement(iterations = 30)
public class LoadStatusBenchmark
{
  // Number of total data segments
  @Param({"10000"})
  int totalSegmentsCount;

  @Param({"true", "false"})
  private boolean serverHasAllSegments;

  private Set<DataSegment> datasourceSegments;
  private Collection<DataSegment> serverSegments;

  @Setup(Level.Invocation)
  public void setup()
  {
    Map<String, DataSegment> immutableDatasourceSegmentsMap;
    ConcurrentHashMap<String, DataSegment> serverSegmentsMap;

    HashMap<String, DataSegment> datasourceSegmentsMap = Maps.newHashMap();
    serverSegmentsMap = new ConcurrentHashMap<>();

    for (int i = 0; i < totalSegmentsCount; i++) {
      DataSegment segment = new DataSegment(
          "benchmarkDatasource",
          Intervals.of(StringUtils.format("%s-01-01/%s-12-31", i + 1970, i + 1970)),
          "1",
          null,
          null,
          null,
          NoneShardSpec.instance(),
          1,
          1
      );

      datasourceSegmentsMap.put(segment.getIdentifier(), segment);

      if (serverHasAllSegments || i % 2 == 0) {
        serverSegmentsMap.put(segment.getIdentifier(), segment);
      }
    }

    immutableDatasourceSegmentsMap = ImmutableMap.copyOf(datasourceSegmentsMap);

    datasourceSegments = Sets.newHashSet(immutableDatasourceSegmentsMap.values());
    serverSegments = Collections.unmodifiableCollection(serverSegmentsMap.values());
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void oldVersion(Blackhole blackhole)
  {
    datasourceSegments.removeAll(serverSegments);
    blackhole.consume(datasourceSegments);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void newVersion(Blackhole blackhole)
  {
    for (DataSegment segment : serverSegments) {
      datasourceSegments.remove(segment);
    }
    blackhole.consume(datasourceSegments);
  }
}

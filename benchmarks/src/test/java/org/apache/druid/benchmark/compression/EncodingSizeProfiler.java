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

package org.apache.druid.benchmark.compression;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import java.util.Collection;
import java.util.Collections;

/**
 * Crude jmh 'profiler' that allows calling benchmark methods to set this static value in a benchmark run, and if
 * this profiler to the run and have this additional measurement show up in the results.
 *
 * This allows 2 measurements to be collected for the result set, timing of the test, and size in bytes set here.
 *
 * @see ColumnarLongsSelectRowsFromGeneratorBenchmark#selectRows(Blackhole)
 * @see ColumnarLongsSelectRowsFromGeneratorBenchmark#selectRowsVectorized(Blackhole)
 * @see ColumnarLongsEncodeDataFromGeneratorBenchmark#encodeColumn(Blackhole)
 * @see ColumnarLongsSelectRowsFromSegmentBenchmark#selectRows(Blackhole)
 * @see ColumnarLongsSelectRowsFromSegmentBenchmark#selectRowsVectorized(Blackhole)
 * @see ColumnarLongsEncodeDataFromSegmentBenchmark#encodeColumn(Blackhole)
 */
public class EncodingSizeProfiler implements InternalProfiler
{
  public static int encodedSize;

  @Override
  public void beforeIteration(
      BenchmarkParams benchmarkParams,
      IterationParams iterationParams
  )
  {
  }

  @Override
  public Collection<? extends Result> afterIteration(
      BenchmarkParams benchmarkParams,
      IterationParams iterationParams,
      IterationResult result
  )
  {
    return Collections.singletonList(
        new ScalarResult("encoded size", encodedSize, "bytes", AggregationPolicy.MAX)
    );
  }

  @Override
  public String getDescription()
  {
    return "super janky encoding size result collector";
  }
}

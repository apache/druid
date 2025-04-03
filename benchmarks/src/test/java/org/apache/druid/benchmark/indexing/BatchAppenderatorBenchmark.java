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

package org.apache.druid.benchmark.indexing;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.segment.realtime.appenderator.BatchAppenderatorTester;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;

@Warmup(iterations = 1)
@Measurement(iterations = 3)
public class BatchAppenderatorBenchmark extends AppenderatorBenchmark
{
  @Setup
  @Override
  public void setup() throws IOException
  {
    super.setup();

    final BatchAppenderatorTester tester = new BatchAppenderatorTester(NUM_ROWS + 1, 100_000_000, tempDir, false);

    appenderator = tester.getAppenderator();
    appenderator.startJob();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void benchmarkAddRow(Blackhole blackhole) throws Exception
  {
    for (int i = 0; i < NUM_ROWS; ++i) {
      final InputRow row = createRow(timestamps[i], dimensionValues[i], metricValues[i]);

      final SegmentIdWithShardSpec identifier = identifiers.get(i % identifiers.size());
      // note: disk flushes are disabled for this test to avoid variance in access latencies
      blackhole.consume(appenderator.add(identifier, row, null, false));
    }
  }
}

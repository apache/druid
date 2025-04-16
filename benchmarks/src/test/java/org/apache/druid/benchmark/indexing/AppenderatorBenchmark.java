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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorTester;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgs = {
    "-Xms4G",
    "-Xmx4G",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
})
public abstract class AppenderatorBenchmark
{
  @Param({"10000"})
  protected int NUM_ROWS;

  protected static final int UNIQUE_DIMS = 500;
  protected static final int NUM_SEGMENTS = 5;
  protected static final String SEGMENT_INTERVAL = "2020-01-01/2020-02-01";

  protected Appenderator appenderator;
  protected List<SegmentIdWithShardSpec> identifiers;
  protected File tempDir;

  // Pre-generated values
  protected long[] timestamps;
  protected String[] dimensionValues;
  protected int[] metricValues;

  @Setup
  public void setup() throws IOException
  {
    tempDir = File.createTempFile("druid-appenderator-benchmark", "tmp");
    if (!tempDir.delete()) {
      throw new IOException("Could not delete appenderator benchmark temp dir");
    }
    if (!tempDir.mkdir()) {
      throw new IOException("Could not create appenderator benchmark temp dir");
    }

    // Pre-generate test data
    timestamps = new long[NUM_ROWS];
    dimensionValues = new String[NUM_ROWS];
    metricValues = new int[NUM_ROWS];

    final long startTimestamp = DateTimes.of("2020-01-01").getMillis();
    final long endTimestamp = DateTimes.of("2020-02-01").getMillis();
    final long timeRange = endTimestamp - startTimestamp - 1;

    final ThreadLocalRandom random = ThreadLocalRandom.current();
    for (int i = 0; i < NUM_ROWS; ++i) {
      timestamps[i] = startTimestamp + random.nextLong(timeRange);
      dimensionValues[i] = "dim_" + (i % UNIQUE_DIMS);
      metricValues[i] = i;
    }

    identifiers = new ArrayList<>();
    for (int i = 0; i < NUM_SEGMENTS; ++i) {
      identifiers.add(createSegmentId(SEGMENT_INTERVAL, "v1", i));
    }
  }


  protected static InputRow createRow(long timestamp, String dimValue, int metricValue)
  {
    return new MapBasedInputRow(
        timestamp,
        ImmutableList.of("dim"),
        ImmutableMap.of(
            "dim", dimValue,
            "met", metricValue
        )
    );
  }

  protected static SegmentIdWithShardSpec createSegmentId(String interval, String version, int partitionNum)
  {
    return new SegmentIdWithShardSpec(
        StreamAppenderatorTester.DATASOURCE,
        Intervals.of(interval),
        version,
        new LinearShardSpec(partitionNum)
    );
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    if (appenderator != null) {
      appenderator.close();
    }
    cleanupDirs(tempDir);
  }

  protected void cleanupDirs(File file)
  {
    if (file.isDirectory()) {
      final File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          cleanupDirs(child);
        }
      }
    }
    file.delete();
  }
}

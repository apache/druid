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

import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.key.KeyTestUtils;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollectorImpl;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.serde.ClusterByStatisticsSnapshotSerde;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks the controller merging the key statistics snapshots of every worker, as it does before it generates
 * partition boundaries for a range shuffle.
 *
 * Each worker collects statistics for its share of the keys, and its snapshot is serialized as it would be sent to the
 * controller. Each invocation merges freshly deserialized snapshots into an empty collector, so row keys have no cached
 * hash codes. Keys are wide, with several low-cardinality leading columns, like the keys of a GROUP BY on many columns.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class MsqSketchesMergeBenchmark extends InitializedNullHandlingTest
{
  private static final long WORKER_MAX_RETAINED_BYTES = 150_000_000;
  private static final long CONTROLLER_MAX_RETAINED_BYTES = 300_000_000;
  private static final int MAX_BUCKETS = 5_000;

  /**
   * Number of workers that send each distinct key, as when the rows of a group are spread across several segments.
   */
  private static final int WORKERS_PER_KEY = 4;

  private static final RowSignature SIGNATURE = RowSignature.builder()
                                                            .add("t", ColumnType.LONG)
                                                            .add("a", ColumnType.STRING)
                                                            .add("b", ColumnType.STRING)
                                                            .add("c", ColumnType.STRING)
                                                            .add("d", ColumnType.STRING)
                                                            .add("e", ColumnType.STRING)
                                                            .add("f", ColumnType.DOUBLE)
                                                            .add("g", ColumnType.STRING)
                                                            .build();

  private static final ClusterBy CLUSTER_BY = makeClusterBy();

  private static final RowSignature KEY_SIGNATURE =
      KeyTestUtils.createKeySignature(CLUSTER_BY.getColumns(), SIGNATURE);

  @Param({"10", "100"})
  private int numWorkers;

  @Param({"10000"})
  private int keysPerWorker;

  /**
   * True for the distinct-key collectors used by GROUP BY shuffles; false for the quantiles sketches used by others.
   */
  @Param({"true", "false"})
  private boolean aggregate;

  private List<byte[]> serializedSnapshots;
  private List<ClusterByStatisticsSnapshot> snapshots;

  @Setup(Level.Trial)
  public void setup() throws IOException
  {
    // Worker w gets the keys [w * stride, w * stride + keysPerWorker), wrapping around, so each key goes to
    // WORKERS_PER_KEY workers.
    final int stride = keysPerWorker / WORKERS_PER_KEY;
    final long numDistinctKeys = (long) numWorkers * stride;

    serializedSnapshots = new ArrayList<>();
    for (int worker = 0; worker < numWorkers; worker++) {
      final ClusterByStatisticsCollector collector = makeCollector(WORKER_MAX_RETAINED_BYTES);
      for (int i = 0; i < keysPerWorker; i++) {
        final long keyId = ((long) worker * stride + i) % numDistinctKeys;
        collector.add(KeyTestUtils.createKey(KEY_SIGNATURE, FrameType.latestRowBased(), makeKeyValues(keyId)), 1);
      }

      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      ClusterByStatisticsSnapshotSerde.serialize(out, collector.snapshot());
      serializedSnapshots.add(out.toByteArray());
    }
  }

  @Setup(Level.Invocation)
  public void deserializeSnapshots()
  {
    snapshots = new ArrayList<>();
    for (final byte[] serializedSnapshot : serializedSnapshots) {
      snapshots.add(ClusterByStatisticsSnapshotSerde.deserialize(ByteBuffer.wrap(serializedSnapshot)));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void mergeSnapshots(final Blackhole blackhole)
  {
    final ClusterByStatisticsCollector collector = makeCollector(CONTROLLER_MAX_RETAINED_BYTES);
    for (final ClusterByStatisticsSnapshot snapshot : snapshots) {
      collector.addAll(snapshot);
    }
    blackhole.consume(collector.estimatedTotalWeight());
  }

  private ClusterByStatisticsCollector makeCollector(final long maxRetainedBytes)
  {
    return ClusterByStatisticsCollectorImpl.create(
        CLUSTER_BY,
        SIGNATURE,
        FrameType.latestRowBased(),
        maxRetainedBytes,
        MAX_BUCKETS,
        aggregate,
        false
    );
  }

  private static ClusterBy makeClusterBy()
  {
    final List<KeyColumn> keyColumns = new ArrayList<>();
    for (final String column : SIGNATURE.getColumnNames()) {
      keyColumns.add(new KeyColumn(column, KeyOrder.ASCENDING));
    }
    return new ClusterBy(keyColumns, 0);
  }

  /**
   * Values for one distinct key. Derived from the key id but not ordered by it, so each worker's keys are spread
   * across the key space. The last column makes every key distinct.
   */
  private static Object[] makeKeyValues(final long keyId)
  {
    final SplittableRandom random = new SplittableRandom(keyId);
    return new Object[]{
        1_700_000_000_000L + 86_400_000L * random.nextInt(280),
        "value_a",
        "value_b_" + random.nextInt(4),
        "value_c_" + random.nextInt(3),
        "value_d_" + random.nextInt(3),
        "value_e_" + random.nextInt(30_000),
        random.nextInt(20) * 0.25,
        "value_g_" + keyId
    };
  }
}

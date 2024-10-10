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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.key.KeyTestUtils;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollectorImpl;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.serde.ClusterByStatisticsSnapshotSerde;
import org.apache.druid.segment.TestHelper;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class MsqSketchesBenchmark extends InitializedNullHandlingTest
{
  private static final int MAX_BYTES = 1_000_000_000;
  private static final int MAX_BUCKETS = 10_000;

  private static final RowSignature SIGNATURE = RowSignature.builder()
                                                            .add("x", ColumnType.LONG)
                                                            .add("y", ColumnType.LONG)
                                                            .add("z", ColumnType.STRING)
                                                            .build();

  private static final ClusterBy CLUSTER_BY_XYZ_BUCKET_BY_X = new ClusterBy(
      ImmutableList.of(
          new KeyColumn("x", KeyOrder.ASCENDING),
          new KeyColumn("y", KeyOrder.ASCENDING),
          new KeyColumn("z", KeyOrder.ASCENDING)
      ),
      1
  );

  @Param({"1", "1000"})
  private long numBuckets;

  @Param({"100000", "1000000"})
  private long numRows;

  @Param({"true", "false"})
  private boolean aggregate;

  private ObjectMapper jsonMapper;
  private ClusterByStatisticsSnapshot snapshot;

  @Setup(Level.Trial)
  public void setup()
  {
    jsonMapper = TestHelper.makeJsonMapper();
    final Iterable<RowKey> keys = () ->
        LongStream.range(0, numRows)
                  .mapToObj(n -> createKey(numBuckets, n))
                  .iterator();

    ClusterByStatisticsCollectorImpl collector = makeCollector(aggregate);
    keys.forEach(k -> collector.add(k, 1));
    snapshot = collector.snapshot();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchmarkJacksonSketch(Blackhole blackhole) throws IOException
  {
    final byte[] serializedSnapshot = jsonMapper.writeValueAsBytes(snapshot);

    final ClusterByStatisticsSnapshot deserializedSnapshot = jsonMapper.readValue(
        serializedSnapshot,
        ClusterByStatisticsSnapshot.class
    );
    blackhole.consume(deserializedSnapshot);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchmarkOctetSketch(Blackhole blackhole) throws IOException
  {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ClusterByStatisticsSnapshotSerde.serialize(byteArrayOutputStream, snapshot);
    final ByteBuffer serializedSnapshot = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    final ClusterByStatisticsSnapshot deserializedSnapshot = ClusterByStatisticsSnapshotSerde.deserialize(serializedSnapshot);
    blackhole.consume(deserializedSnapshot);
  }

  private ClusterByStatisticsCollectorImpl makeCollector(final boolean aggregate)
  {
    return (ClusterByStatisticsCollectorImpl) ClusterByStatisticsCollectorImpl.create(MsqSketchesBenchmark.CLUSTER_BY_XYZ_BUCKET_BY_X, SIGNATURE, MAX_BYTES, MAX_BUCKETS, aggregate, false);
  }

  private static RowKey createKey(final long numBuckets, final long keyNo)
  {
    final Object[] key = new Object[3];
    key[0] = keyNo % numBuckets;
    key[1] = keyNo % 5;
    key[2] = StringUtils.repeat("*", 67);
    return KeyTestUtils.createKey(KeyTestUtils.createKeySignature(MsqSketchesBenchmark.CLUSTER_BY_XYZ_BUCKET_BY_X.getColumns(), SIGNATURE), key);
  }
}

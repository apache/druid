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

import com.yahoo.sketches.hll.HllSketch;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchMergeAggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@Fork(1)
@State(Scope.Benchmark)
public class DataSketchesHllBenchmark
{
  private final AggregatorFactory aggregatorFactory = new HllSketchMergeAggregatorFactory(
      "hll",
      "hll",
      null,
      null,
      false
  );

  private final ByteBuffer buf = ByteBuffer.allocateDirect(aggregatorFactory.getMaxIntermediateSize());

  private BufferAggregator aggregator;

  @Setup(Level.Trial)
  public void setUp()
  {
    aggregator = aggregatorFactory.factorizeBuffered(
        new ColumnSelectorFactory()
        {
          @Override
          public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
          {
            return null;
          }

          @Override
          public ColumnValueSelector makeColumnValueSelector(String columnName)
          {
            return null;
          }

          @Nullable
          @Override
          public ColumnCapabilities getColumnCapabilities(String column)
          {
            return null;
          }
        }
    );
  }

  @TearDown(Level.Trial)
  public void tearDown()
  {
    aggregator.close();
    aggregator = null;
  }

  @Benchmark
  public void init(Blackhole bh)
  {
    aggregator.init(buf, 0);
  }

  @Benchmark
  public Object initAndGet()
  {
    aggregator.init(buf, 0);
    return aggregator.get(buf, 0);
  }

  @Benchmark
  public Object initAndSerde()
  {
    aggregator.init(buf, 0);
    return aggregatorFactory.deserialize(((HllSketch) aggregator.get(buf, 0)).toCompactByteArray());
  }
}

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.druid.benchmark.datagen.BenchmarkColumnSchema;
import io.druid.benchmark.datagen.BenchmarkSchemaInfo;
import io.druid.benchmark.datagen.SegmentGenerator;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.js.JavaScriptConfig;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.JavaScriptAggregatorFactory;
import io.druid.query.expression.TestExprMacroTable;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Cursor;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.ValueType;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.joda.time.Interval;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 15)
@Measurement(iterations = 30)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ExpressionBenchmark
{
  @Param({"1000000"})
  private int rowsPerSegment;

  private SegmentGenerator segmentGenerator;
  private QueryableIndex index;
  private JavaScriptAggregatorFactory javaScriptAggregatorFactory;
  private DoubleSumAggregatorFactory expressionAggregatorFactory;
  private ByteBuffer aggregationBuffer = ByteBuffer.allocate(Double.BYTES);

  @Setup(Level.Trial)
  public void setup() throws Exception
  {
    final BenchmarkSchemaInfo schemaInfo = new BenchmarkSchemaInfo(
        ImmutableList.of(
            BenchmarkColumnSchema.makeNormal("x", ValueType.FLOAT, false, 1, 0d, 0d, 10000d, false),
            BenchmarkColumnSchema.makeNormal("y", ValueType.FLOAT, false, 1, 0d, 0d, 10000d, false)
        ),
        ImmutableList.of(),
        new Interval("2000/P1D"),
        false
    );

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .build();

    this.segmentGenerator = new SegmentGenerator();
    this.index = segmentGenerator.generate(dataSegment, schemaInfo, rowsPerSegment);
    this.javaScriptAggregatorFactory = new JavaScriptAggregatorFactory(
        "name",
        ImmutableList.of("x", "y"),
        "function(current,x,y) { if (x > 0) { return current + x + 1 } else { return current + y + 1 } }",
        "function() { return 0 }",
        "function(a,b) { return a + b }",
        JavaScriptConfig.getEnabledInstance()
    );
    this.expressionAggregatorFactory = new DoubleSumAggregatorFactory(
        "name",
        null,
        "if(x>0,1.0+x,y+1)",
        TestExprMacroTable.INSTANCE
    );
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    if (index != null) {
      index.close();
      index = null;
    }

    if (segmentGenerator != null) {
      segmentGenerator.close();
      segmentGenerator = null;
    }
  }

  @Benchmark
  public void queryUsingJavaScript(Blackhole blackhole) throws Exception
  {
    final Double result = compute(javaScriptAggregatorFactory::factorizeBuffered);
    blackhole.consume(result);
  }

  @Benchmark
  public void queryUsingExpression(Blackhole blackhole) throws Exception
  {
    final Double result = compute(expressionAggregatorFactory::factorizeBuffered);
    blackhole.consume(result);
  }

  @Benchmark
  public void queryUsingNative(Blackhole blackhole) throws Exception
  {
    final Double result = compute(
        columnSelectorFactory ->
            new NativeBufferAggregator(
                columnSelectorFactory.makeFloatColumnSelector("x"),
                columnSelectorFactory.makeFloatColumnSelector("y")
            )
    );
    blackhole.consume(result);
  }

  private double compute(final Function<ColumnSelectorFactory, BufferAggregator> aggregatorFactory)
  {
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);

    final Sequence<Cursor> cursors = adapter.makeCursors(
        null,
        index.getDataInterval(),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    final List<Double> results = Sequences.toList(
        Sequences.map(
            cursors,
            cursor -> {
              final BufferAggregator bufferAggregator = aggregatorFactory.apply(cursor);
              bufferAggregator.init(aggregationBuffer, 0);

              while (!cursor.isDone()) {
                bufferAggregator.aggregate(aggregationBuffer, 0);
                cursor.advance();
              }

              final Double dbl = (Double) bufferAggregator.get(aggregationBuffer, 0);
              bufferAggregator.close();
              return dbl;
            }
        ),
        new ArrayList<>()
    );

    return Iterables.getOnlyElement(results);
  }

  private static class NativeBufferAggregator implements BufferAggregator
  {
    private final FloatColumnSelector xSelector;
    private final FloatColumnSelector ySelector;

    public NativeBufferAggregator(final FloatColumnSelector xSelector, final FloatColumnSelector ySelector)
    {
      this.xSelector = xSelector;
      this.ySelector = ySelector;
    }

    @Override
    public void init(final ByteBuffer buf, final int position)
    {
      buf.putDouble(0, 0d);
    }

    @Override
    public void aggregate(final ByteBuffer buf, final int position)
    {
      final float x = xSelector.get();
      final double n = x > 0 ? x + 1 : ySelector.get() + 1;
      buf.putDouble(0, buf.getDouble(position) + n);
    }

    @Override
    public Object get(final ByteBuffer buf, final int position)
    {
      return buf.getDouble(position);
    }

    @Override
    public float getFloat(final ByteBuffer buf, final int position)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(final ByteBuffer buf, final int position)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(ByteBuffer buf, int position)
    {
      throw new UnsupportedOperationException();
    }
    @Override
    public void close()
    {

    }
  }
}

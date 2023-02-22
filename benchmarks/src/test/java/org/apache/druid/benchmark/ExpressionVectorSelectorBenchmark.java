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

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.ColumnCache;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.segment.virtual.ExpressionVectorSelectorsTest;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
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

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;


@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ExpressionVectorSelectorBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  @Param({"1000000"})
  private int rowsPerSegment;

  @Param({"false", "true"})
  private boolean vectorize;

  @Param({
      "long1 * long2",
      "double1 * double3",
      "float1 + float3",
      "(long1 - long4) / double3",
      "max(double3, double5)",
      "min(double4, double1)",
      "cos(float3)",
      "sin(long4)",
      "parse_long(string1)",
      "parse_long(string1) * double3",
      "parse_long(string5) * parse_long(string1)",
      "parse_long(string5) * parse_long(string1) * double3"
  })
  private String expression;

  private QueryableIndex index;
  private Closer closer;

  @Nullable
  private ExpressionType outputType;

  @Setup(Level.Trial)
  public void setup()
  {
    this.closer = Closer.create();

    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("expression-testbench");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();

    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
    this.index = closer.register(
        segmentGenerator.generate(dataSegment, schemaInfo, Granularities.HOUR, rowsPerSegment)
    );

    Expr parsed = Parser.parse(expression, ExprMacroTable.nil());
    outputType = parsed.getOutputType(new ColumnCache(index, closer));
    checkSanity();
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void scan(Blackhole blackhole)
  {
    final VirtualColumns virtualColumns = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "v",
                expression,
                ExpressionType.toColumnType(outputType),
                TestExprMacroTable.INSTANCE
            )
        )
    );
    if (vectorize) {
      VectorCursor cursor = new QueryableIndexStorageAdapter(index).makeVectorCursor(
          null,
          index.getDataInterval(),
          virtualColumns,
          false,
          512,
          null
      );
      if (outputType.isNumeric()) {
        VectorValueSelector selector = cursor.getColumnSelectorFactory().makeValueSelector("v");
        if (outputType.is(ExprType.DOUBLE)) {
          while (!cursor.isDone()) {
            blackhole.consume(selector.getDoubleVector());
            blackhole.consume(selector.getNullVector());
            cursor.advance();
          }
        } else {
          while (!cursor.isDone()) {
            blackhole.consume(selector.getLongVector());
            blackhole.consume(selector.getNullVector());
            cursor.advance();
          }
        }
        closer.register(cursor);
      }
    } else {
      Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
          null,
          index.getDataInterval(),
          virtualColumns,
          Granularities.ALL,
          false,
          null
      );

      int rowCount = cursors
          .map(cursor -> {
            final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");
            int rows = 0;
            while (!cursor.isDone()) {
              blackhole.consume(selector.getObject());
              rows++;
              cursor.advance();
            }
            return rows;
          }).accumulate(0, (acc, in) -> acc + in);

      blackhole.consume(rowCount);
    }
  }

  private void checkSanity()
  {
    ExpressionVectorSelectorsTest.sanityTestVectorizedExpressionSelectors(expression, outputType, index, closer, rowsPerSegment);
  }
}

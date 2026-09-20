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

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.math.expr.SettableVectorInputBinding;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
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

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ExpressionVectorProcessorBenchmark
{
  @Param({"128", "512", "1024", "2048"})
  private int vectorSize;

  @Param({"false", "true"})
  private boolean nullable;

  @Param({"false", "true"})
  private boolean useVectorApi;

  @Param({
      "((((l1 + l2) * l3) - l2) + l1)",
      "(((l1 + d1) * 2.5) - l2)",
      "(((l1 + 7) * 3) - 11)"
  })
  private String expression;

  private SettableVectorInputBinding bindings;
  private ExprVectorProcessor<?> processor;

  @Setup(Level.Trial)
  public void setup()
  {
    if (useVectorApi) {
      ExpressionProcessing.initializeForVectorApiTests();
    } else {
      ExpressionProcessing.initializeForTests();
    }

    final long[] l1 = new long[vectorSize];
    final long[] l2 = new long[vectorSize];
    final long[] l3 = new long[vectorSize];
    final double[] d1 = new double[vectorSize];
    final boolean[] nulls = new boolean[vectorSize];
    for (int i = 0; i < vectorSize; i++) {
      l1[i] = i + 1L;
      l2[i] = i * 3L + 1L;
      l3[i] = i % 17L + 1L;
      d1[i] = i * 0.25 + 1.0;
      nulls[i] = nullable && i % 16 == 0;
    }

    final boolean[] inputNulls = nullable ? nulls : null;
    bindings = new SettableVectorInputBinding(vectorSize)
        .addLong("l1", l1, inputNulls)
        .addLong("l2", l2, inputNulls)
        .addLong("l3", l3, inputNulls)
        .addDouble("d1", d1, inputNulls);
    final Expr parsed = Parser.parse(expression, ExprMacroTable.nil());
    processor = parsed.asVectorProcessor(bindings);
  }

  @TearDown(Level.Trial)
  public void tearDown()
  {
    ExpressionProcessing.initializeForTests();
  }

  @Benchmark
  public void evaluate(Blackhole blackhole)
  {
    final ExprEvalVector<?> result = processor.evalVector(bindings);
    blackhole.consume(result.values());
    blackhole.consume(result.getNullVector());
  }
}

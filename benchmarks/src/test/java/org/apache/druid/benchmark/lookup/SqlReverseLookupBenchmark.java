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

package org.apache.druid.benchmark.lookup;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.benchmark.query.SqlBenchmark;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.rule.ReverseLookupRule;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.timeline.DataSegment;
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

/**
 * Benchmark for SQL planning involving {@link ReverseLookupRule}.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
public class SqlReverseLookupBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  /**
   * Type of lookup to benchmark. All are members of enum {@link LookupBenchmarkUtil.LookupType}.
   */
  @Param({"hashmap", "immutable"})
  private String lookupType;

  /**
   * Number of keys in the lookup table.
   */
  @Param({"5000000"})
  private int numKeys;

  /**
   * Average number of keys that map to each value.
   */
  @Param({"1000", "5000", "10000", "100000"})
  private int keysPerValue;

  private SqlEngine engine;
  private LookupExtractor lookup;

  @Nullable
  private PlannerFactory plannerFactory;
  private final Closer closer = Closer.create();

  @Setup(Level.Trial)
  public void setup()
  {
    final int numValues = Math.max(1, numKeys / keysPerValue);
    lookup = LookupBenchmarkUtil.makeLookupExtractor(
        LookupBenchmarkUtil.LookupType.valueOf(StringUtils.toUpperCase(lookupType)),
        numKeys,
        numValues
    );

    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");
    final DataSegment dataSegment = schemaInfo.makeSegmentDescriptor("foo");
    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());

    final QueryableIndex index =
        segmentGenerator.generate(dataSegment, schemaInfo, IndexSpec.DEFAULT, Granularities.NONE, 1);

    final Pair<PlannerFactory, SqlEngine> sqlSystem = SqlBenchmark.createSqlSystem(
        ImmutableMap.of(dataSegment, index),
        ImmutableMap.of("benchmark-lookup", lookup),
        null,
        closer
    );

    plannerFactory = sqlSystem.lhs;
    engine = sqlSystem.rhs;
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void planEquals(Blackhole blackhole)
  {
    final String sql = StringUtils.format(
        "SELECT COUNT(*) FROM foo WHERE LOOKUP(dimZipf, 'benchmark-lookup', 'N/A') = '%s'",
        LookupBenchmarkUtil.makeKeyOrValue(0)
    );
    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(engine, sql, ImmutableMap.of())) {
      final PlannerResult plannerResult = planner.plan();
      blackhole.consume(plannerResult);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void planNotEquals(Blackhole blackhole)
  {
    final String sql = StringUtils.format(
        "SELECT COUNT(*) FROM foo WHERE LOOKUP(dimZipf, 'benchmark-lookup', 'N/A') <> '%s'",
        LookupBenchmarkUtil.makeKeyOrValue(0)
    );
    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(engine, sql, ImmutableMap.of())) {
      final PlannerResult plannerResult = planner.plan();
      blackhole.consume(plannerResult);
    }
  }
}

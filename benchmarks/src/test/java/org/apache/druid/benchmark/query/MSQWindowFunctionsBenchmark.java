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

package org.apache.druid.benchmark.query;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.msq.test.ExtractResultsFactory;
import org.apache.druid.msq.test.MSQTestOverlordServiceClient;
import org.apache.druid.msq.test.StandardMSQComponentSupplier;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
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

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark that tests various SQL queries with window functions against MSQ engine.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 5)
@SqlTestFrameworkConfig.ComponentSupplier(MSQWindowFunctionsBenchmark.MSQComponentSupplier.class)
public class MSQWindowFunctionsBenchmark extends BaseCalciteQueryTest
{
  static {
    NullHandling.initializeForTests();
  }

  private static final Logger log = new Logger(MSQWindowFunctionsBenchmark.class);
  private final Closer closer = Closer.create();

  @Param({"20000000"})
  private int rowsPerSegment;

  @Param({"2", "5"})
  private int maxNumTasks;

  private List<Annotation> annotations;

  @Setup(Level.Trial)
  public void setup()
  {
    annotations = Arrays.asList(MSQWindowFunctionsBenchmark.class.getAnnotations());

    // Populate the QueryableIndex for the benchmark datasource.
    TestDataBuilder.makeQueryableIndexForBenchmarkDatasource(closer, rowsPerSegment);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Benchmark
  public void windowWithoutGroupBy(Blackhole blackhole)
  {
    String sql = "SELECT ROW_NUMBER() "
                 + "OVER (PARTITION BY dimUniform ORDER BY dimSequential) "
                 + "FROM benchmark_ds";
    querySql(sql, blackhole);
  }

  @Benchmark
  public void windowWithoutSorting(Blackhole blackhole)
  {
    String sql = "SELECT dimZipf, dimSequential,"
                 + "ROW_NUMBER() "
                 + "OVER (PARTITION BY dimZipf) "
                 + "from benchmark_ds\n"
                 + "group by dimZipf, dimSequential";
    querySql(sql, blackhole);
  }

  @Benchmark
  public void windowWithSorting(Blackhole blackhole)
  {
    String sql = "SELECT dimZipf, dimSequential,"
                 + "ROW_NUMBER() "
                 + "OVER (PARTITION BY dimZipf ORDER BY dimSequential) "
                 + "from benchmark_ds\n"
                 + "group by dimZipf, dimSequential";
    querySql(sql, blackhole);
  }

  @Benchmark
  public void windowWithHighCardinalityPartitionBy(Blackhole blackhole)
  {
    String sql = "select\n"
                 + "__time,\n"
                 + "row_number() over (partition by __time) as c1\n"
                 + "from benchmark_ds\n"
                 + "group by __time";
    querySql(sql, blackhole);
  }

  @Benchmark
  public void windowWithLowCardinalityPartitionBy(Blackhole blackhole)
  {
    String sql = "select\n"
                 + "dimZipf,\n"
                 + "row_number() over (partition by dimZipf) as c1\n"
                 + "from benchmark_ds\n"
                 + "group by dimZipf";
    querySql(sql, blackhole);
  }

  @Benchmark
  public void multipleWindows(Blackhole blackhole)
  {
    String sql = "select\n"
                 + "dimZipf, dimSequential, minFloatZipf,\n"
                 + "row_number() over (partition by dimSequential order by minFloatZipf) as c1,\n"
                 + "row_number() over (partition by dimZipf order by minFloatZipf) as c2,\n"
                 + "row_number() over (partition by minFloatZipf order by minFloatZipf) as c3,\n"
                 + "row_number() over (partition by dimSequential, dimZipf order by minFloatZipf, dimSequential) as c4,\n"
                 + "row_number() over (partition by minFloatZipf, dimZipf order by dimSequential) as c5,\n"
                 + "row_number() over (partition by minFloatZipf, dimSequential order by dimZipf) as c6,\n"
                 + "row_number() over (partition by dimSequential, minFloatZipf, dimZipf order by dimZipf, minFloatZipf) as c7,\n"
                 + "row_number() over (partition by dimSequential, minFloatZipf, dimZipf order by minFloatZipf) as c8\n"
                 + "from benchmark_ds\n"
                 + "group by dimZipf, dimSequential, minFloatZipf";
    querySql(sql, blackhole);
  }

  public void querySql(String sql, Blackhole blackhole)
  {
    final Map<String, Object> context = ImmutableMap.of(
        MultiStageQueryContext.CTX_MAX_NUM_TASKS, maxNumTasks
    );
    CalciteTestConfig calciteTestConfig = createCalciteTestConfig();
    QueryTestBuilder queryTestBuilder = new QueryTestBuilder(calciteTestConfig)
        .addCustomRunner(
            new ExtractResultsFactory(() -> (MSQTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient())
        );

    queryFrameworkRule.setConfig(new SqlTestFrameworkConfig(annotations));
    final List<Object[]> resultList = queryTestBuilder
        .skipVectorize(true)
        .queryContext(context)
        .sql(sql)
        .results()
        .results;

    if (!resultList.isEmpty()) {
      log.info("Total number of rows returned by query: %d", resultList.size());
      Object[] lastRow = resultList.get(resultList.size() - 1);
      blackhole.consume(lastRow);
    } else {
      log.info("No rows returned by the query.");
    }
  }

  protected static class MSQComponentSupplier extends StandardMSQComponentSupplier
  {
    public MSQComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
        QueryRunnerFactoryConglomerate conglomerate,
        JoinableFactoryWrapper joinableFactory,
        Injector injector
    )
    {
      final SpecificSegmentsQuerySegmentWalker retVal = super.createQuerySegmentWalker(
          conglomerate,
          joinableFactory,
          injector);
      TestDataBuilder.attachIndexesForBenchmarkDatasource(retVal);
      return retVal;
    }
  }
}

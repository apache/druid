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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.QueryContexts;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark that tests various SQL queries.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SqlWindowFunctionsBenchmark extends SqlBaseQueryBenchmark
{
  private static final List<String> QUERIES = ImmutableList.of(
      "SELECT SUM(dimSequentialHalfNull) FROM druid.basic GROUP BY dimUniform",
      "SELECT SUM(SUM(dimSequentialHalfNull)) OVER (ORDER BY dimUniform) FROM druid.basic GROUP BY dimUniform",
      "SELECT ROW_NUMBER() OVER (PARTITION BY dimUniform ORDER BY dimSequential) FROM druid.basic",
      "SELECT COUNT(*) OVER (PARTITION BY dimUniform RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM druid.basic",
      "SELECT COUNT(*) OVER (PARTITION BY dimUniform ORDER BY dimSequential RANGE UNBOUNDED PRECEDING) FROM druid.basic",
      "SELECT COUNT(*) OVER (PARTITION BY dimUniform ORDER BY dimSequential RANGE UNBOUNDED FOLLOWING) FROM druid.basic",
      "SELECT COUNT(*) OVER (PARTITION BY dimUniform ORDER BY dimSequential) FROM druid.basic GROUP BY dimSequential, dimUniform",
      "SELECT COUNT(*) OVER (PARTITION BY dimUniform ORDER BY dimSequential) FROM druid.basic GROUP BY dimUniform, dimSequential",
      "SELECT SUM(dimSequentialHalfNull) + SUM(dimZipf), LAG(SUM(dimSequentialHalfNull + dimZipf)) OVER (PARTITION BY dimUniform ORDER BY dimSequential) FROM druid.basic GROUP BY __time, dimUniform, dimSequential"
  );


  @Param({
      "0",
      "1",
      "2",
      "3",
      "4",
      "5",
      "6",
      "7",
      "8"
  })
  private String query;


  @Override
  public String getQuery()
  {
    return QUERIES.get(Integer.parseInt(query));
  }

  @Override
  public List<String> getDatasources()
  {
    return ImmutableList.of(SqlBenchmarkDatasets.BASIC);
  }

  @Override
  protected Map<String, Object> getContext()
  {
    return ImmutableMap.of(
        QueryContexts.MAX_SUBQUERY_BYTES_KEY, "disabled",
        QueryContexts.MAX_SUBQUERY_ROWS_KEY, -1,
        QueryContexts.VECTORIZE_KEY, vectorize,
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize
    );
  }
}

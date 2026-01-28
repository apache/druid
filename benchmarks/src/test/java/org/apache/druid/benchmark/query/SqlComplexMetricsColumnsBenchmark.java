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
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SqlComplexMetricsColumnsBenchmark extends SqlBaseQueryBenchmark
{
  private static final List<String> QUERIES = ImmutableList.of(
      "SELECT APPROX_COUNT_DISTINCT_DS_HLL(hll_string5) FROM druid.datasketches",
      "SELECT APPROX_COUNT_DISTINCT_DS_THETA(theta_string5) FROM druid.datasketches",
      "SELECT DS_GET_QUANTILE(DS_QUANTILES_SKETCH(quantiles_float4), 0.5) FROM druid.datasketches",
      "SELECT DS_GET_QUANTILE(DS_QUANTILES_SKETCH(quantiles_long3), 0.9) FROM druid.datasketches",
      "SELECT string2, APPROX_COUNT_DISTINCT_DS_HLL(hll_string5) FROM druid.datasketches GROUP BY 1 ORDER BY 2 DESC",
      "SELECT string2, APPROX_COUNT_DISTINCT_DS_THETA(theta_string5, 4096) FROM druid.datasketches GROUP BY 1 ORDER BY 2 DESC",
      "SELECT string2, DS_GET_QUANTILE(DS_QUANTILES_SKETCH(quantiles_float4), 0.5) FROM druid.datasketches GROUP BY 1 ORDER BY 2 DESC",
      "SELECT string2, DS_GET_QUANTILE(DS_QUANTILES_SKETCH(quantiles_long3), 0.9) FROM druid.datasketches GROUP BY 1 ORDER BY 2 DESC"
  );

  @Param({
      "0",
      "1",
      "2",
      "3",
      "4",
      "5",
      "6",
      "7"
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
    return ImmutableList.of(SqlBenchmarkDatasets.DATASKETCHES);
  }
}

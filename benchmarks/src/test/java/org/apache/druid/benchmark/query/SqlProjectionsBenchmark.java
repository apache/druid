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
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.Map;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SqlProjectionsBenchmark extends SqlBaseQueryBenchmark
{
  private static final List<String> QUERIES = ImmutableList.of(
      "SELECT string2, APPROX_COUNT_DISTINCT_DS_HLL(string5) FROM druid.projections GROUP BY 1 ORDER BY 2",
      "SELECT string2, SUM(long4) FROM druid.projections GROUP BY 1 ORDER BY 2"
  );

  @Param({
      "0",
      "1"
  })
  private String query;

  @Param({
      "true",
      "false"
  })
  private boolean useProjections;

  @Override
  public String getQuery()
  {
    return QUERIES.get(Integer.parseInt(query));
  }

  @Override
  public List<String> getDatasources()
  {
    return ImmutableList.of(SqlBenchmarkDatasets.PROJECTIONS);
  }

  @Override
  protected Map<String, Object> getContext()
  {
    final Map<String, Object> context = ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, vectorize,
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize,
        useProjections ? QueryContexts.FORCE_PROJECTION : QueryContexts.NO_PROJECTIONS, true
    );
    return context;
  }
}

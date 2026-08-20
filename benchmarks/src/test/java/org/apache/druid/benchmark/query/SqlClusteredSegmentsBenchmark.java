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
import org.apache.druid.java.util.common.StringUtils;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;

/**
 * Benchmarks queries against clustered base-table segments versus equivalent non-clustered segments, over the same
 * generated data. The {@link #segmentLayout} parameter selects one of three layouts:
 * <ul>
 *   <li>{@code CLUSTERED}: V10 clustered value-groups segment, ordered clustering columns &rarr; {@code __time} &rarr;
 *       non-clustering columns;</li>
 *   <li>{@code UNCLUSTERED}: V10 regular segment with the same clustering-first ordering but no clustered grouping
 *       (isolates the clustered grouping mechanism from the sort order);</li>
 *   <li>{@code TIME_ORDERED}: regular time-ordered segment ({@code __time} &rarr; clustering columns &rarr;
 *       non-clustering columns)</li>
 * </ul>
 * The clustering-column cardinality ({@link #clusteringCardinality}) is parameterized so the workload can be measured
 * as the number of value groups grows. {@link #numClusteringColumns} defaults to a single value (cluster only on
 * {@code clusterKey1}); set it to {@code "2"} to additionally cluster on the fixed low-cardinality {@code clusterKey2}.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SqlClusteredSegmentsBenchmark extends SqlBaseQueryBenchmark
{
  // %s is the datasource (table) name, filled in from the current parameters
  private static final List<String> QUERIES = ImmutableList.of(
      // 0: equality filter on the clustering column (clustered groups can prune non-matching groups)
      "SELECT SUM(valueLong) FROM %s WHERE clusterKey1 = '3'",
      // 1: group by the clustering column.
      "SELECT clusterKey1, SUM(valueLong) FROM %s GROUP BY 1 ORDER BY 2",
      // 2: filter on the clustering column + group by a secondary (higher cardinality) column
      "SELECT dimSecondary, SUM(valueLong) FROM %s WHERE clusterKey1 = '3' GROUP BY 1 ORDER BY 2",
      // 3: no-filter full aggregate (full scan, no pruning) as an overhead baseline
      "SELECT SUM(valueLong) FROM %s"
  );

  @Param({
      "CLUSTERED",
      "UNCLUSTERED",
      "TIME_ORDERED"
  })
  private String segmentLayout;

  @Param({
      "10",
      "40",
      "160"
  })
  private int clusteringCardinality;

  @Param({
      "1"
  })
  private int numClusteringColumns;

  @Param({
      "0",
      "1",
      "2",
      "3"
  })
  private int query;

  private String datasource()
  {
    return SqlBenchmarkDatasets.clusteringDatasource(
        segmentLayout,
        clusteringCardinality,
        numClusteringColumns
    );
  }

  @Override
  public String getQuery()
  {
    return StringUtils.format(QUERIES.get(query), datasource());
  }

  @Override
  public List<String> getDatasources()
  {
    return ImmutableList.of(datasource());
  }

  @Override
  protected void checkIncompatibleParameters()
  {
    // the clustered-vs-unclustered comparison is about persisted (mmap) V10 segment layout; skip the other
    // storage / schema / encoding / compression combinations the base benchmark sweeps so the matrix stays focused.
    if (storageType != BenchmarkStorage.MMAP
        || !"explicit".equals(schemaType)
        || stringEncoding != BenchmarkStringEncodingStrategy.UTF8
        || !"none".equals(complexCompression)) {
      System.exit(0);
    }
    super.checkIncompatibleParameters();
  }
}

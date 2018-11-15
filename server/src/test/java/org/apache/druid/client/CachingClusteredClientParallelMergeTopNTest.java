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
package org.apache.druid.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.VirtualColumns;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class CachingClusteredClientParallelMergeTopNTest extends CachingClusteredClientParallelMergeTestBase
{
  @Override
  void addServers(TestTimelineServerView serverView, int numServers, Random random)
  {
    for (int i = 0; i < numServers; i++) {
      final List<Result<TopNResultValue>> totalRows = new ArrayList<>();

      for (int day = 1; day < 6; day++) {
        final int numRows = random.nextInt(10000) + 10000;
        final List<Map<String, Object>> rows = new ArrayList<>(numRows);
        for (int j = 0; j < numRows; j++) {
          final Map<String, Object> row = createRow(
              StringUtils.format("dim2_%d", j),
              random.nextInt(100),
              random.nextDouble()
          );
          rows.add(row);
        }
        rows.sort(Comparator.comparing(row -> (String) row.get("dim2")));
        totalRows.add(new Result<>(DateTimes.of("2018-01-0%d", day), new TopNResultValue(rows)));
      }

      serverView.addServer(createServer(i + 1), new TestQueryRunner<>(totalRows));
    }
  }

  private static Map<String, Object> createRow(String dim2, long cnt, double doubleMet)
  {
    return ImmutableMap.of("dim2", dim2, "cnt", cnt, "double_max", doubleMet);
  }

  @Test
  public void test()
  {
    final TopNQuery expectedQuery = new TopNQuery(
        new TableDataSource(DATA_SOURCE),
        VirtualColumns.EMPTY,
        new DefaultDimensionSpec("dim2", "dim2"),
        new NumericTopNMetricSpec("double_max"),
        20000, // should be larger than cardinalrity of dim2
        new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2018-01-01/2018-01-07"))),
        null,
        Granularities.DAY,
        ImmutableList.of(
            new LongSumAggregatorFactory("cnt", "cnt"),
            new DoubleMaxAggregatorFactory("double_max", "double_met")
        ),
        null,
        null
    );

    runAndVerify(expectedQuery);
  }
}

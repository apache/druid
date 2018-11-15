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
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.VirtualColumns;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class CachingClusteredClientParallelMergeGroupByTest extends CachingClusteredClientParallelMergeTestBase
{
  private static final int DIM1_CARD = 11;
  private static final int DIM2_CARD = 3;

  @Override
  void addServers(TestTimelineServerView serverView, int numServers, Random random)
  {
    for (int i = 0; i < numServers; i++) {
      final int numRows = random.nextInt(10000) + 10000;
      final List<Row> rows = new ArrayList<>(numRows);
      for (int j = 0; j < numRows; j++) {
        final Row row = createRow(
            "2018-01-01",
            StringUtils.format("dim1_%d", random.nextInt(DIM1_CARD)),
            StringUtils.format("dim2_%d", random.nextInt(DIM2_CARD)),
            random.nextInt(100),
            random.nextDouble()
        );
        rows.add(row);
      }
      rows.sort(
          (r1, r2) -> {
            final int cmp = r1.getDimension("dim1").get(0).compareTo(r2.getDimension("dim1").get(0));
            if (cmp == 0) {
              return r1.getDimension("dim2").get(0).compareTo(r2.getDimension("dim2").get(0));
            } else {
              return cmp;
            }
          }
      );
      serverView.addServer(createServer(i + 1), new TestQueryRunner<>(rows));
    }
  }

  private static Row createRow(String timestamp, String dim1, String dim2, long cnt, double doubleMet)
  {
    return new MapBasedRow(
        DateTimes.of(timestamp),
        ImmutableMap.of("dim1", dim1, "dim2", dim2, "cnt", cnt, "double_max", doubleMet)
    );
  }

  @Test
  public void test()
  {
    final GroupByQuery expectedQuery = new GroupByQuery(
        new TableDataSource(DATA_SOURCE),
        new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2018-01-01/2018-01-07"))),
        VirtualColumns.EMPTY,
        null,
        Granularities.ALL,
        ImmutableList.of(new DefaultDimensionSpec("dim1", "dim1"), new DefaultDimensionSpec("dim2", "dim2")),
        ImmutableList.of(
            new LongSumAggregatorFactory("cnt", "cnt"),
            new DoubleMaxAggregatorFactory("double_max", "double_met")
        ),
        null,
        null,
        null,
        null,
        null
    );

    runAndVerify(expectedQuery);
  }
}

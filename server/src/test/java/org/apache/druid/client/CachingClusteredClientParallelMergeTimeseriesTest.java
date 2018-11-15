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
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.VirtualColumns;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class CachingClusteredClientParallelMergeTimeseriesTest extends CachingClusteredClientParallelMergeTestBase
{
  @Override
  void addServers(TestTimelineServerView serverView, int numServers, Random random)
  {
    for (int i = 0; i < numServers; i++) {
      final List<Result<TimeseriesResultValue>> totalRows = new ArrayList<>();

      for (int day = 1; day < 6; day++) {
        for (int hour = 1; hour < 24; hour++) {
          for (int minute = 1; minute < 60; minute++) {
            final Result<TimeseriesResultValue> row = createRow(
                DateTimes.of("2018-01-%02dT%02d:%02d:00", day, hour, minute),
                random.nextInt(100),
                random.nextDouble()
            );
            totalRows.add(row);
          }
        }
      }

      serverView.addServer(createServer(i + 1), new TestQueryRunner<>(totalRows));
    }
  }

  private static Result<TimeseriesResultValue> createRow(DateTime timestamp, long cnt, double doubleMet)
  {
    return new Result<>(
        timestamp,
        new TimeseriesResultValue(ImmutableMap.of("cnt", cnt, "double_max", doubleMet))
    );
  }

  @Test
  public void test()
  {
    final TimeseriesQuery expectedQuery = new TimeseriesQuery(
        new TableDataSource(DATA_SOURCE),
        new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2018-01-01/2018-01-07"))),
        false,
        VirtualColumns.EMPTY,
        null,
        Granularities.MINUTE,
        ImmutableList.of(
            new LongSumAggregatorFactory("cnt", "cnt"),
            new DoubleMaxAggregatorFactory("double_max", "double_met")
        ),
        null,
        0,
        null
    );

    runAndVerify(expectedQuery);
  }
}

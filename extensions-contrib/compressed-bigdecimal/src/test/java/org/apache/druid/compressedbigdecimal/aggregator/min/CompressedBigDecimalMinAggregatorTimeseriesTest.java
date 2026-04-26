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

package org.apache.druid.compressedbigdecimal.aggregator.min;

import org.apache.druid.compressedbigdecimal.aggregator.CompressedBigDecimalAggregatorTimeseriesTestBase;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.timeseries.TimeseriesQuery;

import java.util.List;

public class CompressedBigDecimalMinAggregatorTimeseriesTest extends CompressedBigDecimalAggregatorTimeseriesTestBase
{
  private static final TimeseriesQuery QUERY = Druids.newTimeseriesQueryBuilder()
      .dataSource("test_datasource")
      .granularity(Granularities.ALL)
      .aggregators(new CompressedBigDecimalMinAggregatorFactory("cbdStringRevenue", "revenue", 3, 9, null))
      .filters(new NotDimFilter(new SelectorDimFilter("property", "XXX", null)))
      .intervals("2017-01-01T00:00:00.000Z/P1D")
      .build();

  @Override
  public void testIngestAndTimeseriesQuery() throws Exception
  {
    testIngestAndTimeseriesQueryHelper(
        List.of(new CompressedBigDecimalMinAggregatorFactory("bigDecimalRevenue", "revenue", 3, 9, null)),
        QUERY,
        "-1.000000000"
    );
  }

  @Override
  public void testIngestMultipleSegmentsAndTimeseriesQuery() throws Exception
  {
    testIngestMultipleSegmentsAndTimeseriesQueryHelper(
        List.of(new CompressedBigDecimalMinAggregatorFactory("bigDecimalRevenue", "revenue", 3, 9, null)),
        QUERY,
        "-1.000000000"
    );
  }
}

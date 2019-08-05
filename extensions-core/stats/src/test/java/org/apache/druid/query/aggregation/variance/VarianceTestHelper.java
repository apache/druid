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

package org.apache.druid.query.aggregation.variance;

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.stats.DruidStatsModule;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class VarianceTestHelper extends QueryRunnerTestHelper
{
  static {
    DruidStatsModule module = new DruidStatsModule();
    module.configure(null);
  }

  public static final String INDEX_VARIANCE_METRIC = "index_var";

  public static final VarianceAggregatorFactory INDEX_VARIANCE_AGGR = new VarianceAggregatorFactory(
      INDEX_VARIANCE_METRIC,
      INDEX_METRIC
  );

  public static final String STD_DEV_OF_INDEX_METRIC = "index_stddev";

  public static final PostAggregator STD_DEV_OF_INDEX_POST_AGGR = new StandardDeviationPostAggregator(
      STD_DEV_OF_INDEX_METRIC,
      INDEX_VARIANCE_METRIC,
      null
  );

  public static final List<AggregatorFactory> COMMON_PLUS_VAR_AGGREGATORS = Arrays.asList(
      ROWS_COUNT,
      INDEX_DOUBLE_SUM,
      QUALITY_UNIQUES,
      INDEX_VARIANCE_AGGR
  );

  public static class RowBuilder
  {
    private final String[] names;
    private final List<Row> rows = new ArrayList<>();

    public RowBuilder(String[] names)
    {
      this.names = names;
    }

    public RowBuilder add(final String timestamp, Object... values)
    {
      rows.add(build(timestamp, values));
      return this;
    }

    public List<ResultRow> build(final GroupByQuery query)
    {
      try {
        return rows.stream().map(row -> ResultRow.fromLegacyRow(row, query)).collect(Collectors.toList());
      }
      finally {
        rows.clear();
      }
    }

    private Row build(final String timestamp, Object... values)
    {
      Preconditions.checkArgument(names.length == values.length);

      Map<String, Object> theVals = new HashMap<>();
      for (int i = 0; i < values.length; i++) {
        theVals.put(names[i], values[i]);
      }
      DateTime ts = DateTimes.of(timestamp);
      return new MapBasedRow(ts, theVals);
    }
  }
}

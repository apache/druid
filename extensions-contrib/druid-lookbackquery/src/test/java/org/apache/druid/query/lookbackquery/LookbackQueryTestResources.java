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

package org.apache.druid.query.lookbackquery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.joda.time.Period;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Class containing resources for testing lookback query
 */
public class LookbackQueryTestResources
{
  public static final QuerySegmentSpec oneDayQuerySegmentSpec = new MultipleIntervalSegmentSpec(
      Collections.singletonList(Intervals.of("2015-10-10T00:00:00.000Z/2015-10-11T00:00:00.000Z"))
  );
  public static final Period period = Period.days(-1);
  public static final Granularity dayGran = Granularities.DAY;
  public static final TableDataSource tableDataSource = new TableDataSource("slice1");
  public static final CountAggregatorFactory pageViews = new CountAggregatorFactory("pageViews");
  public static final LongSumAggregatorFactory timeSpent = new LongSumAggregatorFactory("timeSpent", "timeSpent");
  public static final ArithmeticPostAggregator pageViewsPerTimeSpent = new ArithmeticPostAggregator(
      "pageViewsPerTimeSpent",
      "/",
      Arrays.asList(
          new FieldAccessPostAggregator("pageViews", "pageViews"),
          new FieldAccessPostAggregator("timeSpent", "timeSpent")
      )
  );
  public static final ArithmeticPostAggregator totalPageViews = new ArithmeticPostAggregator(
      "totalPageViews",
      "+",
      Lists.<PostAggregator>newArrayList(
          new FieldAccessPostAggregator("pageViews", "pageViews"),
          new FieldAccessPostAggregator("lookback_P-1D_pageViews", "lookback_P-1D_pageViews")
      )
  );

  public static final ArithmeticPostAggregator totalTimeSpent = new ArithmeticPostAggregator(
      "totalTimeSpent",
      "+",
      Lists.<PostAggregator>newArrayList(
          new FieldAccessPostAggregator("timeSpent", "timeSpent"),
          new FieldAccessPostAggregator("lookback_P-1D_timeSpent", "lookback_P-1D_timeSpent")
      )
  );

  public static final DimensionSpec dim1 = new DefaultDimensionSpec("gender", "gender");

  public static final DimensionSpec dim2 = new DefaultDimensionSpec("country", "country");

  public static final List<PostAggregator> singleLookbackPostAggList =
      Lists.<PostAggregator>newArrayList(
          totalPageViews
      );


  public static final Query<Result<TimeseriesResultValue>> timeSeriesQuery = new TimeseriesQuery(
      tableDataSource,
      oneDayQuerySegmentSpec,
      false,
      null,
      null,
      Granularities.ALL,
      Arrays.asList(pageViews, timeSpent),
      Collections.singletonList(pageViewsPerTimeSpent),
      0,
      ImmutableMap.<String, Object>of("queryId", "2")
  );

  public static final Query<Result<TimeseriesResultValue>> timeSeriesQueryWithUnionDataSource = new TimeseriesQuery(
      new UnionDataSource(Arrays.asList(tableDataSource)),
      oneDayQuerySegmentSpec,
      false,
      null,
      null,
      Granularities.ALL,
      Arrays.asList(pageViews, timeSpent),
      Collections.singletonList(pageViewsPerTimeSpent),
      0,
      ImmutableMap.<String, Object>of("queryId", "2")
  );

  public static final Query<Row> groupByQuery = new GroupByQuery(
      tableDataSource,
      oneDayQuerySegmentSpec,
      null,
      null,
      Granularities.DAY,
      Arrays.asList(dim1, dim2),
      Arrays.asList(pageViews, timeSpent),
      Arrays.asList(pageViewsPerTimeSpent),
      null,
      null,
      null,
      null
  );

  public static final QueryDataSource timeSeriesQueryDataSource = new QueryDataSource(timeSeriesQuery);
  public static final QueryDataSource timeSeriesQueryUnionDataSource = new QueryDataSource(
      timeSeriesQueryWithUnionDataSource);
  public static final QueryDataSource groupByQueryDataSource = new QueryDataSource(groupByQuery);

  public static final QuerySegmentSpec twoDaysQuerySegmentSpec = new MultipleIntervalSegmentSpec(
      Collections.singletonList(Intervals.of("2015-10-10T00:00:00.000Z/2015-10-12T00:00:00.000Z"))
  );
}

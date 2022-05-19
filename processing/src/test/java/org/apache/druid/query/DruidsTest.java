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

package org.apache.druid.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids.SearchQueryBuilder;
import org.apache.druid.query.Druids.TimeBoundaryQueryBuilder;
import org.apache.druid.query.Druids.TimeseriesQueryBuilder;
import org.apache.druid.query.search.SearchQuery;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

@RunWith(Enclosed.class)
public class DruidsTest
{
  private static final String DATASOURCE = "datasource";
  private static final QuerySegmentSpec QUERY_SEGMENT_SPEC = new MultipleSpecificSegmentSpec(
      ImmutableList.of(
          new SegmentDescriptor(Intervals.of("2000/3000"), "0", 0),
          new SegmentDescriptor(Intervals.of("2000/3000"), "0", 1)
      )
  );

  public static class TimeseriesQueryBuilderTest
  {
    private TimeseriesQueryBuilder builder;

    @Before
    public void setup()
    {
      builder = Druids.newTimeseriesQueryBuilder()
                      .dataSource(DATASOURCE)
                      .intervals(QUERY_SEGMENT_SPEC)
                      .granularity(Granularities.ALL);
    }

    @Test
    public void testQueryIdWhenContextInBuilderIsNullReturnContextContainingQueryId()
    {
      final TimeseriesQuery query = builder
          .queryId("queryId")
          .build();
      Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "queryId"), query.getContext());
    }

    @Test
    public void testQueryIdWhenBuilderHasNonnullContextWithoutQueryIdReturnMergedContext()
    {
      final TimeseriesQuery query = builder
          .context(ImmutableMap.of("my", "context"))
          .queryId("queryId")
          .build();
      Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "queryId", "my", "context"), query.getContext());
    }

    @Test
    public void testQueryIdWhenBuilderHasNonnullContextWithQueryIdReturnMergedContext()
    {
      final TimeseriesQuery query = builder
          .context(ImmutableMap.of("my", "context", BaseQuery.QUERY_ID, "queryId"))
          .queryId("realQueryId")
          .build();
      Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "realQueryId", "my", "context"), query.getContext());
    }

    @Test
    public void testContextAfterSettingQueryIdReturnContextWithoutQueryId()
    {
      final TimeseriesQuery query = builder
          .queryId("queryId")
          .context(ImmutableMap.of("my", "context"))
          .build();
      Assert.assertEquals(ImmutableMap.of("my", "context"), query.getContext());
    }

    @Test
    public void testContextContainingQueryIdAfterSettingQueryIdOverwriteQueryId()
    {
      final TimeseriesQuery query = builder
          .queryId("queryId")
          .context(ImmutableMap.of("my", "context", BaseQuery.QUERY_ID, "realQueryId"))
          .build();
      Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "realQueryId", "my", "context"), query.getContext());
    }
  }

  public static class SearchQueryBuilderTest
  {
    private SearchQueryBuilder builder;

    @Before
    public void setup()
    {
      builder = Druids.newSearchQueryBuilder()
                      .dataSource(DATASOURCE)
                      .intervals(QUERY_SEGMENT_SPEC)
                      .granularity(Granularities.ALL);
    }

    @Test
    public void testQueryIdWhenContextInBuilderIsNullReturnContextContainingQueryId()
    {
      final SearchQuery query = builder
          .queryId("queryId")
          .build();
      Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "queryId"), query.getContext());
    }

    @Test
    public void testQueryIdWhenBuilderHasNonnullContextWithoutQueryIdReturnMergedContext()
    {
      final SearchQuery query = builder
          .context(ImmutableMap.of("my", "context"))
          .queryId("queryId")
          .build();
      Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "queryId", "my", "context"), query.getContext());
    }

    @Test
    public void testQueryIdWhenBuilderHasNonnullContextWithQueryIdReturnMergedContext()
    {
      final SearchQuery query = builder
          .context(ImmutableMap.of("my", "context", BaseQuery.QUERY_ID, "queryId"))
          .queryId("realQueryId")
          .build();
      Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "realQueryId", "my", "context"), query.getContext());
    }

    @Test
    public void testContextAfterSettingQueryIdReturnContextWithoutQueryId()
    {
      final SearchQuery query = builder
          .queryId("queryId")
          .context(ImmutableMap.of("my", "context"))
          .build();
      Assert.assertEquals(ImmutableMap.of("my", "context"), query.getContext());
    }

    @Test
    public void testContextContainingQueryIdAfterSettingQueryIdOverwriteQueryId()
    {
      final SearchQuery query = builder
          .queryId("queryId")
          .context(ImmutableMap.of("my", "context", BaseQuery.QUERY_ID, "realQueryId"))
          .build();
      Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "realQueryId", "my", "context"), query.getContext());
    }
  }

  public static class TimeBoundaryBuilderTest
  {
    private TimeBoundaryQueryBuilder builder;

    @Before
    public void setup()
    {
      builder = Druids.newTimeBoundaryQueryBuilder()
                      .dataSource(DATASOURCE)
                      .intervals(QUERY_SEGMENT_SPEC);
    }

    @Test
    public void testQueryIdWhenContextInBuilderIsNullReturnContextContainingQueryId()
    {
      final TimeBoundaryQuery query = builder
          .queryId("queryId")
          .build();
      Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "queryId"), query.getContext());
    }

    @Test
    public void testQueryIdWhenBuilderHasNonnullContextWithoutQueryIdReturnMergedContext()
    {
      final TimeBoundaryQuery query = builder
          .context(ImmutableMap.of("my", "context"))
          .queryId("queryId")
          .build();
      Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "queryId", "my", "context"), query.getContext());
    }

    @Test
    public void testQueryIdWhenBuilderHasNonnullContextWithQueryIdReturnMergedContext()
    {
      final TimeBoundaryQuery query = builder
          .context(ImmutableMap.of("my", "context", BaseQuery.QUERY_ID, "queryId"))
          .queryId("realQueryId")
          .build();
      Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "realQueryId", "my", "context"), query.getContext());
    }

    @Test
    public void testContextAfterSettingQueryIdReturnContextWithoutQueryId()
    {
      final TimeBoundaryQuery query = builder
          .queryId("queryId")
          .context(ImmutableMap.of("my", "context"))
          .build();
      Assert.assertEquals(ImmutableMap.of("my", "context"), query.getContext());
    }

    @Test
    public void testContextContainingQueryIdAfterSettingQueryIdOverwriteQueryId()
    {
      final TimeBoundaryQuery query = builder
          .queryId("queryId")
          .context(ImmutableMap.of("my", "context", BaseQuery.QUERY_ID, "realQueryId"))
          .build();
      Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "realQueryId", "my", "context"), query.getContext());
    }
  }
}

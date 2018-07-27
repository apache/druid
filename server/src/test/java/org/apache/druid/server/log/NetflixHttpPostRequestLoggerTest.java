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

package org.apache.druid.server.log;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.RangeSet;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.LegacyDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NetflixHttpPostRequestLoggerTest
{
  private static final String QUERY_ID = "fakequeryid";
  private static final String REMOTE_ADDRESS = "some.host";
  private static final String DATASOURCE = "fakeds";
  private static final String QUERY_TYPE = "faketype";
  private static final boolean DESCENDING = true;
  private static final boolean HAS_FILTERS = false;
  private static final String ERROR_STACK_TRACE = Throwables.getStackTraceAsString(new Exception());
  private static final Long QUERY_TIME = 100L;
  private static final Long QUERY_BYTES = 200L;
  private static final Map<String, Object> QUERY_CONTEXT = ImmutableMap.of(
      "foo",
      "bar",
      BaseQuery.QUERY_ID,
      QUERY_ID
  );

  @Test
  public void testSuccessfulQueryPayload()
  {
    final DateTime timestamp = DateTimes.of("2016-01-01T00:00:00Z");
    final Query successfulQuery = new FakeQuery(
        new LegacyDataSource(DATASOURCE),
        new QuerySegmentSpec()
        {
          @Override
          public List<Interval> getIntervals()
          {
            return Collections.singletonList(Intervals.of("2016-01-01T00Z/2016-01-02T00Z"));
          }

          @Override
          public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
          {
            return null;
          }
        }, DESCENDING, QUERY_CONTEXT
    );
    final QueryStats successfulQueryStats = new QueryStats(ImmutableMap.of(
        NetflixHttpPostRequestLogger.QueryStatsKey.QUERY_TIME.toString(),
        QUERY_TIME,
        NetflixHttpPostRequestLogger.QueryStatsKey.QUERY_BYTES.toString(),
        QUERY_BYTES,
        NetflixHttpPostRequestLogger.QueryStatsKey.SUCCESS.toString(),
        true
    ));
    final RequestLogLine logLine = RequestLogLine.forNative(
        successfulQuery,
        timestamp,
        REMOTE_ADDRESS,
        successfulQueryStats
    );
    NetflixHttpPostRequestLogger.KeyStoneGatewayRequest request = new NetflixHttpPostRequestLogger.KeyStoneGatewayRequest(
        "druid",
        "localhost",
        false,
        logLine
    );
    NetflixHttpPostRequestLogger.Payload payload = request.getPayload();
    Assert.assertEquals(QUERY_ID, payload.getQueryId());
    Assert.assertEquals(DATASOURCE, payload.getDatasource());
    Assert.assertEquals(QUERY_TYPE, payload.getQueryType());
    Assert.assertEquals(DESCENDING, payload.isDescending());
    Assert.assertEquals(HAS_FILTERS, payload.isHasFilters());
    Assert.assertEquals(REMOTE_ADDRESS, payload.getRemoteAddress());
    Assert.assertEquals(true, payload.isQuerySuccessful());
    Assert.assertEquals(QUERY_TIME, payload.getQueryTime());
    Assert.assertEquals(QUERY_BYTES, payload.getQueryBytes());
    Assert.assertNull(payload.getErrorStackTrace());
    Assert.assertFalse(payload.isWasInterrupted());
    Assert.assertNull(payload.getInterruptionReason());
  }

  @Test
  public void testFailedQueryPayload()
  {
    final DateTime timestamp = DateTimes.of("2016-01-01T00:00:00Z");

    final Query failedQuery = new FakeQuery(
        new LegacyDataSource(DATASOURCE),
        new QuerySegmentSpec()
        {
          @Override
          public List<Interval> getIntervals()
          {
            return Collections.singletonList(Intervals.of("2016-01-01T00Z/2016-01-02T00Z"));
          }

          @Override
          public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
          {
            return null;
          }
        }, DESCENDING, QUERY_CONTEXT
    );
    final QueryStats failedQueryStats = new QueryStats(ImmutableMap.of(
        NetflixHttpPostRequestLogger.QueryStatsKey.QUERY_TIME.toString(),
        QUERY_TIME,
        NetflixHttpPostRequestLogger.QueryStatsKey.QUERY_BYTES.toString(),
        QUERY_BYTES,
        NetflixHttpPostRequestLogger.QueryStatsKey.ERROR_STACKTRACE.toString(),
        ERROR_STACK_TRACE,
        NetflixHttpPostRequestLogger.QueryStatsKey.SUCCESS.toString(),
        false
    ));
    final RequestLogLine logLine = RequestLogLine.forNative(failedQuery, timestamp, REMOTE_ADDRESS, failedQueryStats);
    NetflixHttpPostRequestLogger.KeyStoneGatewayRequest request = new NetflixHttpPostRequestLogger.KeyStoneGatewayRequest(
        "druid",
        "localhost",
        false,
        logLine
    );
    NetflixHttpPostRequestLogger.Payload payload = request.getPayload();
    Assert.assertEquals(QUERY_ID, payload.getQueryId());
    Assert.assertEquals(DATASOURCE, payload.getDatasource());
    Assert.assertEquals(QUERY_TYPE, payload.getQueryType());
    Assert.assertEquals(DESCENDING, payload.isDescending());
    Assert.assertEquals(HAS_FILTERS, payload.isHasFilters());
    Assert.assertEquals(REMOTE_ADDRESS, payload.getRemoteAddress());
    Assert.assertFalse(payload.isQuerySuccessful());
    Assert.assertEquals(QUERY_TIME, payload.getQueryTime());
    Assert.assertEquals(QUERY_BYTES, payload.getQueryBytes());
    Assert.assertEquals(ERROR_STACK_TRACE, payload.getErrorStackTrace());
    Assert.assertFalse(payload.isWasInterrupted());
    Assert.assertNull(payload.getInterruptionReason());
  }

  @Test
  public void testInterruptedQueryPayload()
  {
    final DateTime timestamp = DateTimes.of("2016-01-01T00:00:00Z");
    final Query interruptedQuery = new FakeQuery(
        new LegacyDataSource(DATASOURCE),
        new QuerySegmentSpec()
        {
          @Override
          public List<Interval> getIntervals()
          {
            return Collections.singletonList(Intervals.of("2016-01-01T00Z/2016-01-02T00Z"));
          }

          @Override
          public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
          {
            return null;
          }
        }, DESCENDING, QUERY_CONTEXT
    );
    final String interruptionReason = "query timed out";
    final QueryStats interruptedQueryStats = new QueryStats(ImmutableMap.of(
        NetflixHttpPostRequestLogger.QueryStatsKey.QUERY_TIME.toString(),
        QUERY_TIME,
        NetflixHttpPostRequestLogger.QueryStatsKey.QUERY_BYTES.toString(),
        QUERY_BYTES,
        NetflixHttpPostRequestLogger.QueryStatsKey.INTERRUPTED.toString(),
        true,
        NetflixHttpPostRequestLogger.QueryStatsKey.INTERRUPTION_REASON.toString(),
        interruptionReason,
        NetflixHttpPostRequestLogger.QueryStatsKey.SUCCESS.toString(),
        false

    ));
    final RequestLogLine logLine = RequestLogLine.forNative(
        interruptedQuery,
        timestamp,
        REMOTE_ADDRESS,
        interruptedQueryStats
    );
    NetflixHttpPostRequestLogger.KeyStoneGatewayRequest request = new NetflixHttpPostRequestLogger.KeyStoneGatewayRequest(
        "druid",
        "localhost",
        false,
        logLine
    );
    NetflixHttpPostRequestLogger.Payload payload = request.getPayload();
    Assert.assertEquals(QUERY_ID, payload.getQueryId());
    Assert.assertEquals(DATASOURCE, payload.getDatasource());
    Assert.assertEquals(QUERY_TYPE, payload.getQueryType());
    Assert.assertEquals(DESCENDING, payload.isDescending());
    Assert.assertEquals(HAS_FILTERS, payload.isHasFilters());
    Assert.assertEquals(REMOTE_ADDRESS, payload.getRemoteAddress());
    Assert.assertFalse(payload.isQuerySuccessful());
    Assert.assertEquals(QUERY_TIME, payload.getQueryTime());
    Assert.assertEquals(QUERY_BYTES, payload.getQueryBytes());
    Assert.assertNull(payload.getErrorStackTrace());
    Assert.assertTrue(payload.isWasInterrupted());
    Assert.assertEquals(interruptionReason, payload.getInterruptionReason());
  }


  @JsonTypeName("fake")
  private static class FakeQuery extends BaseQuery
  {
    public FakeQuery(DataSource dataSource, QuerySegmentSpec querySegmentSpec, boolean descending, Map context)
    {
      super(dataSource, querySegmentSpec, descending, context);
    }

    @Override
    public boolean hasFilters()
    {
      return false;
    }

    @Override
    public DimFilter getFilter()
    {
      return new DimFilter()
      {
        @Override
        public DimFilter optimize()
        {
          return null;
        }

        @Override
        public Filter toFilter()
        {
          return null;
        }

        @Override
        public RangeSet<String> getDimensionRangeSet(String dimension)
        {
          return null;
        }

        @Override
        public Set<String> getRequiredColumns()
        {
          return null;
        }

        @Override
        public byte[] getCacheKey()
        {
          return new byte[0];
        }
      };
    }

    @Override
    public String getType()
    {
      return QUERY_TYPE;
    }

    @Override
    public Query withQuerySegmentSpec(QuerySegmentSpec spec)
    {
      return this;
    }

    @Override
    public Query withDataSource(DataSource dataSource)
    {
      return this;
    }

    @Override
    public Query withOverriddenContext(Map contextOverride)
    {
      return this;
    }
  }
}

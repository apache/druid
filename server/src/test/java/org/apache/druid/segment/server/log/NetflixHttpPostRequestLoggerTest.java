package io.druid.server.log;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.RangeSet;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.LegacyDataSource;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.Filter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.server.QueryStats;
import io.druid.server.RequestLogLine;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

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
      BaseQuery.QUERYID,
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
    final RequestLogLine logLine = new RequestLogLine(
        timestamp,
        REMOTE_ADDRESS,
        successfulQuery,
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
    final RequestLogLine logLine = new RequestLogLine(
        timestamp,
        REMOTE_ADDRESS,
        failedQuery,
        failedQueryStats
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
    final RequestLogLine logLine = new RequestLogLine(
        timestamp,
        REMOTE_ADDRESS,
        interruptedQuery,
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

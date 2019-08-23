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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.LegacyDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.OutputStreamAppender;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Mostly just test that it doesn't crash
public class LoggingRequestLoggerTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final ByteArrayOutputStream BAOS = new ByteArrayOutputStream();
  private static Appender appender;

  final DateTime timestamp = DateTimes.of("2016-01-01T00:00:00Z");
  final String remoteAddr = "some.host.tld";
  final Map<String, Object> queryContext = ImmutableMap.of("foo", "bar");
  final Query query = new FakeQuery(
      new LegacyDataSource("datasource"),
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
      }, false, queryContext
  );

  final Query nestedQuery = new FakeQuery(
      new QueryDataSource(query),
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
      }, false, queryContext
  );

  final Query nestedNestedQuery = new FakeQuery(
      new QueryDataSource(nestedQuery),
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
      }, false, queryContext
  );

  final Query unionQuery = new FakeQuery(
      new UnionDataSource(ImmutableList.of(new LegacyDataSource("A"), new LegacyDataSource("B"))),
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
      }, false, queryContext
  );


  final QueryStats queryStats = new QueryStats(ImmutableMap.of());
  final RequestLogLine logLine = RequestLogLine.forNative(
      query,
      timestamp,
      remoteAddr,
      queryStats
  );

  @BeforeClass
  public static void setUpStatic()
  {
    appender = OutputStreamAppender
        .newBuilder()
        .setName("test stream")
        .setTarget(BAOS)
        .setLayout(JsonLayout.createLayout(false, true, false, true, true, StandardCharsets.UTF_8))
        .build();
    final Logger logger = (Logger)
        LogManager.getLogger(LoggingRequestLogger.class);
    appender.start();
    logger.addAppender(appender);
  }

  @After
  public void tearDown()
  {
    BAOS.reset();
  }

  @AfterClass
  public static void tearDownStatic()
  {
    final Logger logger = (Logger) LogManager.getLogger(
        LoggingRequestLogger.class);
    logger.removeAppender(appender);
    appender.stop();
  }

  @Test
  public void testSimpleLogging() throws Exception
  {
    final LoggingRequestLogger requestLogger = new LoggingRequestLogger(new DefaultObjectMapper(), false, false);
    requestLogger.logNativeQuery(logLine);
  }

  @Test
  public void testLoggingMDC() throws Exception
  {
    final LoggingRequestLogger requestLogger = new LoggingRequestLogger(new DefaultObjectMapper(), true, false);
    requestLogger.logNativeQuery(logLine);
    final Map<String, Object> map = readContextMap(BAOS.toByteArray());
    Assert.assertEquals("datasource", map.get("dataSource"));
    Assert.assertEquals("PT86400S", map.get("duration"));
    Assert.assertEquals("false", map.get("hasFilters"));
    Assert.assertEquals("fake", map.get("queryType"));
    Assert.assertEquals("some.host.tld", map.get("remoteAddr"));
    Assert.assertEquals("false", map.get("descending"));
    Assert.assertEquals("false", map.get("isNested"));
    Assert.assertNull(map.get("foo"));
  }

  @Test
  public void testLoggingMDCContext() throws Exception
  {
    final LoggingRequestLogger requestLogger = new LoggingRequestLogger(new DefaultObjectMapper(), true, true);
    requestLogger.logNativeQuery(logLine);
    final Map<String, Object> map = readContextMap(BAOS.toByteArray());
    Assert.assertEquals("datasource", map.get("dataSource"));
    Assert.assertEquals("PT86400S", map.get("duration"));
    Assert.assertEquals("false", map.get("hasFilters"));
    Assert.assertEquals("fake", map.get("queryType"));
    Assert.assertEquals("some.host.tld", map.get("remoteAddr"));
    Assert.assertEquals("false", map.get("descending"));
    Assert.assertEquals("false", map.get("isNested"));
    Assert.assertEquals("bar", map.get("foo"));
  }

  @Test
  public void testNestedQueryLoggingMDC() throws Exception
  {
    final LoggingRequestLogger requestLogger = new LoggingRequestLogger(new DefaultObjectMapper(), true, false);
    requestLogger.logNativeQuery(RequestLogLine.forNative(
        nestedQuery,
        timestamp,
        remoteAddr,
        queryStats
    ));
    final Map<String, Object> map = readContextMap(BAOS.toByteArray());
    Assert.assertEquals("datasource", map.get("dataSource"));
    Assert.assertEquals("PT86400S", map.get("duration"));
    Assert.assertEquals("false", map.get("hasFilters"));
    Assert.assertEquals("fake", map.get("queryType"));
    Assert.assertEquals("some.host.tld", map.get("remoteAddr"));
    Assert.assertEquals("false", map.get("descending"));
    Assert.assertEquals("true", map.get("isNested"));
    Assert.assertNull(map.get("foo"));
  }

  @Test
  public void testNestedNestedQueryLoggingMDC() throws Exception
  {
    final LoggingRequestLogger requestLogger = new LoggingRequestLogger(new DefaultObjectMapper(), true, false);
    requestLogger.logNativeQuery(RequestLogLine.forNative(
        nestedNestedQuery,
        timestamp,
        remoteAddr,
        queryStats
    ));
    final Map<String, Object> map = readContextMap(BAOS.toByteArray());
    Assert.assertEquals("datasource", map.get("dataSource"));
    Assert.assertEquals("PT86400S", map.get("duration"));
    Assert.assertEquals("false", map.get("hasFilters"));
    Assert.assertEquals("fake", map.get("queryType"));
    Assert.assertEquals("some.host.tld", map.get("remoteAddr"));
    Assert.assertEquals("true", map.get("isNested"));
    Assert.assertEquals("false", map.get("descending"));
    Assert.assertNull(map.get("foo"));
  }

  @Test
  public void testUnionQueryLoggingMDC() throws Exception
  {
    final LoggingRequestLogger requestLogger = new LoggingRequestLogger(new DefaultObjectMapper(), true, false);
    requestLogger.logNativeQuery(RequestLogLine.forNative(
        unionQuery,
        timestamp,
        remoteAddr,
        queryStats
    ));
    final Map<String, Object> map = readContextMap(BAOS.toByteArray());
    Assert.assertEquals("A,B", map.get("dataSource"));
    Assert.assertEquals("true", map.get("isNested"));
    Assert.assertEquals("PT86400S", map.get("duration"));
    Assert.assertEquals("false", map.get("hasFilters"));
    Assert.assertEquals("fake", map.get("queryType"));
    Assert.assertEquals("some.host.tld", map.get("remoteAddr"));
    Assert.assertEquals("false", map.get("descending"));
    Assert.assertNull(map.get("foo"));
  }

  private static Map<String, Object> readContextMap(byte[] bytes) throws Exception
  {
    final Map<String, Object> rawMap = MAPPER.readValue(bytes, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
    final Object contextMap = rawMap.get("contextMap");
    if (contextMap == null) {
      return null;
    }
    final Collection<Map<String, Object>> contextList = (Collection<Map<String, Object>>) contextMap;
    final Map<String, Object> context = new HashMap<>();
    for (Map<String, Object> microContext : contextList) {
      final String key = microContext.get("key").toString();
      final Object value = microContext.get("value");
      if (key != null && value != null) {
        context.put(key, value);
      }
    }
    return ImmutableMap.copyOf(context);
  }
}

@JsonTypeName("fake")
class FakeQuery extends BaseQuery
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
    throw new UnsupportedOperationException("shouldn't be here");
  }

  @Override
  public String getType()
  {
    return "fake";
  }

  @Override
  public Query withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    throw new UnsupportedOperationException("shouldn't be here");
  }

  @Override
  public Query withDataSource(DataSource dataSource)
  {
    throw new UnsupportedOperationException("shouldn't be here");
  }

  @Override
  public Query withOverriddenContext(Map contextOverride)
  {
    throw new UnsupportedOperationException("shouldn't be here");
  }
}

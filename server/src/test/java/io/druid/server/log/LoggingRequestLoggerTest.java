/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.log;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.LegacyDataSource;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.server.QueryStats;
import io.druid.server.RequestLogLine;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Mostly just test that it doesn't crash
public class LoggingRequestLoggerTest
{
  private static final ObjectMapper mapper = new DefaultObjectMapper();
  private static final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private static Appender appender;

  final DateTime timestamp = DateTime.parse("2016-01-01T00:00:00Z");
  final String remoteAddr = "some.host.tld";
  final Map<String, Object> queryContext = ImmutableMap.<String, Object>of("foo", "bar");
  final Query query = new FakeQuery(
      new LegacyDataSource("datasource"),
      new QuerySegmentSpec()
      {
        @Override
        public List<Interval> getIntervals()
        {
          return Collections.singletonList(Interval.parse("2016-01-01T00Z/2016-01-02T00Z"));
        }

        @Override
        public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
        {
          return null;
        }
      }, false, queryContext
  );
  final QueryStats queryStats = new QueryStats(ImmutableMap.<String, Object>of());
  final RequestLogLine logLine = new RequestLogLine(
      timestamp,
      remoteAddr,
      query,
      queryStats
  );

  @BeforeClass
  public static void setUpStatic() throws Exception
  {
    appender = OutputStreamAppender
        .newBuilder()
        .setName("test stream")
        .setTarget(baos)
        .setLayout(JsonLayout.createLayout(false, true, false, true, true, Charsets.UTF_8))
        .build();
    final Logger logger = (Logger)
        LogManager.getLogger(LoggingRequestLogger.class);
    appender.start();
    logger.addAppender(appender);
  }

  @After
  public void tearDown()
  {
    baos.reset();
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
    requestLogger.log(logLine);
  }

  @Test
  public void testLoggingMDC() throws Exception
  {
    final LoggingRequestLogger requestLogger = new LoggingRequestLogger(new DefaultObjectMapper(), true, false);
    requestLogger.log(logLine);
    final Map<String, Object> map = readContextMap(baos.toByteArray());
    Assert.assertEquals("datasource", map.get("dataSource"));
    Assert.assertEquals("PT86400S", map.get("duration"));
    Assert.assertEquals("false", map.get("hasFilters"));
    Assert.assertEquals("fake", map.get("queryType"));
    Assert.assertEquals("some.host.tld", map.get("remoteAddr"));
    Assert.assertEquals("false", map.get("descending"));
    Assert.assertNull(map.get("foo"));
  }

  @Test
  public void testLoggingMDCContext() throws Exception
  {
    final LoggingRequestLogger requestLogger = new LoggingRequestLogger(new DefaultObjectMapper(), true, true);
    requestLogger.log(logLine);
    final Map<String, Object> map = readContextMap(baos.toByteArray());
    Assert.assertEquals("datasource", map.get("dataSource"));
    Assert.assertEquals("PT86400S", map.get("duration"));
    Assert.assertEquals("false", map.get("hasFilters"));
    Assert.assertEquals("fake", map.get("queryType"));
    Assert.assertEquals("some.host.tld", map.get("remoteAddr"));
    Assert.assertEquals("false", map.get("descending"));
    Assert.assertEquals("bar", map.get("foo"));
  }

  private static Map<String, Object> readContextMap(byte[] bytes) throws Exception
  {
    final Map<String, Object> rawMap = mapper.readValue(bytes, new TypeReference<Map<String, Object>>()
    {
    });
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

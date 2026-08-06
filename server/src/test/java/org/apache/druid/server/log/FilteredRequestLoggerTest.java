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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.ProvisionException;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.validation.Validation;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class FilteredRequestLoggerTest
{
  private final DefaultObjectMapper mapper = new DefaultObjectMapper();
  private final SegmentMetadataQuery testSegmentMetadataQuery = new SegmentMetadataQuery(
      new TableDataSource("foo"),
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
  );

  public FilteredRequestLoggerTest()
  {
    mapper.registerSubtypes(
        LoggingRequestLoggerProvider.class,
        FilteredRequestLoggerProvider.class,
        TestRequestLoggerProvider.class,
        NoopRequestLoggerProvider.class
    );

    final InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(ObjectMapper.class, mapper);
    mapper.setInjectableValues(injectableValues);
  }

  @Test
  public void testFilterBelowThreshold() throws IOException
  {
    RequestLogger delegate = EasyMock.createStrictMock(RequestLogger.class);
    delegate.logNativeQuery(EasyMock.anyObject());
    EasyMock.expectLastCall().andThrow(new IOException());
    delegate.logSqlQuery(EasyMock.anyObject());
    EasyMock.expectLastCall().andThrow(new IOException());

    FilteredRequestLoggerProvider.FilteredRequestLogger logger = new FilteredRequestLoggerProvider.FilteredRequestLogger(
        delegate,
        1000,
        2000,
        Set.of()
    );
    RequestLogLine nativeRequestLogLine = EasyMock.createMock(RequestLogLine.class);
    EasyMock.expect(nativeRequestLogLine.getQueryStats())
            .andReturn(new QueryStats(ImmutableMap.of("query/time", 100)))
            .once();

    RequestLogLine sqlRequestLogLine = EasyMock.createMock(RequestLogLine.class);
    EasyMock.expect(sqlRequestLogLine.getQueryStats())
            .andReturn(new QueryStats(ImmutableMap.of("sqlQuery/time", 1000)));

    EasyMock.replay(nativeRequestLogLine, sqlRequestLogLine, delegate);
    logger.logNativeQuery(nativeRequestLogLine);
    logger.logSqlQuery(sqlRequestLogLine);
  }

  private static class MockLogger implements RequestLogger
  {
    private int nativeCount;
    private int sqlCount;

    @Override
    public void logNativeQuery(RequestLogLine requestLogLine)
    {
      nativeCount++;
    }

    @Override
    public void logSqlQuery(RequestLogLine requestLogLine)
    {
      sqlCount++;
    }
  }

  @Test
  public void testNotFilterAboveThreshold() throws IOException
  {
    MockLogger delegate = new MockLogger();

    FilteredRequestLoggerProvider.FilteredRequestLogger logger = new FilteredRequestLoggerProvider.FilteredRequestLogger(
        delegate,
        1000,
        2000,
        Set.of()
    );

    RequestLogLine nativeRequestLogLine = RequestLogLine.forNative(
        testSegmentMetadataQuery,
        DateTimes.nowUtc(), // Not used
        null, // Not used
        new QueryStats(ImmutableMap.of("query/time", 1000))
    );

    RequestLogLine sqlRequestLogLine = RequestLogLine.forSql(
        "SELECT * FROM foo",
        null,
        DateTimes.nowUtc(), // Not used
        null,  // Not used
        new QueryStats(ImmutableMap.of("sqlQuery/time", 2000))
    );

    logger.logNativeQuery(nativeRequestLogLine);
    logger.logNativeQuery(nativeRequestLogLine);
    logger.logSqlQuery(sqlRequestLogLine);
    logger.logSqlQuery(sqlRequestLogLine);

    Assertions.assertEquals(2, delegate.nativeCount);
    Assertions.assertEquals(2, delegate.sqlCount);
  }

  @Test
  public void testNotFilterAboveThresholdSkipSegmentMetadata() throws IOException
  {
    MockLogger delegate = new MockLogger();

    FilteredRequestLoggerProvider.FilteredRequestLogger logger = new FilteredRequestLoggerProvider.FilteredRequestLogger(
        delegate,
        1000,
        2000,
        Set.of(Query.SEGMENT_METADATA)
    );

    RequestLogLine nativeRequestLogLine = RequestLogLine.forNative(
        testSegmentMetadataQuery,
        DateTimes.nowUtc(), // Not used
        null, // Not used
        new QueryStats(ImmutableMap.of("query/time", 10000))
    );

    RequestLogLine sqlRequestLogLine = RequestLogLine.forSql(
        "SELECT * FROM foo",
        null,
        DateTimes.nowUtc(), // Not used
        null,  // Not used
        new QueryStats(ImmutableMap.of("sqlQuery/time", 10000))
    );

    logger.logNativeQuery(nativeRequestLogLine);
    logger.logSqlQuery(sqlRequestLogLine);

    Assertions.assertEquals(0, delegate.nativeCount);
    Assertions.assertEquals(1, delegate.sqlCount);
  }

  @Test
  public void testConfiguration()
  {
    final Properties properties = new Properties();
    properties.setProperty("log.type", "filtered");
    properties.setProperty("log.queryTimeThresholdMs", "100");
    properties.setProperty("log.delegate.type", "slf4j");
    properties.setProperty("log.delegate.setMDC", "true");
    properties.setProperty("log.delegate.setContextMDC", "true");

    final JsonConfigurator configurator = new JsonConfigurator(
        mapper,
        Validation.buildDefaultValidatorFactory()
                  .getValidator()
    );

    final FilteredRequestLoggerProvider provider = (FilteredRequestLoggerProvider) configurator.configurate(
        properties,
        "log",
        RequestLoggerProvider.class
    );
    final FilteredRequestLoggerProvider.FilteredRequestLogger logger =
        ((FilteredRequestLoggerProvider.FilteredRequestLogger) provider.get());
    final LoggingRequestLogger delegate = (LoggingRequestLogger) logger.getDelegate();

    Assertions.assertEquals(100, logger.getQueryTimeThresholdMs());
    Assertions.assertTrue(delegate.isSetContextMDC());
    Assertions.assertTrue(delegate.isSetMDC());
  }

  @Test
  public void testStartStop() throws Exception
  {
    final Properties properties = new Properties();
    properties.setProperty("log.type", "filtered");
    properties.setProperty("log.queryTimeThresholdMs", "100");
    properties.setProperty("log.delegate.type", "test");

    final JsonConfigurator configurator = new JsonConfigurator(
        mapper,
        Validation.buildDefaultValidatorFactory()
                  .getValidator()
    );

    final FilteredRequestLoggerProvider provider = (FilteredRequestLoggerProvider) configurator.configurate(
        properties,
        "log",
        RequestLoggerProvider.class
    );

    final FilteredRequestLoggerProvider.FilteredRequestLogger logger =
        ((FilteredRequestLoggerProvider.FilteredRequestLogger) provider.get());
    final TestRequestLogger delegate = (TestRequestLogger) logger.getDelegate();

    Assertions.assertFalse(delegate.isStarted());

    logger.start();
    Assertions.assertTrue(delegate.isStarted());

    logger.stop();
    Assertions.assertFalse(delegate.isStarted());
  }

  @Test
  public void testInvalidDelegateType()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(ProvisionException.class, () -> {
      final Properties properties = new Properties();
      properties.setProperty("log.type", "filtered");
      properties.setProperty("log.queryTimeThresholdMs", "100");
      properties.setProperty("log.delegate.type", "nope");

      final JsonConfigurator configurator = new JsonConfigurator(
          mapper,
          Validation.buildDefaultValidatorFactory()
              .getValidator()
      );
      configurator.configurate(properties, "log", RequestLoggerProvider.class);
    });
    assertTrue(exception.getMessage().contains("Could not resolve type id 'nope'"));
  }

  @Test
  public void testNoDelegate()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(ProvisionException.class, () -> {
      final Properties properties = new Properties();
      properties.setProperty("log.type", "filtered");
      properties.setProperty("log.queryTimeThresholdMs", "100");

      final JsonConfigurator configurator = new JsonConfigurator(
          mapper,
          Validation.buildDefaultValidatorFactory()
              .getValidator()
      );
      configurator.configurate(properties, "log", RequestLoggerProvider.class);
    });
    assertTrue(exception.getMessage().contains("log.delegate - must not be null"));
  }
}

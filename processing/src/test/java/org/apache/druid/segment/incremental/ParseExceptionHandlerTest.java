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

package org.apache.druid.segment.incremental;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.testing.junit.LoggerCaptureRule;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.stream.IntStream;

public class ParseExceptionHandlerTest
{
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Rule
  public LoggerCaptureRule logger = new LoggerCaptureRule(ParseExceptionHandler.class);

  @Test
  public void testMetricWhenAllConfigurationsAreTurnedOff()
  {
    final ParseException parseException = new ParseException("test");
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        false,
        Integer.MAX_VALUE,
        0
    );

    IntStream.range(0, 100).forEach(i -> {
      parseExceptionHandler.handle(parseException);
      Assert.assertEquals(i + 1, rowIngestionMeters.getUnparseable());
    });
  }

  @Test
  public void testLogParseExceptions()
  {
    final ParseException parseException = new ParseException("test");
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        true,
        Integer.MAX_VALUE,
        0
    );
    parseExceptionHandler.handle(parseException);

    List<LogEvent> logEvents = logger.getLogEvents();
    Assert.assertEquals(1, logEvents.size());
    String logMessage = logEvents.get(0).getMessage().getFormattedMessage();
    Assert.assertTrue(logMessage.contains("Encountered parse exception"));
  }

  @Test
  public void testGetSavedParseExceptionsReturnNullWhenMaxSavedParseExceptionsIsZero()
  {
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        new SimpleRowIngestionMeters(),
        false,
        Integer.MAX_VALUE,
        0
    );
    Assert.assertNull(parseExceptionHandler.getSavedParseExceptions());
  }

  @Test
  public void testMaxAllowedParseExceptionsThrowExceptionWhenItHitsMax()
  {
    final ParseException parseException = new ParseException("test");
    final int maxAllowedParseExceptions = 3;
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        false,
        maxAllowedParseExceptions,
        0
    );

    IntStream.range(0, maxAllowedParseExceptions).forEach(i -> parseExceptionHandler.handle(parseException));
    Assert.assertEquals(3, rowIngestionMeters.getUnparseable());

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Max parse exceptions[3] exceeded");
    try {
      parseExceptionHandler.handle(parseException);
    }
    catch (RuntimeException e) {
      Assert.assertEquals(4, rowIngestionMeters.getUnparseable());
      throw e;
    }
  }

  @Test
  public void testGetSavedParseExceptionsReturnMostRecentParseExceptions()
  {
    final int maxSavedParseExceptions = 3;
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        false,
        Integer.MAX_VALUE,
        maxSavedParseExceptions
    );
    Assert.assertNotNull(parseExceptionHandler.getSavedParseExceptions());
    int exceptionCounter = 0;
    for (; exceptionCounter < maxSavedParseExceptions; exceptionCounter++) {
      parseExceptionHandler.handle(new ParseException(StringUtils.format("test %d", exceptionCounter)));
    }
    Assert.assertEquals(3, rowIngestionMeters.getUnparseable());
    Assert.assertEquals(maxSavedParseExceptions, parseExceptionHandler.getSavedParseExceptions().size());
    for (int i = 0; i < maxSavedParseExceptions; i++) {
      Assert.assertEquals(
          StringUtils.format("test %d", i),
          parseExceptionHandler.getSavedParseExceptions().get(i).getMessage()
      );
    }
    for (; exceptionCounter < 5; exceptionCounter++) {
      parseExceptionHandler.handle(new ParseException(StringUtils.format("test %d", exceptionCounter)));
    }
    Assert.assertEquals(5, rowIngestionMeters.getUnparseable());

    Assert.assertEquals(maxSavedParseExceptions, parseExceptionHandler.getSavedParseExceptions().size());
    for (int i = 0; i < maxSavedParseExceptions; i++) {
      Assert.assertEquals(
          StringUtils.format("test %d", i + 2),
          parseExceptionHandler.getSavedParseExceptions().get(i).getMessage()
      );
    }
  }
}

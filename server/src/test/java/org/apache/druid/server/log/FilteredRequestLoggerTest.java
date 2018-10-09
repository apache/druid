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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.server.RequestLogLine;
import org.apache.druid.server.QueryStats;
import org.easymock.EasyMock;
import org.junit.Test;

import java.io.IOException;

public class FilteredRequestLoggerTest
{
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
        2000
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

  @Test
  public void testNotFilterAboveThreshold() throws IOException
  {
    RequestLogger delegate = EasyMock.createStrictMock(RequestLogger.class);
    delegate.logNativeQuery(EasyMock.anyObject());
    EasyMock.expectLastCall().times(2);
    delegate.logSqlQuery(EasyMock.anyObject());
    EasyMock.expectLastCall().times(2);

    FilteredRequestLoggerProvider.FilteredRequestLogger logger = new FilteredRequestLoggerProvider.FilteredRequestLogger(
        delegate,
        1000,
        2000
    );

    RequestLogLine nativeRequestLogLine = EasyMock.createMock(RequestLogLine.class);
    EasyMock.expect(nativeRequestLogLine.getQueryStats())
            .andReturn(new QueryStats(ImmutableMap.of("query/time", 10000)))
            .once();
    EasyMock.expect(nativeRequestLogLine.getQueryStats())
            .andReturn(new QueryStats(ImmutableMap.of("query/time", 1000)))
            .once();

    RequestLogLine sqlRequestLogLine = EasyMock.createMock(RequestLogLine.class);
    EasyMock.expect(sqlRequestLogLine.getQueryStats())
            .andReturn(new QueryStats(ImmutableMap.of("sqlQuery/time", 10000)))
            .once();
    EasyMock.expect(sqlRequestLogLine.getQueryStats())
            .andReturn(new QueryStats(ImmutableMap.of("sqlQuery/time", 2000)))
            .once();

    EasyMock.replay(nativeRequestLogLine, sqlRequestLogLine, delegate);

    logger.logNativeQuery(nativeRequestLogLine);
    logger.logNativeQuery(nativeRequestLogLine);
    logger.logSqlQuery(sqlRequestLogLine);
    logger.logSqlQuery(sqlRequestLogLine);

    EasyMock.verify(nativeRequestLogLine, sqlRequestLogLine, delegate);
  }
}

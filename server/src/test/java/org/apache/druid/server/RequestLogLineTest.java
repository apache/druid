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

package org.apache.druid.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class RequestLogLineTest
{
  private Query query;

  @BeforeEach
  public void setUp()
  {
    query = new TimeBoundaryQuery(
        new TableDataSource("test"),
        null,
        null,
        null,
        null
    );
  }

  @Test
  public void nullTimestamp()
  {
    org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class, () -> {
      RequestLogLine requestLogLine = RequestLogLine.forNative(
          query,
          null,
          "",
          new QueryStats(ImmutableMap.of())
      );
    });
  }

  @Test
  public void nullQueryStats()
  {
    org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class, () -> {
      RequestLogLine requestLogLine = RequestLogLine.forNative(
          query,
          DateTimes.nowUtc(),
          "",
          null
      );
    });
  }

  @Test
  public void nullRemoteAddressAndNullSqlQueryContext() throws JsonProcessingException
  {
    RequestLogLine requestLogLine = RequestLogLine.forNative(
        query,
        DateTimes.nowUtc(),
        null,
        new QueryStats(ImmutableMap.of())
    );
    Assertions.assertEquals("", requestLogLine.getRemoteAddr());
    requestLogLine.getNativeQueryLine(new DefaultObjectMapper()); // call should not throw exception

    requestLogLine = RequestLogLine.forSql(
        "", null, DateTimes.nowUtc(), null, new QueryStats(ImmutableMap.of())
    );
    Assertions.assertEquals("", requestLogLine.getRemoteAddr());
    Assertions.assertEquals(ImmutableMap.<String, Object>of(), requestLogLine.getSqlQueryContext());
    requestLogLine.getSqlQueryLine(new DefaultObjectMapper()); // call should not throw exception
  }

}

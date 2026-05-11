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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.QueryResource.QueryMetricCounter;
import org.apache.druid.server.QueryResultPusher.ResultsWriter;
import org.apache.druid.server.QueryResultPusher.Writer;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.ResponseBuilder;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

public class QueryResultPusherTest
{
  private static final DruidNode DRUID_NODE = new DruidNode(
      "broker",
      "localhost",
      true,
      8082,
      null,
      true,
      false);

  @Test
  public void testResultPusherRetainsNestedExceptionBacktraces()
  {

    HttpServletRequest request = new MockHttpServletRequest();
    ObjectMapper jsonMapper = new DefaultObjectMapper();
    ResponseContextConfig responseContextConfig = ResponseContextConfig.newConfig(true);
    DruidNode selfNode = DRUID_NODE;
    QueryResource.QueryMetricCounter counter = new NoopQueryMetricCounter();
    String queryId = "someQuery";
    MediaType contentType = MediaType.APPLICATION_JSON_TYPE;
    Map<String, String> extraHeaders = new HashMap<String, String>();
    AtomicBoolean recordFailureInvoked = new AtomicBoolean();

    String embeddedExceptionMessage = "Embedded Exception Message!";
    RuntimeException embeddedException = new RuntimeException(embeddedExceptionMessage);
    RuntimeException topException = new RuntimeException("Where's the party?", embeddedException);

    ResultsWriter resultWriter = new ResultsWriter()
    {

      @Override
      public void close()
      {
      }

      @Override
      public ResponseBuilder start()
      {
        throw topException;
      }

      @Override
      public void recordSuccess(long numBytes)
      {
      }

      @Override
      public void recordSuccess(long numBytes, long numRowsScanned, long cpuTimeInMillis)
      {

      }

      @Override
      public void recordFailure(Exception e)
      {
        assertTrue(Throwables.getStackTraceAsString(e).contains(embeddedExceptionMessage));
        recordFailureInvoked.set(true);
      }

      @Override
      public Writer makeWriter(OutputStream out)
      {
        return null;
      }

      @Override
      public QueryResponse<Object> getQueryResponse()
      {
        return null;
      }
    };
    QueryResultPusher pusher = new QueryResultPusher(
        request,
        jsonMapper,
        responseContextConfig,
        selfNode,
        counter,
        queryId,
        contentType,
        extraHeaders)
    {

      @Override
      public void writeException(Exception e, OutputStream out)
      {
      }

      @Override
      public ResultsWriter start()
      {
        return resultWriter;
      }
    };

    pusher.push();

    assertTrue("recordFailure(e) should have been invoked!", recordFailureInvoked.get());
  }

  static class NoopQueryMetricCounter implements QueryMetricCounter
  {

    @Override
    public void incrementSuccess()
    {
    }

    @Override
    public void incrementFailed()
    {
    }

    @Override
    public void incrementInterrupted()
    {
    }

    @Override
    public void incrementTimedOut()
    {
    }

  }
}

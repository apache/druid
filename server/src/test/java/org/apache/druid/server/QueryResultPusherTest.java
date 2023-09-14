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
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.server.QueryResultPusher.ResultsWriter;
import org.junit.Test;

import javax.servlet.AsyncContext;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class QueryResultPusherTest
{
  @Test
  public void testResultPusherRetainsNestedExceptionBacktraces()
  {
    HttpServletRequest request = mock(HttpServletRequest.class);
    ObjectMapper jsonMapper = new ObjectMapper();
    ResponseContextConfig responseContextConfig = ResponseContextConfig.newConfig(true);
    DruidNode selfNode = mock(DruidNode.class);
    QueryResource.QueryMetricCounter counter = mock(QueryResource.QueryMetricCounter.class);
    String queryId = "someQuery";
    MediaType contentType = mock(MediaType.class);
    Map<String, String> extraHeaders = new HashMap<String, String>();

    ResultsWriter resultWriter = mock(ResultsWriter.class);
    List<Exception> loggedExceptions = mock(List.class);

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
      public void writeException(Exception e, OutputStream out) throws IOException
      {
        loggedExceptions.add(e);
      }

      @Override
      public ResultsWriter start()
      {
        return resultWriter;
      }
    };

    String embeddedExceptionMessage = "Embedded Exception Message!";
    RuntimeException embeddedException = new RuntimeException(embeddedExceptionMessage);
    RuntimeException topException = new RuntimeException("Where's the party?", embeddedException);

    QueryResponse<Object> queryResponse = mock(QueryResponse.class);
    Sequence<Object> results = mock(Sequence.class);
    AsyncContext asyncContext = mock(AsyncContext.class);
    ServletResponse response = mock(HttpServletResponse.class);

    when(resultWriter.getQueryResponse()).thenReturn(queryResponse);
    when(queryResponse.getResults()).thenReturn(results);
    when(results.accumulate(any(), any())).thenThrow(topException);
    when(request.startAsync()).thenReturn(asyncContext);
    when(asyncContext.getResponse()).thenReturn(response);

    // run pusher
    pusher.push();

    verify(resultWriter)
        .recordFailure(argThat(e -> Throwables.getStackTraceAsString(e).contains(embeddedExceptionMessage)));
    verify(loggedExceptions)
        .add(argThat(e -> Throwables.getStackTraceAsString(e).contains(embeddedExceptionMessage)));

  }
}

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
import org.apache.druid.server.QueryResultPusher.ResultsWriter;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.easymock.IArgumentMatcher;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reportMatcher;
import static org.easymock.EasyMock.verify;

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
  public void testResultPusherRetainsNestedExceptionBacktraces() throws Exception
  {

    HttpServletRequest request = new MockHttpServletRequest();
    ObjectMapper jsonMapper = new DefaultObjectMapper();
    ResponseContextConfig responseContextConfig = ResponseContextConfig.newConfig(true);
    DruidNode selfNode = DRUID_NODE;
    QueryResource.QueryMetricCounter counter = mock(QueryResource.QueryMetricCounter.class);
    String queryId = "someQuery";
    MediaType contentType = MediaType.APPLICATION_JSON_TYPE;
    Map<String, String> extraHeaders = new HashMap<String, String>();

    ResultsWriter resultWriter = mock(ResultsWriter.class);
    @SuppressWarnings("unchecked")
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

    expect(resultWriter.start()).andThrow(topException);

    resultWriter.recordFailure(exceptionBacktraceMessageContains(embeddedExceptionMessage));
    expectLastCall();
    resultWriter.close();
    expectLastCall();

    replay(resultWriter);

    // run pusher
    pusher.push();

    verify(resultWriter);
  }

  private Exception exceptionBacktraceMessageContains(final String message)
  {
    reportMatcher(new IArgumentMatcher()
    {
      @Override
      public boolean matches(Object argument)
      {
        return Throwables.getStackTraceAsString((Throwable) argument).contains(message);
      }

      @Override
      public void appendTo(StringBuffer buffer)
      {
        buffer.append("exceptionBacktraceMessageContains(\"" + message + "\")");
      }
    });
    return null;
  }
}

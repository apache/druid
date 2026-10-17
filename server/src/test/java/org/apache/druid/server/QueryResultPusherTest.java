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

import com.google.common.base.Throwables;
import org.apache.druid.common.exception.ErrorResponseTransformStrategy;
import org.apache.druid.common.exception.NoErrorResponseTransformStrategy;
import org.apache.druid.common.exception.PersonaBasedErrorTransformStrategy;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.error.QueryExceptionCompat;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.server.QueryResource.QueryMetricCounter;
import org.apache.druid.server.QueryResultPusher.ResultsWriter;
import org.apache.druid.server.QueryResultPusher.Writer;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

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
  private static final String QUERY_ID = "someQuery";
  private static final DruidExceptionMatcher HIDDEN_ERROR =
      new DruidExceptionMatcher(DruidException.Persona.USER, DruidException.Category.RUNTIME_FAILURE, "general")
          .expectMessageIs(
              "Internal server error, please contact your administrator with Error ID [" + QUERY_ID
              + "] if the issue persists."
          );

  @Test
  public void testResultPusherRetainsNestedExceptionBacktraces()
  {
    final String embeddedExceptionMessage = "Embedded Exception Message!";
    final RuntimeException topException =
        new RuntimeException("Where's the party?", new RuntimeException(embeddedExceptionMessage));
    final AtomicReference<Exception> recordedFailure = new AtomicReference<>();

    makeFailingPusher(topException, recordedFailure, NoErrorResponseTransformStrategy.INSTANCE).push();

    Assertions.assertNotNull(recordedFailure.get(), "recordFailure(e) should have been invoked!");
    Assertions.assertTrue(Throwables.getStackTraceAsString(recordedFailure.get()).contains(embeddedExceptionMessage));
  }

  @Test
  public void testExecutionFailureIsTransformedForClient()
  {
    final DruidException original = DruidException.forPersona(DruidException.Persona.OPERATOR)
                                                  .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                                                  .build("internal detail");
    final AtomicReference<Exception> recordedFailure = new AtomicReference<>();

    final Response response =
        makeFailingPusher(original, recordedFailure, PersonaBasedErrorTransformStrategy.INSTANCE).push();

    Assertions.assertEquals(500, response.getStatus());
    DruidExceptionMatcher.assertThat(((ErrorResponse) response.getEntity()).getUnderlyingException(), HIDDEN_ERROR);
    Assertions.assertSame(original, recordedFailure.get());
  }

  @Test
  public void testUnexpectedExecutionFailureIsTransformedForClient()
  {
    final Response response = makeFailingPusher(
        new IllegalStateException("internal detail"),
        new AtomicReference<>(),
        PersonaBasedErrorTransformStrategy.INSTANCE
    ).push();

    Assertions.assertEquals(500, response.getStatus());
    DruidExceptionMatcher.assertThat(((ErrorResponse) response.getEntity()).getUnderlyingException(), HIDDEN_ERROR);
  }

  @Test
  public void testExecutionTimeoutIsNotTransformed()
  {
    final Response response = makeFailingPusher(
        new QueryTimeoutException("Query timed out"),
        new AtomicReference<>(),
        PersonaBasedErrorTransformStrategy.INSTANCE
    ).push();

    Assertions.assertEquals(504, response.getStatus());
    DruidExceptionMatcher.assertThat(
        ((ErrorResponse) response.getEntity()).getUnderlyingException(),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.TIMEOUT,
            QueryExceptionCompat.ERROR_CODE
        ).expectMessageIs("Query timed out")
    );
  }

  private static QueryResultPusher makeFailingPusher(
      final RuntimeException failure,
      final AtomicReference<Exception> recordedFailure,
      final ErrorResponseTransformStrategy strategy
  )
  {
    final ResultsWriter resultsWriter = new ResultsWriter()
    {
      @Override
      public void close()
      {
      }

      @Override
      public ResponseBuilder start()
      {
        throw failure;
      }

      @Override
      public void recordSuccess(long numBytes)
      {
      }

      @Override
      public void recordFailure(Exception e, long bytesWritten)
      {
        recordedFailure.set(e);
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

    return new QueryResultPusher(
        new MockHttpServletRequest(),
        new DefaultObjectMapper(),
        ResponseContextConfig.newConfig(true),
        DRUID_NODE,
        new NoopQueryMetricCounter(),
        QUERY_ID,
        MediaType.APPLICATION_JSON_TYPE,
        new HashMap<>(),
        Collections.emptyMap(),
        strategy
    )
    {
      @Override
      public void writeException(Exception e, OutputStream out)
      {
      }

      @Override
      public ResultsWriter start()
      {
        return resultsWriter;
      }
    };
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

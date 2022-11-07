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

package org.apache.druid.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@RunWith(Enclosed.class)
public class JsonParserIteratorTest
{
  private static final JavaType JAVA_TYPE = Mockito.mock(JavaType.class);
  private static final String URL = "url";
  private static final String HOST = "host";
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @SuppressWarnings("ResultOfMethodCallIgnored")
  public static class FutureExceptionTest
  {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testConvertFutureTimeoutToQueryTimeoutException()
    {
      JsonParserIterator<Object> iterator = new JsonParserIterator<>(
          JAVA_TYPE,
          Futures.immediateFailedFuture(
              new QueryException(
                  QueryTimeoutException.ERROR_CODE,
                  "timeout exception conversion test",
                  null,
                  HOST
              )
          ),
          URL,
          null,
          HOST,
          OBJECT_MAPPER
      );
      expectedException.expect(QueryTimeoutException.class);
      expectedException.expectMessage("timeout exception conversion test");
      iterator.hasNext();
    }

    @Test
    public void testConvertFutureCancellationToQueryInterruptedException()
    {
      JsonParserIterator<Object> iterator = new JsonParserIterator<>(
          JAVA_TYPE,
          Futures.immediateCancelledFuture(),
          URL,
          null,
          HOST,
          OBJECT_MAPPER
      );
      expectedException.expect(QueryInterruptedException.class);
      expectedException.expectMessage("Immediate cancelled future.");
      iterator.hasNext();
    }

    @Test
    public void testConvertFutureInterruptedToQueryInterruptedException()
    {
      JsonParserIterator<Object> iterator = new JsonParserIterator<>(
          JAVA_TYPE,
          Futures.immediateFailedFuture(new InterruptedException("interrupted future")),
          URL,
          null,
          HOST,
          OBJECT_MAPPER
      );
      expectedException.expect(QueryInterruptedException.class);
      expectedException.expectMessage("interrupted future");
      iterator.hasNext();
    }

    @Test
    public void testConvertIOExceptionToQueryInterruptedException() throws IOException
    {
      InputStream exceptionThrowingStream = Mockito.mock(InputStream.class);
      IOException ioException = new IOException("ioexception test");
      Mockito.when(exceptionThrowingStream.read()).thenThrow(ioException);
      Mockito.when(exceptionThrowingStream.read(ArgumentMatchers.any())).thenThrow(ioException);
      Mockito.when(
          exceptionThrowingStream.read(ArgumentMatchers.any(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt())
      ).thenThrow(ioException);
      JsonParserIterator<Object> iterator = new JsonParserIterator<>(
          JAVA_TYPE,
          Futures.immediateFuture(exceptionThrowingStream),
          URL,
          null,
          HOST,
          OBJECT_MAPPER
      );
      expectedException.expect(QueryInterruptedException.class);
      expectedException.expectMessage("ioexception test");
      iterator.hasNext();
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @RunWith(Parameterized.class)
  public static class NonQueryInterruptedExceptionRestoreTest
  {
    @Parameters(name = "{0}")
    public static Iterable<Object[]> constructorFeeder()
    {
      return ImmutableList.of(
          new Object[]{new QueryTimeoutException()},
          new Object[]{
              QueryCapacityExceededException.withErrorMessageAndResolvedHost("capacity exceeded exception test")
          },
          new Object[]{new QueryUnsupportedException("unsupported exception test")},
          new Object[]{new ResourceLimitExceededException("resource limit exceeded exception test")}
      );
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final Exception exception;

    public NonQueryInterruptedExceptionRestoreTest(Exception exception)
    {
      this.exception = exception;
    }

    @Test
    public void testRestoreException() throws JsonProcessingException
    {
      JsonParserIterator<Object> iterator = new JsonParserIterator<>(
          JAVA_TYPE,
          Futures.immediateFuture(mockErrorResponse(exception)),
          URL,
          null,
          HOST,
          OBJECT_MAPPER
      );
      expectedException.expect(exception.getClass());
      expectedException.expectMessage(exception.getMessage());
      iterator.hasNext();
    }
  }

  public static class QueryInterruptedExceptionConversionTest
  {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testConvertQueryExceptionWithNullErrorCodeToQueryInterruptedException() throws JsonProcessingException
    {
      JsonParserIterator<Object> iterator = new JsonParserIterator<>(
          JAVA_TYPE,
          Futures.immediateFuture(mockErrorResponse(new QueryException(null, "query exception test", null, null))),
          URL,
          null,
          HOST,
          OBJECT_MAPPER
      );
      expectedException.expect(QueryInterruptedException.class);
      expectedException.expectMessage("query exception test");
      iterator.hasNext();
    }

    @Test
    public void testConvertQueryExceptionWithNonNullErrorCodeToQueryInterruptedException()
        throws JsonProcessingException
    {
      JsonParserIterator<Object> iterator = new JsonParserIterator<>(
          JAVA_TYPE,
          Futures.immediateFuture(
              mockErrorResponse(new QueryException("test error", "query exception test", null, null))
          ),
          URL,
          null,
          HOST,
          OBJECT_MAPPER
      );
      expectedException.expect(QueryInterruptedException.class);
      expectedException.expectMessage("query exception test");
      iterator.hasNext();
    }
  }

  public static class TimeoutExceptionConversionTest
  {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testTimeoutBeforeCallingFuture()
    {
      JsonParserIterator<?> iterator = new JsonParserIterator<>(
          JAVA_TYPE,
          Mockito.mock(Future.class),
          URL,
          mockQuery("qid", 0L), // should always timeout
          HOST,
          OBJECT_MAPPER
      );
      expectedException.expect(QueryTimeoutException.class);
      expectedException.expectMessage(StringUtils.format("url[%s] timed out", URL));
      iterator.hasNext();
    }

    @Test
    public void testTimeoutWhileCallingFuture()
    {
      Future<InputStream> future = new AbstractFuture<InputStream>()
      {
        @Override
        public InputStream get(long timeout, TimeUnit unit)
            throws InterruptedException
        {
          Thread.sleep(2000); // Sleep longer than timeout
          return null; // should return null so that JsonParserIterator checks timeout
        }
      };
      JsonParserIterator<?> iterator = new JsonParserIterator<>(
          JAVA_TYPE,
          future,
          URL,
          mockQuery("qid", System.currentTimeMillis() + 500L), // timeout in 500 ms
          HOST,
          OBJECT_MAPPER
      );
      expectedException.expect(QueryTimeoutException.class);
      expectedException.expectMessage(StringUtils.format("url[%s] timed out", URL));
      iterator.hasNext();
    }

    @Test
    public void testTimeoutAfterCallingFuture()
    {
      ExecutorService service = Execs.singleThreaded("timeout-test");
      try {
        JsonParserIterator<?> iterator = new JsonParserIterator<>(
            JAVA_TYPE,
            service.submit(() -> {
              Thread.sleep(2000); // Sleep longer than timeout
              return null;
            }),
            URL,
            mockQuery("qid", System.currentTimeMillis() + 500L), // timeout in 500 ms
            HOST,
            OBJECT_MAPPER
        );
        expectedException.expect(QueryTimeoutException.class);
        expectedException.expectMessage("Query [qid] timed out");
        iterator.hasNext();

      }
      finally {
        service.shutdownNow();
      }
    }

    private Query<?> mockQuery(String queryId, long timeoutAt)
    {
      Query<?> query = Mockito.mock(Query.class);
      QueryContext context = Mockito.mock(QueryContext.class);
      Mockito.when(query.getId()).thenReturn(queryId);
      Mockito.when(query.context()).thenReturn(
          QueryContext.of(ImmutableMap.of(DirectDruidClient.QUERY_FAIL_TIME, timeoutAt)));
      return query;
    }
  }

  private static InputStream mockErrorResponse(Exception e) throws JsonProcessingException
  {
    return new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsBytes(e));
  }
}

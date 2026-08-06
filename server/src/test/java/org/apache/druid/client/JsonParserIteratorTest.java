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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonParserIteratorTest
{
  private static final JavaType JAVA_TYPE = Mockito.mock(JavaType.class);
  private static final String URL = "url";
  private static final String HOST = "host";
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @SuppressWarnings("ResultOfMethodCallIgnored")
  public static class FutureExceptionTest
  {

    @Test
    public void testConvertFutureTimeoutToQueryTimeoutException()
    {
      Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(QueryTimeoutException.class, () -> {
        JsonParserIterator<Object> iterator = new JsonParserIterator<>(
            JAVA_TYPE,
            Futures.immediateFailedFuture(
                new QueryException(
                    QueryException.QUERY_TIMEOUT_ERROR_CODE,
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
        iterator.hasNext();
      });
      assertTrue(exception.getMessage().contains("timeout exception conversion test"));
    }

    @Test
    public void testConvertFutureCancellationToQueryInterruptedException()
    {
      Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(QueryInterruptedException.class, () -> {
        JsonParserIterator<Object> iterator = new JsonParserIterator<>(
            JAVA_TYPE,
            Futures.immediateCancelledFuture(),
            URL,
            null,
            HOST,
            OBJECT_MAPPER
        );
        iterator.hasNext();
      });
      assertTrue(exception.getMessage().contains("Task was cancelled."));
    }

    @Test
    public void testConvertFutureInterruptedToQueryInterruptedException()
    {
      Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(QueryInterruptedException.class, () -> {
        JsonParserIterator<Object> iterator = new JsonParserIterator<>(
            JAVA_TYPE,
            Futures.immediateFailedFuture(new InterruptedException("interrupted future")),
            URL,
            null,
            HOST,
            OBJECT_MAPPER
        );
        iterator.hasNext();
      });
      assertTrue(exception.getMessage().contains("interrupted future"));
    }

    @Test
    public void testConvertIOExceptionToQueryInterruptedException()
    {
      Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(QueryInterruptedException.class, () -> {
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
        iterator.hasNext();
      });
      assertTrue(exception.getMessage().contains("ioexception test"));
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  public static class NonQueryInterruptedExceptionRestoreTest
  {
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

    private Exception exception;

    public void initNonQueryInterruptedExceptionRestoreTest(Exception exception)
    {
      this.exception = exception;
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "{0}")
    public void testRestoreException(Exception exception)
    {
      Throwable thrown = org.junit.jupiter.api.Assertions.assertThrows(exception.getClass(), () -> {
        initNonQueryInterruptedExceptionRestoreTest(exception);
        JsonParserIterator<Object> iterator = new JsonParserIterator<>(
            JAVA_TYPE,
            Futures.immediateFuture(mockErrorResponse(exception)),
            URL,
            null,
            HOST,
            OBJECT_MAPPER
        );
        iterator.hasNext();
      });
      assertTrue(thrown.getMessage().contains(exception.getMessage()));
    }
  }

  public static class QueryInterruptedExceptionConversionTest
  {

    @Test
    public void testConvertQueryExceptionWithNullErrorCodeToQueryInterruptedException()
    {
      Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(QueryInterruptedException.class, () -> {
        JsonParserIterator<Object> iterator = new JsonParserIterator<>(
            JAVA_TYPE,
            Futures.immediateFuture(mockErrorResponse(new QueryException(null, "query exception test", null, null))),
            URL,
            null,
            HOST,
            OBJECT_MAPPER
        );
        iterator.hasNext();
      });
      assertTrue(exception.getMessage().contains("query exception test"));
    }

    @Test
    public void testConvertQueryExceptionWithNonNullErrorCodeToQueryInterruptedException()
    {
      Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(QueryInterruptedException.class, () -> {
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
        iterator.hasNext();
      });
      assertTrue(exception.getMessage().contains("query exception test"));
    }
  }

  public static class TimeoutExceptionConversionTest
  {

    @Test
    public void testTimeoutBeforeCallingFuture()
    {
      Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(QueryTimeoutException.class, () -> {
        JsonParserIterator<?> iterator = new JsonParserIterator<>(
            JAVA_TYPE,
            Mockito.mock(Future.class),
            URL,
            mockQuery("qid", 0L), // should always timeout
            HOST,
            OBJECT_MAPPER
        );
        iterator.hasNext();
      });
      assertTrue(exception.getMessage().contains(StringUtils.format("url[%s] timed out", URL)));
    }

    @Test
    public void testTimeoutWhileCallingFuture()
    {
      Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(QueryTimeoutException.class, () -> {
        Future<InputStream> future = new AbstractFuture<>()
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
        iterator.hasNext();
      });
      assertTrue(exception.getMessage().contains(StringUtils.format("url[%s] timed out", URL)));
    }

    @Test
    public void testTimeoutAfterCallingFuture()
    {
      Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(QueryTimeoutException.class, () -> {
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
          iterator.hasNext();

        }
        finally {
          service.shutdownNow();
        }
      });
      assertTrue(exception.getMessage().contains("Query [qid] timed out"));
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

  public static class IAEExceptionConversionTest
  {

    private String errorMessage = "pstream connect error or disconnect/reset before header";
    private String nullErrMsg = null;

    @Test
    public void testNullErrorMsg()
    {
      Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(QueryInterruptedException.class, () -> {
        JsonParserIterator<Object> iterator = new JsonParserIterator<>(
            JAVA_TYPE,
            Futures.immediateFuture(
                mockErrorResponse(nullErrMsg)
            ),
            URL,
            null,
            HOST,
            OBJECT_MAPPER
        );
        iterator.hasNext();
      });
      assertTrue(exception.getMessage().contains(""));
    }

    @Test
    public void testParsingError()
    {
      Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(QueryInterruptedException.class, () -> {
        JsonParserIterator<Object> iterator = new JsonParserIterator<>(
            JAVA_TYPE,
            Futures.immediateFuture(
                mockErrorResponse(errorMessage)
            ),
            URL,
            null,
            HOST,
            OBJECT_MAPPER
        );
        iterator.hasNext();
      });
      assertTrue(exception.getMessage().contains(errorMessage));
    }
  }

  private static InputStream mockErrorResponse(Exception e) throws JsonProcessingException
  {
    return new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsBytes(e));
  }

  private static InputStream mockErrorResponse(String errMsg) throws JsonProcessingException
  {
    return new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsBytes(errMsg));
  }
}

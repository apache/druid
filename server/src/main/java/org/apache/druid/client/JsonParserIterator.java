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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class JsonParserIterator<T> implements Iterator<T>, Closeable
{
  private static final Logger LOG = new Logger(JsonParserIterator.class);

  private JsonParser jp;
  private ObjectCodec objectCodec;
  private final JavaType typeRef;
  private final Future<InputStream> future;
  private final String url;
  private final String host;
  private final ObjectMapper objectMapper;
  private final boolean hasTimeout;
  private final long timeoutAt;
  private final String queryId;

  public JsonParserIterator(
      JavaType typeRef,
      Future<InputStream> future,
      String url,
      @Nullable Query<T> query,
      String host,
      ObjectMapper objectMapper
  )
  {
    this.typeRef = typeRef;
    this.future = future;
    this.url = url;
    if (query != null) {
      this.timeoutAt = query.<Long>getContextValue(DirectDruidClient.QUERY_FAIL_TIME, -1L);
      this.queryId = query.getId();
    } else {
      this.timeoutAt = -1;
      this.queryId = null;
    }
    this.jp = null;
    this.host = host;
    this.objectMapper = objectMapper;
    this.hasTimeout = timeoutAt > -1;
  }

  @Override
  public boolean hasNext()
  {
    init();

    if (jp.isClosed()) {
      return false;
    }
    if (jp.getCurrentToken() == JsonToken.END_ARRAY) {
      CloseQuietly.close(jp);
      return false;
    }

    return true;
  }

  @Override
  public T next()
  {
    init();

    try {
      final T retVal = objectCodec.readValue(jp, typeRef);
      jp.nextToken();
      return retVal;
    }
    catch (IOException e) {
      // check for timeout, a failure here might be related to a timeout, so lets just attribute it
      if (checkTimeout()) {
        QueryTimeoutException timeoutException = timeoutQuery();
        timeoutException.addSuppressed(e);
        throw timeoutException;
      } else {
        throw convertException(e);
      }
    }
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException
  {
    if (jp != null) {
      jp.close();
    }
  }

  private boolean checkTimeout()
  {
    long timeLeftMillis = timeoutAt - System.currentTimeMillis();
    return checkTimeout(timeLeftMillis);
  }

  private boolean checkTimeout(long timeLeftMillis)
  {
    if (hasTimeout && timeLeftMillis < 1) {
      return true;
    }
    return false;
  }

  private void init()
  {
    if (jp == null) {
      try {
        long timeLeftMillis = timeoutAt - System.currentTimeMillis();
        if (checkTimeout(timeLeftMillis)) {
          throw timeoutQuery();
        }
        InputStream is = hasTimeout ? future.get(timeLeftMillis, TimeUnit.MILLISECONDS) : future.get();

        if (is != null) {
          jp = objectMapper.getFactory().createParser(is);
        } else if (checkTimeout()) {
          throw timeoutQuery();
        } else {
          // TODO: NettyHttpClient should check the actual cause of the failure and set it in the future properly.
          throw new ResourceLimitExceededException(
              "Possibly max scatter-gather bytes limit reached while reading from url[%s].",
              url
          );
        }

        final JsonToken nextToken = jp.nextToken();
        if (nextToken == JsonToken.START_ARRAY) {
          jp.nextToken();
          objectCodec = jp.getCodec();
        } else if (nextToken == JsonToken.START_OBJECT) {
          throw convertException(jp.getCodec().readValue(jp, QueryException.class));
        } else {
          throw convertException(
              new IAE("Next token wasn't a START_ARRAY, was[%s] from url[%s]", jp.getCurrentToken(), url)
          );
        }
      }
      catch (IOException | InterruptedException | ExecutionException | CancellationException e) {
        throw convertException(e);
      }
      catch (TimeoutException e) {
        throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query [%s] timed out!", queryId), host);
      }
    }
  }

  private QueryTimeoutException timeoutQuery()
  {
    return new QueryTimeoutException(StringUtils.nonStrictFormat("url[%s] timed out", url), host);
  }

  /**
   * Converts the given exception to a proper type of {@link QueryException}.
   * The use cases of this method are:
   *
   * - All non-QueryExceptions are wrapped with {@link QueryInterruptedException}.
   * - The QueryException from {@link DirectDruidClient} is converted to a more specific type of QueryException
   *   based on {@link QueryException#getErrorCode()}. During conversion, {@link QueryException#host} is overridden
   *   by {@link #host}.
   */
  private QueryException convertException(Exception cause)
  {
    LOG.warn(cause, "Query [%s] to host [%s] interrupted", queryId, host);
    if (cause instanceof QueryException) {
      final QueryException queryException = (QueryException) cause;
      if (queryException.getErrorCode() == null) {
        // errorCode should not be null now, but maybe could be null in the past..
        return new QueryInterruptedException(
            queryException.getErrorCode(),
            queryException.getMessage(),
            queryException.getErrorClass(),
            host
        );
      }

      // Note: this switch clause is to restore the 'type' information of QueryExceptions which is lost during
      // JSON serialization. This is not a good way to restore the correct exception type. Rather, QueryException
      // should store its type when it is serialized, so that we can know the exact type when it is deserialized.
      switch (queryException.getErrorCode()) {
        // The below is the list of exceptions that can be thrown in historicals and propagated to the broker.
        case QueryTimeoutException.ERROR_CODE:
          return new QueryTimeoutException(
              queryException.getErrorCode(),
              queryException.getMessage(),
              queryException.getErrorClass(),
              host
          );
        case QueryCapacityExceededException.ERROR_CODE:
          return new QueryCapacityExceededException(
              queryException.getErrorCode(),
              queryException.getMessage(),
              queryException.getErrorClass(),
              host
          );
        case QueryUnsupportedException.ERROR_CODE:
          return new QueryUnsupportedException(
              queryException.getErrorCode(),
              queryException.getMessage(),
              queryException.getErrorClass(),
              host
          );
        case ResourceLimitExceededException.ERROR_CODE:
          return new ResourceLimitExceededException(
              queryException.getErrorCode(),
              queryException.getMessage(),
              queryException.getErrorClass(),
              host
          );
        default:
          return new QueryInterruptedException(
              queryException.getErrorCode(),
              queryException.getMessage(),
              queryException.getErrorClass(),
              host
          );
      }
    } else {
      return new QueryInterruptedException(cause, host);
    }
  }
}

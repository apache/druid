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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.concurrent.CancellationException;

/**
 * Exception representing a failed query. The name "QueryInterruptedException" is a misnomer; this is actually
 * used on the client side for *all* kinds of failed queries.
 * <p>
 * Fields:
 * - "errorCode" is a well-defined errorCode code taken from a specific list (see the static constants). "Unknown exception"
 * represents all wrapped exceptions other than interrupt, cancellation, resource limit exceeded, unauthorized
 * request, and unsupported operation.
 * - "errorMessage" is the toString of the wrapped exception
 * - "errorClass" is the class of the wrapped exception
 * - "host" is the host that the errorCode occurred on
 * <p>
 * The QueryResource is expected to emit the JSON form of this object when errors happen, and the DirectDruidClient
 * deserializes and wraps them.
 */
public class QueryInterruptedException extends QueryException
{
  @JsonCreator
  public QueryInterruptedException(
      @JsonProperty("error") @Nullable String errorCode,
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("errorClass") @Nullable String errorClass,
      @JsonProperty("host") @Nullable String host
  )
  {
    super(errorCode, errorMessage, errorClass, host);
  }

  /**
   * Creates a new QueryInterruptedException wrapping an underlying exception. The errorMessage and errorClass
   * of this exception will be based on the highest non-QueryInterruptedException in the causality chain.
   *
   * @param cause wrapped exception
   */
  public QueryInterruptedException(Throwable cause)
  {
    this(cause, getHostFromThrowable(cause));
  }

  public QueryInterruptedException(Throwable cause, String host)
  {
    super(cause, getErrorCodeFromThrowable(cause), getErrorClassFromThrowable(cause), host);
  }

  private static String getErrorCodeFromThrowable(Throwable e)
  {
    if (e instanceof QueryInterruptedException) {
      return ((QueryInterruptedException) e).getErrorCode();
    } else if (e instanceof InterruptedException) {
      return QUERY_INTERRUPTED_ERROR_CODE;
    } else if (e instanceof CancellationException) {
      return QUERY_CANCELED_ERROR_CODE;
    } else if (e instanceof UnsupportedOperationException) {
      return UNSUPPORTED_OPERATION_ERROR_CODE;
    } else if (e instanceof TruncatedResponseContextException) {
      return TRUNCATED_RESPONSE_CONTEXT_ERROR_CODE;
    } else {
      return UNKNOWN_EXCEPTION_ERROR_CODE;
    }
  }

  @Nullable
  private static String getErrorClassFromThrowable(Throwable e)
  {
    if (e instanceof QueryInterruptedException) {
      return ((QueryInterruptedException) e).getErrorClass();
    } else if (e != null) {
      return e.getClass().getName();
    } else {
      return null;
    }
  }

  @Nullable
  private static String getHostFromThrowable(Throwable e)
  {
    if (e instanceof QueryInterruptedException) {
      return ((QueryInterruptedException) e).getHost();
    } else {
      return null;
    }
  }

  public static QueryInterruptedException wrapIfNeeded(Throwable e)
  {
    return e instanceof QueryInterruptedException ? (QueryInterruptedException) e : new QueryInterruptedException(e);
  }
}

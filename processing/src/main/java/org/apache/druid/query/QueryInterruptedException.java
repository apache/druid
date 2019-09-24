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
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

/**
 * Exception representing a failed query. The name "QueryInterruptedException" is a misnomer; this is actually
 * used on the client side for *all* kinds of failed queries.
 *
 * Fields:
 * - "errorCode" is a well-defined errorCode code taken from a specific list (see the static constants). "Unknown exception"
 * represents all wrapped exceptions other than interrupt, timeout, cancellation, resource limit exceeded, unauthorized
 * request, and unsupported operation.
 * - "errorMessage" is the toString of the wrapped exception
 * - "errorClass" is the class of the wrapped exception
 * - "host" is the host that the errorCode occurred on
 *
 * The QueryResource is expected to emit the JSON form of this object when errors happen, and the DirectDruidClient
 * deserializes and wraps them.
 */
public class QueryInterruptedException extends RuntimeException
{
  public static final String QUERY_INTERRUPTED = "Query interrupted";
  public static final String QUERY_TIMEOUT = "Query timeout";
  public static final String QUERY_CANCELLED = "Query cancelled";
  public static final String RESOURCE_LIMIT_EXCEEDED = "Resource limit exceeded";
  public static final String UNAUTHORIZED = "Unauthorized request.";
  public static final String UNSUPPORTED_OPERATION = "Unsupported operation";
  public static final String UNKNOWN_EXCEPTION = "Unknown exception";

  private final String errorCode;
  private final String errorClass;
  private final String host;

  @JsonCreator
  public QueryInterruptedException(
      @JsonProperty("error") @Nullable String errorCode,
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("errorClass") @Nullable String errorClass,
      @JsonProperty("host") @Nullable String host
  )
  {
    super(errorMessage);
    this.errorCode = errorCode;
    this.errorClass = errorClass;
    this.host = host;
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
    super(cause == null ? null : cause.getMessage(), cause);
    this.errorCode = getErrorCodeFromThrowable(cause);
    this.errorClass = getErrorClassFromThrowable(cause);
    this.host = host;
  }

  @Nullable
  @JsonProperty("error")
  public String getErrorCode()
  {
    return errorCode;
  }

  @JsonProperty("errorMessage")
  @Override
  public String getMessage()
  {
    return super.getMessage();
  }

  @JsonProperty
  public String getErrorClass()
  {
    return errorClass;
  }

  @JsonProperty
  public String getHost()
  {
    return host;
  }

  @Override
  public String toString()
  {
    return StringUtils.format(
        "QueryInterruptedException{msg=%s, code=%s, class=%s, host=%s}",
        getMessage(),
        errorCode,
        errorClass,
        host
    );
  }

  private static String getErrorCodeFromThrowable(Throwable e)
  {
    if (e instanceof QueryInterruptedException) {
      return ((QueryInterruptedException) e).getErrorCode();
    } else if (e instanceof InterruptedException) {
      return QUERY_INTERRUPTED;
    } else if (e instanceof CancellationException) {
      return QUERY_CANCELLED;
    } else if (e instanceof TimeoutException) {
      return QUERY_TIMEOUT;
    } else if (e instanceof ResourceLimitExceededException) {
      return RESOURCE_LIMIT_EXCEEDED;
    } else if (e instanceof UnsupportedOperationException) {
      return UNSUPPORTED_OPERATION;
    } else {
      return UNKNOWN_EXCEPTION;
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

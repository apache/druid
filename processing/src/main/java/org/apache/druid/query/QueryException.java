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
import org.apache.druid.common.exception.SanitizableException;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.net.InetAddress;
import java.util.function.Function;

/**
 * Base serializable error response.
 * <p>
 * The Object Model that QueryException follows is a little non-intuitive as the primary way that a QueryException is
 * generated is through a child class.  However, those child classes are *not* equivalent to a QueryException, instead
 * they act as a Factory of QueryException objects.  This can be seen in two different places.
 * <p>
 * 1. When sanitize() is called, the response is a QueryException without any indication of which original exception
 * occurred.
 * 2. When these objects get serialized across the wire the recipient deserializes a QueryException. The client is
 * never expected, and fundamentally is not allowed to, ever deserialize a child class of QueryException.
 * <p>
 * For this reason, QueryException must contain all potential state that any of its child classes could ever want to
 * push across the wire.  Additionally, any catch clauses expecting one of the child Exceptions must know that it is
 * running inside of code where the exception has not traveled across the wire.  If there is a chance that the
 * exception could have been serialized across the wire, the code must catch a QueryException and check the errorCode
 * instead.
 * <p>
 * As a corollary, adding new state or adjusting the logic of this class must always be done in a backwards-compatible
 * fashion across all child classes of QueryException.
 * <p>
 * If there is any need to do different logic based on the type of error that has happened, the only reliable method
 * of discerning the type of the error is to look at the errorCode String.  Because these Strings are considered part
 * of the API, they are not allowed to change and must maintain their same semantics.  The known errorCode Strings
 * are pulled together as public static fields on this class in order to make it more clear what options exist.
 * <p>
 * QueryResource and SqlResource are expected to emit the JSON form of this object when errors happen.
 */
public class QueryException extends RuntimeException implements SanitizableException
{
  /**
   * Error codes
   */
  public static final String JSON_PARSE_ERROR_CODE = "Json parse failed";
  public static final String BAD_QUERY_CONTEXT_ERROR_CODE = "Query context parse failed";
  public static final String QUERY_CAPACITY_EXCEEDED_ERROR_CODE = "Query capacity exceeded";
  public static final String QUERY_INTERRUPTED_ERROR_CODE = "Query interrupted";
  // Note: the proper spelling is with a single "l", but the version with
  // two "l"s is documented, we can't change the text of the message.
  public static final String QUERY_CANCELED_ERROR_CODE = "Query cancelled";
  public static final String UNAUTHORIZED_ERROR_CODE = "Unauthorized request";
  public static final String UNSUPPORTED_OPERATION_ERROR_CODE = "Unsupported operation";
  public static final String TRUNCATED_RESPONSE_CONTEXT_ERROR_CODE = "Truncated response context";
  public static final String UNKNOWN_EXCEPTION_ERROR_CODE = "Unknown exception";
  public static final String QUERY_TIMEOUT_ERROR_CODE = "Query timeout";
  public static final String QUERY_UNSUPPORTED_ERROR_CODE = "Unsupported query";
  public static final String RESOURCE_LIMIT_EXCEEDED_ERROR_CODE = "Resource limit exceeded";
  public static final String SQL_PARSE_FAILED_ERROR_CODE = "SQL parse failed";
  public static final String PLAN_VALIDATION_FAILED_ERROR_CODE = "Plan validation failed";
  public static final String SQL_QUERY_UNSUPPORTED_ERROR_CODE = "SQL query is unsupported";

  public enum FailType
  {
    USER_ERROR(400),
    UNAUTHORIZED(401),
    CAPACITY_EXCEEDED(429),
    UNKNOWN(500),
    CANCELED(500),
    QUERY_RUNTIME_FAILURE(500),
    UNSUPPORTED(501),
    TIMEOUT(504);

    private final int expectedStatus;

    FailType(int expectedStatus)
    {
      this.expectedStatus = expectedStatus;
    }

    public int getExpectedStatus()
    {
      return expectedStatus;
    }
  }

  public static FailType fromErrorCode(String errorCode)
  {
    if (errorCode == null) {
      return FailType.UNKNOWN;
    }

    switch (errorCode) {
      case QUERY_CANCELED_ERROR_CODE:
        return FailType.CANCELED;

      // These error codes are generally expected to come from a QueryInterruptedException
      case QUERY_INTERRUPTED_ERROR_CODE:
      case UNSUPPORTED_OPERATION_ERROR_CODE:
      case UNKNOWN_EXCEPTION_ERROR_CODE:
      case TRUNCATED_RESPONSE_CONTEXT_ERROR_CODE:
        return FailType.QUERY_RUNTIME_FAILURE;
      case UNAUTHORIZED_ERROR_CODE:
        return FailType.UNAUTHORIZED;

      case QUERY_CAPACITY_EXCEEDED_ERROR_CODE:
        return FailType.CAPACITY_EXCEEDED;
      case QUERY_TIMEOUT_ERROR_CODE:
        return FailType.TIMEOUT;

      // These error codes are expected to come from BadQueryExceptions
      case JSON_PARSE_ERROR_CODE:
      case BAD_QUERY_CONTEXT_ERROR_CODE:
      case RESOURCE_LIMIT_EXCEEDED_ERROR_CODE:
        // And these ones from the SqlPlanningException which are also BadQueryExceptions
      case SQL_PARSE_FAILED_ERROR_CODE:
      case PLAN_VALIDATION_FAILED_ERROR_CODE:
      case SQL_QUERY_UNSUPPORTED_ERROR_CODE:
        return FailType.USER_ERROR;
      case QUERY_UNSUPPORTED_ERROR_CODE:
        return FailType.UNSUPPORTED;
      default:
        return FailType.UNKNOWN;
    }
  }

  /**
   * Implementation
   */
  private final String errorCode;
  private final String errorClass;
  private final String host;

  protected QueryException(Throwable cause, String errorCode, String errorClass, String host)
  {
    this(cause, errorCode, cause == null ? null : cause.getMessage(), errorClass, host);
  }

  protected QueryException(Throwable cause, String errorCode, String errorMessage, String errorClass, String host)
  {
    super(errorMessage, cause);
    this.errorCode = errorCode;
    this.errorClass = errorClass;
    this.host = host;
  }

  @JsonCreator
  public QueryException(
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

  @Nullable
  protected static String resolveHostname()
  {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    }
    catch (Exception e) {
      return null;
    }
  }

  @Override
  public QueryException sanitize(@NotNull Function<String, String> errorMessageTransformFunction)
  {
    return new QueryException(errorCode, errorMessageTransformFunction.apply(getMessage()), null, null);
  }

  public FailType getFailType()
  {
    return fromErrorCode(errorCode);
  }

  @Override
  public String toString()
  {
    return StringUtils.format(
        "%s{msg=%s, code=%s, class=%s, host=%s}",
        getClass().getSimpleName(),
        getMessage(),
        getErrorCode(),
        getErrorClass(),
        getHost()
    );
  }
}

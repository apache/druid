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

/**
 * This exception is thrown when a query does not finish before the configured query timeout.
 * {@link java.util.concurrent.TimeoutException} exceptions encountered during the lifecycle of a query
 * are rethrown as this exception.
 * <p>
 * As a {@link QueryException}, it is expected to be serialized to a json response, but will be mapped to
 * {@link #STATUS_CODE} instead of the default HTTP 500 status.
 */

public class QueryTimeoutException extends QueryException
{
  private static final String ERROR_CLASS = QueryTimeoutException.class.getName();
  public static final String ERROR_CODE = "Query timeout";
  public static final String ERROR_MESSAGE = "Query Timed Out!";
  public static final int STATUS_CODE = 504;

  @JsonCreator
  public QueryTimeoutException(
      @JsonProperty("error") @Nullable String errorCode,
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("errorClass") @Nullable String errorClass,
      @JsonProperty("host") @Nullable String host
  )
  {
    super(errorCode, errorMessage, errorClass, host);
  }

  public QueryTimeoutException()
  {
    super(ERROR_CODE, ERROR_MESSAGE, ERROR_CLASS, resolveHostname());
  }

  public QueryTimeoutException(String errorMessage)
  {
    super(ERROR_CODE, errorMessage, ERROR_CLASS, resolveHostname());
  }

  public QueryTimeoutException(String errorMessage, String host)
  {
    super(ERROR_CODE, errorMessage, ERROR_CLASS, host);
  }
}

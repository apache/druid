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

package org.apache.druid.error;

import org.apache.druid.query.QueryException;

import java.net.HttpURLConnection;

/**
 * The error category is a combination of the "persona" for the error, and the
 * kind of error. The "persona" is the audience for the error: who can fix the
 * problem. Often the persona is clear: it is the end user if, say, the user
 * asks us to do something that doesn't make sense. (Data source does not exist,
 * SQL with a syntax error, etc.) Other times, the persona is ambiguous: who is
 * responsible for a network error?
 * <p>
 * The persona is not fine enough grain for all needs. So, within a persona, there
 * can be finer-grain functional areas, such as the various kinds of SQL errors.
 * <p>
 * Different kinds of errors require different HTTP status codes for the REST API.
 * <p>
 * To add structure to this confusion of factors, we define a set of error categories:
 * one for each combination of functional area, user and HTTP status. The categories
 * are more an art than a science: they are influenced by the need of the consumers
 * of errors: especially managed Druid installations.
 */
public enum ErrorCategory
{
  SQL_PARSE(
      QueryException.SQL_PARSE_FAILED_ERROR_CODE,
      HttpURLConnection.HTTP_BAD_REQUEST,
      ErrorAudience.USER,
      "",
      MetricCategory.FAILED
  ),
  SQL_VALIDATION(
      QueryException.PLAN_VALIDATION_FAILED_ERROR_CODE,
      HttpURLConnection.HTTP_BAD_REQUEST,
      ErrorAudience.USER,
      "",
      MetricCategory.FAILED
  ),
  SQL_UNSUPPORTED(
      QueryException.SQL_QUERY_UNSUPPORTED_ERROR_CODE,
      HttpURLConnection.HTTP_BAD_REQUEST,
      ErrorAudience.USER,
      "",
      MetricCategory.FAILED
  ),
  INTERNAL(
      QueryException.UNSUPPORTED_OPERATION_ERROR_CODE,
      HttpURLConnection.HTTP_BAD_REQUEST,
      ErrorAudience.DRUID_DEVELOPER,
      "Internal error",
      MetricCategory.FAILED
  );

  private final String userText;
  private final int httpStatus;
  private final ErrorAudience audience;
  private final String messagePrefix;
  private final MetricCategory metricCategory;

  ErrorCategory(
      String userText,
      int httpStatus,
      ErrorAudience audience,
      String messagePrefix,
      MetricCategory metricCategory
  )
  {
    this.userText = userText;
    this.httpStatus = httpStatus;
    this.audience = audience;
    this.messagePrefix = messagePrefix;
    this.metricCategory = metricCategory;
  }

  public String userText()
  {
    return userText;
  }

  public int httpStatus()
  {
    return httpStatus;
  }

  public ErrorAudience audience()
  {
    return audience;
  }

  public String prefix()
  {
    return messagePrefix;
  }

  public MetricCategory metricCategory()
  {
    return metricCategory;
  }
}

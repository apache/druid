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
 * SQL query validation failed, because a SQL statement asked Druid to do
 * something which it does not support. This message indicates that the
 * unsupported thing is by design, not because we've not gotten to it yet.
 * For example, asking for `MAX(VARCHAR)` is not supported because it does
 * not make sense. Use a different exception if the error is due to something
 * that Druid should support, but doesn't yet.
 *
 * @see {@link SqlValidationError} for the general validation error case.
 */
public class SqlUnsupportedError extends DruidException
{
  public SqlUnsupportedError(
      String code,
      String message
  )
  {
    this(null, code, message);
  }

  public SqlUnsupportedError(
      Throwable cause,
      String code,
      String message
  )
  {
    super(
        cause,
        ErrorCode.fullCode(ErrorCode.SQL_UNSUPPORTED_GROUP, code),
        message
    );
    // For backward compatibility.
    // Calcite classes not visible here, so using a string
    this.legacyClass = "org.apache.calcite.plan.RelOptPlanner$CannotPlanException";
    this.legacyCode = QueryException.SQL_QUERY_UNSUPPORTED_ERROR_CODE;
  }

  @Override
  public ErrorAudience audience()
  {
    return ErrorAudience.USER;
  }

  @Override
  public int httpStatus()
  {
    return HttpURLConnection.HTTP_BAD_REQUEST;
  }

  public static DruidException unsupportedAggType(String agg, Object type)
  {
    return new SqlUnsupportedError(
            "InvalidAggArg",
            "${fn} aggregation is not supported for type [${type}]"
         )
        .withValue("fn", agg)
        .withValue("type", type);
  }

  public static DruidException cannotUseOperator(String op, Throwable cause)
  {
    throw new SqlUnsupportedError(
            "Operator",
            "Cannot use [${op}]: [${message}]"
         )
        .withValue("op", op)
        .withValue(DruidException.MESSAGE_KEY, cause.getMessage());
  }
}

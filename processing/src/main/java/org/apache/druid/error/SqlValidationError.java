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
 * SQL query validation failed, most likely due to a problem in the SQL statement
 * which the user provided.
 *
 * @see {@link SqlUnsupportedError} for the special case
 * in which the SQL asked to do something Druid does not support.
 */
public class SqlValidationError extends DruidException
{
  public SqlValidationError(
      String code,
      String message
  )
  {
    this(null, code, message);
  }

  public SqlValidationError(
      Throwable cause,
      String code,
      String message
  )
  {
    super(
        cause,
        ErrorCode.fullCode(ErrorCode.SQL_VALIDATION_GROUP, code),
        message
    );
    this.legacyClass = "org.apache.calcite.tools.ValidationException";
    this.legacyCode = QueryException.PLAN_VALIDATION_FAILED_ERROR_CODE;
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

  public static DruidException forCause(Throwable e)
  {
    return new SqlValidationError(
            ErrorCode.GENERAL_TAIL,
            SIMPLE_MESSAGE
         )
        .withValue(MESSAGE_KEY, e.getMessage());
  }
}

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
 * SQL query parse failed.
 */
public class SqlParseError extends DruidException
{
  public SqlParseError(
      String code,
      String message
  )
  {
    this(null, code, message);
  }

  public SqlParseError(
      Throwable cause,
      String code,
      String message
  )
  {
    super(
        cause,
        ErrorCode.fullCode(ErrorCode.SQL_PARSE_GROUP, code),
        fullMessage(message)
    );
    this.legacyCode = QueryException.SQL_PARSE_FAILED_ERROR_CODE;
    // For backward compatibility.
    // Calcite classes not visible here, so using a string
    this.legacyClass = "org.apache.calcite.sql.parser.SqlParseException";
  }

  public static String fullMessage(String message)
  {
    return "Line ${line}, column ${column}: " + message;
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
}

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

/**
 * SQL query parse failed.
 */
public class SqlParseError extends DruidException
{
  public SqlParseError(String msg, Object...args)
  {
    super(msg, args);
  }

  public SqlParseError(Throwable cause, String msg, Object...args)
  {
    super(cause, msg, args);
  }

  @Override
  public ErrorCategory category()
  {
    return ErrorCategory.SQL_PARSE;
  }

  @Override
  public String errorClass()
  {
    // For backward compatibility.
    // Calcite classes not visible here, so using a string
    return "org.apache.calcite.sql.parser.SqlParseException";
  }
}

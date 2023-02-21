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
 * SQL query validation failed, most likely due to a problem in the SQL statement
 * which the user provided.
 *
 * @see {@link SqlUnsupportedError} for the special case
 * in which the SQL asked to do something Druid does not support.
 */
public class SqlValidationError extends DruidException
{
  public SqlValidationError(String msg, Object...args)
  {
    super(msg, args);
  }

  public SqlValidationError(Throwable cause, String msg, Object...args)
  {
    super(cause, msg, args);
  }

  public SqlValidationError(Throwable cause)
  {
    super(cause, cause.getMessage());
  }

  @Override
  public ErrorCategory category()
  {
    return ErrorCategory.SQL_VALIDATION;
  }

  @Override
  public String errorClass()
  {
    // For backward compatibility.
    // Using string because the class is not visible here.
    return "org.apache.calcite.tools.ValidationException";
  }
}

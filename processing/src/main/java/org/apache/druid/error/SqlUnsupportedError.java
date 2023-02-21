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
  public SqlUnsupportedError(String msg, Object...args)
  {
    super(msg, args);
  }

  public SqlUnsupportedError(Throwable cause, String msg, Object...args)
  {
    super(cause, msg, args);
  }

  @Override
  public ErrorCategory category()
  {
    return ErrorCategory.SQL_UNSUPPORTED;
  }

  @Override
  public String errorClass()
  {
    // For backward compatibility: using text since class is not visible here.
    return "org.apache.calcite.plan.RelOptPlanner$CannotPlanException";
  }
}

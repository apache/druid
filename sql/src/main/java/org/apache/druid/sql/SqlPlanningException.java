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

package org.apache.druid.sql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.query.BadQueryException;

/**
 * An exception for SQL query planning failures.
 */
public class SqlPlanningException extends BadQueryException
{
  public enum PlanningError
  {
    SQL_PARSE_ERROR("SQL parse failed", SqlParseException.class.getName()),
    VALIDATION_ERROR("Plan validation failed", ValidationException.class.getName());

    private final String errorCode;
    private final String errorClass;

    PlanningError(String errorCode, String errorClass)
    {
      this.errorCode = errorCode;
      this.errorClass = errorClass;
    }

    public String getErrorCode()
    {
      return errorCode;
    }

    public String getErrorClass()
    {
      return errorClass;
    }
  }

  public SqlPlanningException(SqlParseException e)
  {
    this(PlanningError.SQL_PARSE_ERROR, e.getMessage());
  }

  public SqlPlanningException(ValidationException e)
  {
    this(PlanningError.VALIDATION_ERROR, e.getMessage());
  }

  private SqlPlanningException(PlanningError planningError, String errorMessage)
  {
    this(planningError.errorCode, errorMessage, planningError.errorClass);
  }

  @JsonCreator
  private SqlPlanningException(
      @JsonProperty("error") String errorCode,
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("errorClass") String errorClass
  )
  {
    super(errorCode, errorMessage, errorClass);
  }
}

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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.rel.type.RelDataType;

/**
 * After preparing an SQL query with {@link DruidPlanner}, the artifacts produced are the output signature of the
 * result row, and type information for any dynamic parameters which must be bound before planning the query.
 */
public class PrepareResult
{
  private final RelDataType validatedRowType;
  private final RelDataType returnedRowType;
  private final RelDataType parameterRowType;

  public PrepareResult(
      final RelDataType validatedRowType,
      final RelDataType returnedRowType,
      final RelDataType parameterRowType
  )
  {
    this.validatedRowType = validatedRowType;
    this.returnedRowType = returnedRowType;
    this.parameterRowType = parameterRowType;
  }

  /**
   * Row type from {@link org.apache.calcite.rel.RelRoot#validatedRowType} prepared by {@link DruidPlanner#prepare()}.
   * Corresponds to the SELECT portion of a SQL statement. For SELECT, this is the row type of the SELECT itself.
   * For EXPLAIN PLAN FOR SELECT, INSERT ... SELECT, or REPLACE ... SELECT, this is the row type of the
   * embedded SELECT.
   */
  public RelDataType getValidatedRowType()
  {
    return validatedRowType;
  }

  /**
   * Row type for the result that the end user will receive. Different from {@link #getValidatedRowType()} in
   * cases like EXPLAIN (where the user gets an explanation, not the query results) and other non-SELECT
   * statements.
   */
  public RelDataType getReturnedRowType()
  {
    return returnedRowType;
  }

  /**
   * Row type from {@link org.apache.calcite.sql.validate.SqlValidator#getParameterRowType} containing the
   * name and type of each parameter.
   */
  public RelDataType getParameterRowType()
  {
    return parameterRowType;
  }
}

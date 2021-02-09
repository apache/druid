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
  private final RelDataType rowType;
  private final RelDataType parameterRowType;

  public PrepareResult(final RelDataType rowType, final RelDataType parameterRowType)
  {
    this.rowType = rowType;
    this.parameterRowType = parameterRowType;
  }

  public RelDataType getRowType()
  {
    return rowType;
  }

  public RelDataType getParameterRowType()
  {
    return parameterRowType;
  }
}

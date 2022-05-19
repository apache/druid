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

import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.druid.sql.calcite.rel.DruidRel;

/**
 * Planner state capture for tests. The captured objects are available as
 * public fields since this is only ever meant to be used in tests, and
 * tests are already tightly coupled to the planner.
 * <p>
 * Spotbugs really doesn't like public fields referenced in another (test)
 * package. This file appears in spotbugs-exclude.xml to avoid the issue.
 */
public class CapturedState implements PlannerStateCapture
{
  public String sql;
  public SqlNode sqlNode;
  public RelRoot relRoot;
  public DruidRel<?> druidRel;
  public RelDataType parameterTypes;
  public PlannerContext plannerContext;
  public ValidationResult validationResult;
  public SqlNode queryNode;
  public SqlInsert insertNode;
  public BindableRel bindableRel;
  public Object execPlan;

  @Override
  public void capturePlannerContext(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  @Override
  public void captureSql(String sql)
  {
    this.sql = sql;
  }

  @Override
  public void captureParse(SqlNode root)
  {
    this.sqlNode = root;
  }

  @Override
  public void captureQueryRel(RelRoot rootQueryRel)
  {
    this.relRoot = rootQueryRel;
  }

  @Override
  public void captureDruidRel(DruidRel<?> druidRel)
  {
    this.druidRel = druidRel;
    this.execPlan = druidRel.dryRun();
  }

  @Override
  public void captureParameterTypes(RelDataType parameterTypes)
  {
    this.parameterTypes = parameterTypes;
  }

  @Override
  public void captureValidationResult(ValidationResult validationResult)
  {
    this.validationResult = validationResult;
  }

  @Override
  public void captureQuery(SqlNode query)
  {
    this.queryNode = query;
  }

  @Override
  public void captureInsert(SqlInsert insert)
  {
    this.insertNode = insert;
  }

  @Override
  public void captureBindableRel(BindableRel bindableRel)
  {
    this.bindableRel = bindableRel;
  }
}

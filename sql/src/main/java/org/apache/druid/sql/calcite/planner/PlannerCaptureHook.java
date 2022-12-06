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
import org.apache.druid.sql.calcite.rel.DruidRel;

public class PlannerCaptureHook implements PlannerHook
{
  private RelRoot relRoot;
  private SqlInsert insertNode;

  @Override
  public void captureSql(String sql)
  {
    // Not used at present. Add a field to capture this if you need it.
  }

  @Override
  public void captureQueryRel(RelRoot rootQueryRel)
  {
    this.relRoot = rootQueryRel;
  }

  @Override
  public void captureDruidRel(DruidRel<?> druidRel)
  {
    // Not used at present. Add a field to capture this if you need it.
  }

  @Override
  public void captureBindableRel(BindableRel bindableRel)
  {
    // Not used at present. Add a field to capture this if you need it.
  }

  @Override
  public void captureParameterTypes(RelDataType parameterTypes)
  {
    // Not used at present. Add a field to capture this if you need it.
  }

  @Override
  public void captureInsert(SqlInsert insert)
  {
    this.insertNode = insert;
  }

  public RelRoot relRoot()
  {
    return relRoot;
  }

  public SqlInsert insertNode()
  {
    return insertNode;
  }
}

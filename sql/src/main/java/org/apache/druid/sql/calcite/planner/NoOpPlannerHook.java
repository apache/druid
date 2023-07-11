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

public class NoOpPlannerHook implements PlannerHook
{
  public static final NoOpPlannerHook INSTANCE = new NoOpPlannerHook();

  @Override
  public void captureSql(String sql)
  {
  }

  @Override
  public void captureQueryRel(RelRoot rootQueryRel)
  {
  }

  @Override
  public void captureDruidRel(DruidRel<?> druidRel)
  {
  }

  @Override
  public void captureBindableRel(BindableRel bindableRel)
  {
  }

  @Override
  public void captureParameterTypes(RelDataType parameterTypes)
  {
  }

  @Override
  public void captureInsert(SqlInsert insert)
  {
  }
}

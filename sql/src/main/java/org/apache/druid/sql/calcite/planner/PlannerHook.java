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
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.sql.calcite.rel.DruidRel;

/**
 * Druid-specific version of Calcite's {@link org.apache.calcite.runtime.Hook Hook}
 * class. Captures artifacts of interest in the Druid planning process, generally
 * for test validation. Calcite's hook has multiple low-level events, but, sadly,
 * none at the points where tests want to verify, except for the opportunity to
 * capture the native query.
 */
@UnstableApi
public interface PlannerHook
{
  void captureSql(String sql);
  default void captureSqlNode(SqlNode node)
  {
  }
  void captureQueryRel(RelRoot rootQueryRel);
  void captureDruidRel(DruidRel<?> druidRel);
  void captureBindableRel(BindableRel bindableRel);
  void captureParameterTypes(RelDataType parameterTypes);
  void captureInsert(SqlInsert insert);
}

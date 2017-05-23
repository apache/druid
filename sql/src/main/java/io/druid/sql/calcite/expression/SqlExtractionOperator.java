/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.expression;

import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;

public interface SqlExtractionOperator
{
  /**
   * Returns the SQL operator corresponding to this function. Should be a singleton.
   *
   * @return operator
   */
  SqlFunction calciteFunction();

  /**
   * Translate a Calcite {@code RexNode} to a Druid column. The returned column may be a real column (from
   * rowSignature) or may be a virtual column (registered in virtualColumnRegistry).
   *
   * @param operatorTable         Operator table that can be used to convert sub-expressions
   * @param plannerContext        SQL planner context
   * @param rowSignature          signature of the rows to be extracted from
   * @param virtualColumnRegistry virtual columns registry for this conversion
   * @param expression            expression meant to be applied on top of the rows
   *
   * @return column name, or null if not possible
   *
   * @see Expressions#toDruidColumn(DruidOperatorTable, PlannerContext, RowSignature, VirtualColumnRegistry, RexNode)
   */
  String convert(
      DruidOperatorTable operatorTable,
      PlannerContext plannerContext,
      RowSignature rowSignature,
      VirtualColumnRegistry virtualColumnRegistry,
      RexNode expression
  );
}

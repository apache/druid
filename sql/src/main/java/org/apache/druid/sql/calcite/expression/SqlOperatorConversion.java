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

package org.apache.druid.sql.calcite.expression;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQuerySignature;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;

public interface SqlOperatorConversion
{
  /**
   * Returns the SQL operator corresponding to this function. Should be a singleton.
   *
   * @return operator
   */
  SqlOperator calciteOperator();

  /**
   * Translate a Calcite {@code RexNode} to a Druid expression.
   *
   * @param plannerContext SQL planner context
   * @param rowSignature   signature of the rows to be extracted from
   * @param rexNode        expression meant to be applied on top of the rows
   *
   * @return Druid expression, or null if translation is not possible
   *
   * @see Expressions#toDruidExpression(PlannerContext, RowSignature, RexNode)
   */
  @Nullable
  DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode);

  /**
   * Returns a Druid Aggregation corresponding to a SQL {@link SqlOperator} used to filter rows
   *
   * @param plannerContext   SQL planner context
   * @param querySignature   signature of the rows being aggregated and expression column references
   * @param rexNode          filter expression rex node
   *
   * @return filter, or null if the call cannot be translated
   */
  @Nullable
  default DimFilter toDruidFilter(
      PlannerContext plannerContext,
      DruidQuerySignature querySignature,
      RexNode rexNode
  )
  {
    return null;
  }
}

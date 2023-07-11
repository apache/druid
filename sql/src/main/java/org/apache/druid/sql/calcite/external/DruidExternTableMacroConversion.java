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

package org.apache.druid.sql.calcite.external;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.druid.catalog.model.table.TableFunction;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;

/**
 * Operator conversion for the `EXTERN` table macros (functions) based on the
 * {@link TableFunction} abstraction defined by the catalog.
 */
public class DruidExternTableMacroConversion implements SqlOperatorConversion
{
  private final SqlUserDefinedTableMacro operator;

  public DruidExternTableMacroConversion(
      final String name,
      final TableFunction fn,
      final ObjectMapper jsonMapper
  )
  {
    this.operator = new DruidExternTableMacro(
        new DruidTableMacro(name, fn, jsonMapper)
    );
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return operator;
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    return null;
  }
}

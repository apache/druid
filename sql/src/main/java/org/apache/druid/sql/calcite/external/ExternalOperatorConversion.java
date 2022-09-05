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

import com.google.inject.Inject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.expression.AuthorizableOperator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Registers the "EXTERN" operator, which is used in queries like "INSERT INTO dst SELECT * FROM TABLE(EXTERN(...))".
 *
 * This class is exercised in CalciteInsertDmlTest but is not currently exposed to end users.
 */
public class ExternalOperatorConversion implements SqlOperatorConversion
{
  public static final String FUNCTION_NAME = "EXTERN";

  // Resource that allows reading external data via SQL.
  public static final ResourceAction EXTERNAL_RESOURCE_ACTION =
      new ResourceAction(new Resource("EXTERNAL", ResourceType.EXTERNAL), Action.READ);

  private static final RelDataTypeFactory TYPE_FACTORY = new SqlTypeFactoryImpl(DruidTypeSystem.INSTANCE);

  private final SqlUserDefinedTableMacro operator;

  @Inject
  public ExternalOperatorConversion(final ExternalTableMacro macro)
  {
    this.operator = new ExternalOperator(macro);
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

  private static class ExternalOperator extends SqlUserDefinedTableMacro implements AuthorizableOperator
  {
    public ExternalOperator(final ExternalTableMacro macro)
    {
      super(
          new SqlIdentifier(FUNCTION_NAME, SqlParserPos.ZERO),
          ReturnTypes.CURSOR,
          null,
          OperandTypes.sequence(
              "(inputSource, inputFormat, signature)",
              OperandTypes.family(SqlTypeFamily.STRING),
              OperandTypes.family(SqlTypeFamily.STRING),
              OperandTypes.family(SqlTypeFamily.STRING)
          ),
          macro.getParameters()
               .stream()
               .map(parameter -> parameter.getType(TYPE_FACTORY))
               .collect(Collectors.toList()),
          macro
      );
    }

    @Override
    public Set<ResourceAction> computeResources(final SqlCall call)
    {
      return Collections.singleton(EXTERNAL_RESOURCE_ACTION);
    }
  }
}

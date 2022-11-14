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
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.table.ExternalTableDefn;
import org.apache.druid.catalog.model.table.ExternalTableSpec;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.expression.AuthorizableOperator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.external.UserDefinedTableMacroFunction.ExtendedTableMacro;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Base class for input-source-specfic table functions with arguments derived from
 * a catalog external table definition. Such functions work in conjunction with the
 * EXTERN key word to provide a schema. Example of the HTTP form:
 * <code><pre>
 * INSERT INTO myTable SELECT ...
 * FROM TABLE(http(
 *     userName => 'bob',
 *     password => 'secret',
 *     uris => 'http:foo.com/bar.csv',
 *     format => 'csv'))
 *   EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
 * PARTITIONED BY ...
 * </pre></code>
 */
public abstract class CatalogExternalTableOperatorConversion implements SqlOperatorConversion
{
  private final SqlUserDefinedTableMacro operator;

  public CatalogExternalTableOperatorConversion(
      final String name,
      final TableDefnRegistry registry,
      final String tableType,
      final ObjectMapper jsonMapper
  )
  {
    ExternalTableDefn tableDefn = (ExternalTableDefn) registry.defnFor(tableType);
    this.operator = new CatalogExternalTableOperator(
        new CatalogTableMacro(
            name,
            tableDefn,
            jsonMapper
        )
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

  public static class CatalogExternalTableOperator extends UserDefinedTableMacroFunction implements AuthorizableOperator
  {
    public CatalogExternalTableOperator(final CatalogTableMacro macro)
    {
      super(
          new SqlIdentifier(macro.name, SqlParserPos.ZERO),
          ReturnTypes.CURSOR,
          null,
          // Use our own definition of variadic since Calcite's doesn't allow
          // optional parameters.
          Externals.variadic(macro.parameters),
          Externals.dataTypes(macro.parameters),
          macro
      );
    }

    @Override
    public Set<ResourceAction> computeResources(final SqlCall call)
    {
      return Collections.singleton(ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION);
    }
  }

  public static class CatalogTableMacro implements ExtendedTableMacro
  {
    private final String name;
    private final List<FunctionParameter> parameters;
    private final ExternalTableDefn tableDefn;
    private final ObjectMapper jsonMapper;

    public CatalogTableMacro(
        final String name,
        final ExternalTableDefn tableDefn,
        final ObjectMapper jsonMapper
    )
    {
      this.name = name;
      this.tableDefn = tableDefn;
      this.jsonMapper = jsonMapper;
      this.parameters = Externals.convertParameters(tableDefn);
    }

    @Override
    public TranslatableTable apply(final List<Object> arguments)
    {
      throw new IAE(
          "The %s table function requires an EXTEND clause with a schema.",
          name
      );
    }

    @Override
    public TranslatableTable apply(List<Object> arguments, SqlNodeList schema)
    {
      final ExternalTableSpec externSpec = Externals.convertArguments(
          tableDefn,
          parameters,
          arguments,
          schema,
          jsonMapper
      );
      return Externals.buildExternalTable(externSpec, jsonMapper);
    }

    @Override
    public List<FunctionParameter> getParameters()
    {
      return parameters;
    }
  }
}

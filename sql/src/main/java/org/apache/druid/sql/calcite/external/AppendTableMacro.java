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

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.curator.shaded.com.google.common.collect.ImmutableList;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.RowSignature.Builder;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.expression.AuthorizableOperator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.DruidSqlValidator;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.DatasourceMetadata;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DatasourceTable.EffectiveMetadata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * FIXME
 */
public class AppendTableMacro extends SqlUserDefinedTableMacro implements AuthorizableOperator
{

  public static final OperatorConversion OPERATOR_CONVERSION = new OperatorConversion();
  public static final SqlOperator APPEND_TABLE_MACRO = new AppendTableMacro();

  private static class OperatorConversion implements SqlOperatorConversion
  {
    public static final String FUNCTION_NAME = "APPEND";

    public OperatorConversion()
    {
    }

    @Override
    public SqlOperator calciteOperator()
    {
      return APPEND_TABLE_MACRO;
    }

    @Override
    public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
    {
      throw new IllegalStateException();
    }
  }

  private AppendTableMacro()
  {
    super(
        new SqlIdentifier(OperatorConversion.FUNCTION_NAME, SqlParserPos.ZERO),
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.CURSOR,
        null,
        new OperandMetadata(),
        null
    );
  }

  private static class OperandMetadata implements SqlOperandMetadata
  {
    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure)
    {
      for (int i = 0; i < callBinding.getOperandCount(); i++) {
        SqlNode operand = callBinding.operand(i);
        if (!callBinding.isOperandLiteral(i, false)) {
          if (throwOnFailure) {
            throw DruidSqlValidator.buildCalciteContextException(
                "All arguments to APPEND should be literal strings."
                    + "Argument #" + i + " is not literal",
                operand
            );
          } else {
            return false;
          }
        }
        RelDataType type = callBinding.getOperandType(i);

        SqlTypeName typeName = type.getSqlTypeName();

        if (!SqlTypeFamily.CHARACTER.getTypeNames().contains(typeName)) {
          if (throwOnFailure) {
            throw DruidSqlValidator.buildCalciteContextException(
                "All arguments to APPEND should be literal strings."
                    + "Argument #" + i + " is not string",
                operand
            );
          } else {
            return false;
          }
        }
      }
      return true;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange()
    {
      return SqlOperandCountRanges.from(2);
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName)
    {
      return "APPEND( <TABLE_NAME>, <TABLE_NAME>[, <TABLE_NAME> ...] )";
    }

    @Override
    public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory)
    {
      RelDataType t = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      return ImmutableList.<RelDataType>builder().add(t, t).build();
    }

    @Override
    public List<String> paramNames()
    {
      return ImmutableList.<String>builder().add("tableName", "tableName").build();
    }
  }

  @Override
  public List<String> getParamNames()
  {
    return ImmutableList.<String>builder().add("tableName", "tableName").build();
  }

  @Override
  public TranslatableTable getTable(SqlOperatorBinding callBinding)
  {
    SqlCallBinding ss = (SqlCallBinding) callBinding;
    SqlValidator validator = ss.getValidator();
    SqlValidatorCatalogReader catalogReader = validator.getCatalogReader();
    List<String> tableNames = getTableNames(callBinding);
    List<RelOptTable> tables = getTables(catalogReader, tableNames);

    AppendDatasourceMetadata metadata = buildUnionDataSource(tables);
    return new DatasourceTable(
        metadata.values,
        metadata,
        EffectiveMetadata.of(metadata.values)
    );
  }

  static class AppendDatasourceMetadata implements DatasourceMetadata
  {
    private final RowSignature values;
    private final DataSource dataSource;

    public AppendDatasourceMetadata(RowSignature values, List<DataSource> dataSources)
    {
      this.values = values;
      this.dataSource = new UnionDataSource(dataSources);
    }

    @Override
    public boolean isJoinable()
    {
      return false;
    }

    @Override
    public boolean isBroadcast()
    {
      return false;
    }

    @Override
    public DataSource dataSource()
    {
      return dataSource;
    }
  }

  private AppendDatasourceMetadata buildUnionDataSource(List<RelOptTable> tables)
  {
    List<DataSource> dataSources = new ArrayList<>();
    Map<String, ColumnType> fields = new LinkedHashMap<>();
    Builder rowSignatureBuilder = RowSignature.builder();
    for (RelOptTable relOptTable : tables) {

      DatasourceTable a = relOptTable.unwrapOrThrow(DatasourceTable.class);

      RowSignature rowSignature = a.getRowSignature();
      for (String currentColumn : rowSignature.getColumnNames()) {

        String key = currentColumn;
        ColumnType currentType = rowSignature.getColumnType(currentColumn).get();
        ColumnType existingType = fields.get(key);
        if (existingType != null && !existingType.equals(currentType)) {
          // FIXME this could be more sophisticated
          throw new IllegalArgumentException("incompatible operands");
        }
        if (existingType == null) {
          fields.put(key, currentType);
        }
      }

      dataSources.add(a.getDataSource());
    }

    for (Entry<String, ColumnType> col : fields.entrySet()) {
      rowSignatureBuilder.add(col.getKey(), col.getValue());
    }
    return new AppendDatasourceMetadata(rowSignatureBuilder.build(), dataSources);
  }

  private List<String> getTableNames(SqlOperatorBinding callBinding)
  {
    List<String> ret = new ArrayList<>();
    for (int i = 0; i < callBinding.getOperandCount(); i++) {
      if (!callBinding.isOperandLiteral(i, false)) {
        throw new IllegalArgumentException(
            "All arguments of call to macro "
                + "APPEND should be literal. Actual argument #"
                + i + " is not literal"
        );
      }
      ret.add(callBinding.getOperandLiteralValue(i, String.class));
    }
    return ret;
  }

  private List<RelOptTable> getTables(SqlValidatorCatalogReader catalogReader, List<String> tableNames)
  {
    List<RelOptTable> ret = new ArrayList<>();
    for (String tableName : tableNames) {
      ImmutableList<String> names = ImmutableList.<String>builder().add(tableName).build();
      SqlValidatorTable t = catalogReader.getTable(names);
      ret.add(t.unwrapOrThrow(RelOptTable.class));
    }
    return ret;
  }

  @Override
  public Set<ResourceAction> computeResources(SqlCall call, boolean inputSourceTypeSecurityEnabled)
  {
    Set<ResourceAction> ret = new HashSet<>();
    for (SqlNode operand : call.getOperandList()) {
      Resource resource = new Resource(operand.toString(), ResourceType.DATASOURCE);
      ret.add(new ResourceAction(resource, Action.READ));
    }
    return ret;
  }
}

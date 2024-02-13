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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandMetadata;
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
import org.apache.druid.sql.calcite.table.DatasourceMetadata;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * FIXME
 */
public class ConcatTableMacro extends SqlUserDefinedTableMacro
{
  public ConcatTableMacro(SqlOperandMetadata t)
  {
    super(
        new SqlIdentifier(TableConcatOperatorConversion.FUNCTION_NAME, SqlParserPos.ZERO),
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.CURSOR,
        null,
        t,
        null
    );

    // this.macro = new MyTableMacro();
  }

  @Override
  public List<String> getParamNames()
  {
    return ImmutableList.<String>builder().add("t1", "t2").build();
  }

  static class MyTableMacro implements TableMacro
  {

    @Override
    public List<FunctionParameter> getParameters()
    {
      final ReflectiveFunctionBase.ParameterListBuilder params = ReflectiveFunctionBase.builder();

      params.add(String.class, "T1");
      params.add(String.class, "T2");
      return params.build();

    }

    @Override
    public TranslatableTable apply(List<? extends @Nullable Object> arguments)
    {
      if (true) {
        throw new RuntimeException("FIXME: Unimplemented!");
      }
      return null;
    }

  }

  @Override
  public TranslatableTable getTable(SqlOperatorBinding callBinding)
  {
    SqlCallBinding ss = (SqlCallBinding) callBinding;
    SqlValidator validator = ss.getValidator();
    SqlValidatorCatalogReader catalogReader = validator.getCatalogReader();
    Object t = catalogReader.getTable(ImmutableList.<String>builder().add("foo").build());

    List<String> tableNames = getTableNames(callBinding);
    List<RelOptTable> tables = getTables(catalogReader, tableNames);

    AppendDesc union = buildUnionDataSource(tables);
    return new AppendTable(union);
  }

  static class AppendDesc
  {
    private RowSignature values;
    private List<DataSource> dataSources;

    public AppendDesc(RowSignature values, List<DataSource> dataSources)
    {
      this.values = values;
      this.dataSources = dataSources;
    }

    public DatasourceMetadata buildPhysicalDatasourceMetadata()
    {
      return new DatasourceMetadata()
      {
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
          return new UnionDataSource(dataSources);
        }
      };
    }
  }

  private AppendDesc buildUnionDataSource(List<RelOptTable> tables)
  {
    List<DataSource> dataSources = new ArrayList<>();
    Map<String, RelDataTypeField> fields = new LinkedHashMap<>();
    Map<String, ColumnType> fields2 = new LinkedHashMap<>();
    Builder rowSignatureBuilder = RowSignature.builder();
    for (RelOptTable relOptTable : tables) {

      if(false) {
        for (RelDataTypeField newField : relOptTable.getRowType().getFieldList()) {

          String key = newField.getKey();
          RelDataTypeField existingField = fields.get(key);
          if (existingField != null && !existingField.equals(newField)) {
            // FIXME this could be more sophisticated
            throw new IllegalArgumentException("incompatible operands");
          }
          if (existingField == null) {
            fields.put(key, newField);
          }
        }
      }
      DatasourceTable a = relOptTable.unwrapOrThrow(DatasourceTable.class);

      RowSignature rowSignature = a.getRowSignature();
      for (String currentColumn : rowSignature.getColumnNames()) {

        String key = currentColumn;
        ColumnType currentType = rowSignature.getColumnType(currentColumn).get();
        ColumnType existingType = fields2.get(key);
        if (existingType != null && !existingType.equals(currentType)) {
          // FIXME this could be more sophisticated
          throw new IllegalArgumentException("incompatible operands");
        }
        if (existingType == null) {
          fields2.put(key, currentType);
        }
      }

      rowSignatureBuilder.addAll(a.getRowSignature());
      dataSources.add(a.getDataSource());
    }
    return new AppendDesc(rowSignatureBuilder.build(), dataSources);
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
      SqlValidatorTable t = catalogReader.getTable(ImmutableList.<String>builder().add(tableName).build());
      ret.add(t.unwrapOrThrow(RelOptTable.class));
    }
    return ret;
  }

  private TranslatableTable apply(List<RelOptTable> tables)
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  static class AppendTable extends DatasourceTable
  {

    public AppendTable(AppendDesc union)
    {
      super(
          null,
          union.buildPhysicalDatasourceMetadata (),
          null
          );
    }

  }
}

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

package io.druid.sql.calcite.schema;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Map;
import java.util.Set;

public class InformationSchema extends AbstractSchema
{
  public static final String NAME = "INFORMATION_SCHEMA";

  private static final String SCHEMATA_TABLE = "SCHEMATA";
  private static final String TABLES_TABLE = "TABLES";
  private static final String COLUMNS_TABLE = "COLUMNS";
  private static final RowSignature SCHEMATA_SIGNATURE = RowSignature
      .builder()
      .add("CATALOG_NAME", ValueType.STRING)
      .add("SCHEMA_NAME", ValueType.STRING)
      .add("SCHEMA_OWNER", ValueType.STRING)
      .add("DEFAULT_CHARACTER_SET_CATALOG", ValueType.STRING)
      .add("DEFAULT_CHARACTER_SET_SCHEMA", ValueType.STRING)
      .add("DEFAULT_CHARACTER_SET_NAME", ValueType.STRING)
      .add("SQL_PATH", ValueType.STRING)
      .build();
  private static final RowSignature TABLES_SIGNATURE = RowSignature
      .builder()
      .add("TABLE_CATALOG", ValueType.STRING)
      .add("TABLE_SCHEMA", ValueType.STRING)
      .add("TABLE_NAME", ValueType.STRING)
      .add("TABLE_TYPE", ValueType.STRING)
      .build();
  private static final RowSignature COLUMNS_SIGNATURE = RowSignature
      .builder()
      .add("TABLE_CATALOG", ValueType.STRING)
      .add("TABLE_SCHEMA", ValueType.STRING)
      .add("TABLE_NAME", ValueType.STRING)
      .add("COLUMN_NAME", ValueType.STRING)
      .add("ORDINAL_POSITION", ValueType.STRING)
      .add("COLUMN_DEFAULT", ValueType.STRING)
      .add("IS_NULLABLE", ValueType.STRING)
      .add("DATA_TYPE", ValueType.STRING)
      .add("CHARACTER_MAXIMUM_LENGTH", ValueType.STRING)
      .add("CHARACTER_OCTET_LENGTH", ValueType.STRING)
      .add("NUMERIC_PRECISION", ValueType.STRING)
      .add("NUMERIC_PRECISION_RADIX", ValueType.STRING)
      .add("NUMERIC_SCALE", ValueType.STRING)
      .add("DATETIME_PRECISION", ValueType.STRING)
      .add("CHARACTER_SET_NAME", ValueType.STRING)
      .add("COLLATION_NAME", ValueType.STRING)
      .add("JDBC_TYPE", ValueType.LONG)
      .build();
  private static final RelDataTypeSystem TYPE_SYSTEM = RelDataTypeSystem.DEFAULT;

  private final SchemaPlus rootSchema;
  private final Map<String, Table> tableMap;

  @Inject
  public InformationSchema(final SchemaPlus rootSchema)
  {
    this.rootSchema = Preconditions.checkNotNull(rootSchema, "rootSchema");
    this.tableMap = ImmutableMap.<String, Table>of(
        SCHEMATA_TABLE, new SchemataTable(),
        TABLES_TABLE, new TablesTable(),
        COLUMNS_TABLE, new ColumnsTable()
    );
  }

  @Override
  protected Map<String, Table> getTableMap()
  {
    return tableMap;
  }

  class SchemataTable implements ScannableTable
  {
    @Override
    public Enumerable<Object[]> scan(final DataContext root)
    {
      final FluentIterable<Object[]> results = FluentIterable
          .from(rootSchema.getSubSchemaNames())
          .transform(
              new Function<String, Object[]>()
              {
                @Override
                public Object[] apply(final String schemaName)
                {
                  final SchemaPlus subSchema = rootSchema.getSubSchema(schemaName);
                  return new Object[]{
                      "", // CATALOG_NAME
                      subSchema.getName(), // SCHEMA_NAME
                      null, // SCHEMA_OWNER
                      null, // DEFAULT_CHARACTER_SET_CATALOG
                      null, // DEFAULT_CHARACTER_SET_SCHEMA
                      null, // DEFAULT_CHARACTER_SET_NAME
                      null  // SQL_PATH
                  };
                }
              }
          );

      return Linq4j.asEnumerable(results);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory)
    {
      return SCHEMATA_SIGNATURE.getRelDataType(typeFactory);
    }

    @Override
    public Statistic getStatistic()
    {
      return Statistics.UNKNOWN;
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }
  }

  class TablesTable implements ScannableTable
  {
    @Override
    public Enumerable<Object[]> scan(final DataContext root)
    {
      final FluentIterable<Object[]> results = FluentIterable
          .from(rootSchema.getSubSchemaNames())
          .transformAndConcat(
              new Function<String, Iterable<Object[]>>()
              {
                @Override
                public Iterable<Object[]> apply(final String schemaName)
                {
                  final SchemaPlus subSchema = rootSchema.getSubSchema(schemaName);
                  final Set<String> tableNames = subSchema.getTableNames();
                  return FluentIterable.from(tableNames).transform(
                      new Function<String, Object[]>()
                      {
                        @Override
                        public Object[] apply(final String tableName)
                        {
                          return new Object[]{
                              null, // TABLE_CATALOG
                              schemaName, // TABLE_SCHEMA
                              tableName, // TABLE_NAME
                              subSchema.getTable(tableName).getJdbcTableType().toString() // TABLE_TYPE
                          };
                        }
                      }
                  );
                }
              }
          );

      return Linq4j.asEnumerable(results);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory)
    {
      return TABLES_SIGNATURE.getRelDataType(typeFactory);
    }

    @Override
    public Statistic getStatistic()
    {
      return Statistics.UNKNOWN;
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }
  }

  class ColumnsTable implements ScannableTable
  {
    @Override
    public Enumerable<Object[]> scan(final DataContext root)
    {
      final FluentIterable<Object[]> results = FluentIterable
          .from(rootSchema.getSubSchemaNames())
          .transformAndConcat(
              new Function<String, Iterable<Object[]>>()
              {
                @Override
                public Iterable<Object[]> apply(final String schemaName)
                {
                  final SchemaPlus subSchema = rootSchema.getSubSchema(schemaName);
                  final Set<String> tableNames = subSchema.getTableNames();
                  final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl(TYPE_SYSTEM);
                  return FluentIterable.from(tableNames).transformAndConcat(
                      new Function<String, Iterable<Object[]>>()
                      {
                        @Override
                        public Iterable<Object[]> apply(final String tableName)
                        {
                          return FluentIterable
                              .from(subSchema.getTable(tableName).getRowType(typeFactory).getFieldList())
                              .transform(
                                  new Function<RelDataTypeField, Object[]>()
                                  {
                                    @Override
                                    public Object[] apply(final RelDataTypeField field)
                                    {
                                      final RelDataType type = field.getType();
                                      boolean isNumeric = SqlTypeName.NUMERIC_TYPES.contains(type.getSqlTypeName());
                                      boolean isCharacter = SqlTypeName.CHAR_TYPES.contains(type.getSqlTypeName());
                                      boolean isDateTime = SqlTypeName.DATETIME_TYPES.contains(type.getSqlTypeName());
                                      return new Object[]{
                                          "", // TABLE_CATALOG
                                          schemaName, // TABLE_SCHEMA
                                          tableName, // TABLE_NAME
                                          field.getName(), // COLUMN_NAME
                                          String.valueOf(field.getIndex()), // ORDINAL_POSITION
                                          "", // COLUMN_DEFAULT
                                          type.isNullable() ? "YES" : "NO", // IS_NULLABLE
                                          type.getSqlTypeName().toString(), // DATA_TYPE
                                          null, // CHARACTER_MAXIMUM_LENGTH
                                          null, // CHARACTER_OCTET_LENGTH
                                          isNumeric ? String.valueOf(type.getPrecision()) : null, // NUMERIC_PRECISION
                                          isNumeric ? "10" : null, // NUMERIC_PRECISION_RADIX
                                          isNumeric ? String.valueOf(type.getScale()) : null, // NUMERIC_SCALE
                                          isDateTime ? String.valueOf(type.getPrecision()) : null, // DATETIME_PRECISION
                                          isCharacter ? type.getCharset().name() : null, // CHARACTER_SET_NAME
                                          isCharacter ? type.getCollation().getCollationName() : null, // COLLATION_NAME
                                          type.getSqlTypeName().getJdbcOrdinal() // JDBC_TYPE (Druid extension)
                                      };
                                    }
                                  }
                              );
                        }
                      }
                  );
                }
              }
          );

      return Linq4j.asEnumerable(results);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory)
    {
      return COLUMNS_SIGNATURE.getRelDataType(typeFactory);
    }

    @Override
    public Statistic getStatistic()
    {
      return Statistics.UNKNOWN;
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }
  }
}

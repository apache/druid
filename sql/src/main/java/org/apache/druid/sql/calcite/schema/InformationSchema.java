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

package org.apache.druid.sql.calcite.schema;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
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
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class InformationSchema extends AbstractSchema
{
  private static final EmittingLogger log = new EmittingLogger(InformationSchema.class);

  private static final String CATALOG_NAME = "druid";
  private static final String SCHEMATA_TABLE = "SCHEMATA";
  private static final String TABLES_TABLE = "TABLES";
  private static final String COLUMNS_TABLE = "COLUMNS";
  private static final RowSignature SCHEMATA_SIGNATURE = RowSignature
      .builder()
      .add("CATALOG_NAME", ColumnType.STRING)
      .add("SCHEMA_NAME", ColumnType.STRING)
      .add("SCHEMA_OWNER", ColumnType.STRING)
      .add("DEFAULT_CHARACTER_SET_CATALOG", ColumnType.STRING)
      .add("DEFAULT_CHARACTER_SET_SCHEMA", ColumnType.STRING)
      .add("DEFAULT_CHARACTER_SET_NAME", ColumnType.STRING)
      .add("SQL_PATH", ColumnType.STRING)
      .build();
  private static final RowSignature TABLES_SIGNATURE = RowSignature
      .builder()
      .add("TABLE_CATALOG", ColumnType.STRING)
      .add("TABLE_SCHEMA", ColumnType.STRING)
      .add("TABLE_NAME", ColumnType.STRING)
      .add("TABLE_TYPE", ColumnType.STRING)
      .add("IS_JOINABLE", ColumnType.STRING)
      .add("IS_BROADCAST", ColumnType.STRING)
      .build();
  private static final RowSignature COLUMNS_SIGNATURE = RowSignature
      .builder()
      .add("TABLE_CATALOG", ColumnType.STRING)
      .add("TABLE_SCHEMA", ColumnType.STRING)
      .add("TABLE_NAME", ColumnType.STRING)
      .add("COLUMN_NAME", ColumnType.STRING)
      .add("ORDINAL_POSITION", ColumnType.STRING)
      .add("COLUMN_DEFAULT", ColumnType.STRING)
      .add("IS_NULLABLE", ColumnType.STRING)
      .add("DATA_TYPE", ColumnType.STRING)
      .add("CHARACTER_MAXIMUM_LENGTH", ColumnType.STRING)
      .add("CHARACTER_OCTET_LENGTH", ColumnType.STRING)
      .add("NUMERIC_PRECISION", ColumnType.STRING)
      .add("NUMERIC_PRECISION_RADIX", ColumnType.STRING)
      .add("NUMERIC_SCALE", ColumnType.STRING)
      .add("DATETIME_PRECISION", ColumnType.STRING)
      .add("CHARACTER_SET_NAME", ColumnType.STRING)
      .add("COLLATION_NAME", ColumnType.STRING)
      .add("JDBC_TYPE", ColumnType.LONG)
      .build();
  private static final RelDataTypeSystem TYPE_SYSTEM = RelDataTypeSystem.DEFAULT;

  private static final String INFO_TRUE = "YES";
  private static final String INFO_FALSE = "NO";

  private final DruidSchemaCatalog rootSchema;
  private final Map<String, Table> tableMap;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public InformationSchema(
      @Named(DruidCalciteSchemaModule.INCOMPLETE_SCHEMA) final DruidSchemaCatalog rootSchema,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.rootSchema = Preconditions.checkNotNull(rootSchema, "rootSchema");
    this.tableMap = ImmutableMap.of(
        SCHEMATA_TABLE, new SchemataTable(),
        TABLES_TABLE, new TablesTable(),
        COLUMNS_TABLE, new ColumnsTable()
    );
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  protected Map<String, Table> getTableMap()
  {
    return tableMap;
  }

  class SchemataTable extends AbstractTable implements ScannableTable
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
                      CATALOG_NAME, // CATALOG_NAME
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
      return RowSignatures.toRelDataType(SCHEMATA_SIGNATURE, typeFactory);
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

  class TablesTable extends AbstractTable implements ScannableTable
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

                  final AuthenticationResult authenticationResult =
                      (AuthenticationResult) root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT);

                  final Set<String> authorizedTableNames = getAuthorizedTableNamesFromSubSchema(
                      subSchema,
                      authenticationResult
                  );

                  final Set<String> authorizedFunctionNames = getAuthorizedFunctionNamesFromSubSchema(
                      subSchema,
                      authenticationResult
                  );

                  return Iterables.filter(
                      Iterables.concat(
                          FluentIterable.from(authorizedTableNames).transform(
                              tableName -> {
                                final Table table = subSchema.getTable(tableName);
                                final boolean isJoinable;
                                final boolean isBroadcast;
                                if (table instanceof DruidTable) {
                                  DruidTable druidTable = (DruidTable) table;
                                  isJoinable = druidTable.isJoinable();
                                  isBroadcast = druidTable.isBroadcast();
                                } else {
                                  isJoinable = false;
                                  isBroadcast = false;
                                }

                                return new Object[]{
                                    CATALOG_NAME, // TABLE_CATALOG
                                    schemaName, // TABLE_SCHEMA
                                    tableName, // TABLE_NAME
                                    table.getJdbcTableType().toString(), // TABLE_TYPE
                                    isJoinable ? INFO_TRUE : INFO_FALSE, // IS_JOINABLE
                                    isBroadcast ? INFO_TRUE : INFO_FALSE // IS_BROADCAST
                                };
                              }
                          ),
                          FluentIterable.from(authorizedFunctionNames).transform(
                              new Function<String, Object[]>()
                              {
                                @Override
                                public Object[] apply(final String functionName)
                                {
                                  if (getView(subSchema, functionName) != null) {
                                    return new Object[]{
                                        CATALOG_NAME, // TABLE_CATALOG
                                        schemaName, // TABLE_SCHEMA
                                        functionName, // TABLE_NAME
                                        "VIEW", // TABLE_TYPE
                                        INFO_FALSE, // IS_JOINABLE
                                        INFO_FALSE // IS_BROADCAST
                                    };
                                  } else {
                                    return null;
                                  }
                                }
                              }
                          )
                      ),
                      Predicates.notNull()
                  );
                }
              }
          );

      return Linq4j.asEnumerable(results);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(TABLES_SIGNATURE, typeFactory);
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

  class ColumnsTable extends AbstractTable implements ScannableTable
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
                  final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl(TYPE_SYSTEM);

                  final AuthenticationResult authenticationResult =
                      (AuthenticationResult) root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT);

                  final Set<String> authorizedTableNames = getAuthorizedTableNamesFromSubSchema(
                      subSchema,
                      authenticationResult
                  );

                  final Set<String> authorizedFunctionNames = getAuthorizedFunctionNamesFromSubSchema(
                      subSchema,
                      authenticationResult
                  );

                  return Iterables.concat(
                      Iterables.filter(
                          Iterables.concat(
                              FluentIterable.from(authorizedTableNames).transform(
                                  new Function<String, Iterable<Object[]>>()
                                  {
                                    @Override
                                    public Iterable<Object[]> apply(final String tableName)
                                    {
                                      return generateColumnMetadata(
                                          schemaName,
                                          tableName,
                                          subSchema.getTable(tableName),
                                          typeFactory
                                      );
                                    }
                                  }
                              ),
                              FluentIterable.from(authorizedFunctionNames).transform(
                                  new Function<String, Iterable<Object[]>>()
                                  {
                                    @Override
                                    public Iterable<Object[]> apply(final String functionName)
                                    {
                                      final TableMacro viewMacro = getView(subSchema, functionName);
                                      if (viewMacro == null) {
                                        return null;
                                      }

                                      try {
                                        return generateColumnMetadata(
                                            schemaName,
                                            functionName,
                                            viewMacro.apply(Collections.emptyList()),
                                            typeFactory
                                        );
                                      }
                                      catch (Exception e) {
                                        log.error(e, "Encountered exception while handling view[%s].", functionName);
                                        return null;
                                      }
                                    }
                                  }
                              )
                          ),
                          Predicates.notNull()
                      )
                  );
                }
              }
          );

      return Linq4j.asEnumerable(results);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(COLUMNS_SIGNATURE, typeFactory);
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

    @Nullable
    private Iterable<Object[]> generateColumnMetadata(
        final String schemaName,
        final String tableName,
        final Table table,
        final RelDataTypeFactory typeFactory
    )
    {
      if (table == null) {
        return null;
      }

      return FluentIterable
          .from(table.getRowType(typeFactory).getFieldList())
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

                  final String typeName = type instanceof RowSignatures.ComplexSqlType ? ((RowSignatures.ComplexSqlType) type).asTypeString() : type.getSqlTypeName().toString();
                  return new Object[]{
                      CATALOG_NAME, // TABLE_CATALOG
                      schemaName, // TABLE_SCHEMA
                      tableName, // TABLE_NAME
                      field.getName(), // COLUMN_NAME
                      String.valueOf(field.getIndex()), // ORDINAL_POSITION
                      "", // COLUMN_DEFAULT
                      type.isNullable() ? INFO_TRUE : INFO_FALSE, // IS_NULLABLE
                      typeName, // DATA_TYPE
                      null, // CHARACTER_MAXIMUM_LENGTH
                      null, // CHARACTER_OCTET_LENGTH
                      isNumeric ? String.valueOf(type.getPrecision()) : null, // NUMERIC_PRECISION
                      isNumeric ? "10" : null, // NUMERIC_PRECISION_RADIX
                      isNumeric ? String.valueOf(type.getScale()) : null, // NUMERIC_SCALE
                      isDateTime ? String.valueOf(type.getPrecision()) : null, // DATETIME_PRECISION
                      isCharacter ? type.getCharset().name() : null, // CHARACTER_SET_NAME
                      isCharacter ? type.getCollation().getCollationName() : null, // COLLATION_NAME
                      Long.valueOf(type.getSqlTypeName().getJdbcOrdinal()) // JDBC_TYPE (Druid extension)
                  };
                }
              }
          );
    }
  }

  /**
   * Return a view macro that may or may not be defined in a certain schema. If it's not defined, returns null.
   *
   * @param schemaPlus   schema
   * @param functionName function name
   *
   * @return view, or null
   */
  @Nullable
  private static TableMacro getView(final SchemaPlus schemaPlus, final String functionName)
  {
    // Look for a zero-arg function that is also a TableMacro. The returned value
    // is never null so we don't need to check for that.
    final Collection<org.apache.calcite.schema.Function> functions =
        schemaPlus.getFunctions(functionName);

    for (org.apache.calcite.schema.Function function : functions) {
      if (function.getParameters().isEmpty() && function instanceof TableMacro) {
        return (TableMacro) function;
      }
    }

    return null;
  }

  private Set<String> getAuthorizedTableNamesFromSubSchema(
      final SchemaPlus subSchema,
      final AuthenticationResult authenticationResult
  )
  {
    return getAuthorizedNamesFromNamedSchema(
        authenticationResult,
        rootSchema.getNamedSchema(subSchema.getName()),
        subSchema.getTableNames()
    );
  }

  private Set<String> getAuthorizedFunctionNamesFromSubSchema(
      final SchemaPlus subSchema,
      final AuthenticationResult authenticationResult
  )
  {
    return getAuthorizedNamesFromNamedSchema(
        authenticationResult,
        rootSchema.getNamedSchema(subSchema.getName()),
        subSchema.getFunctionNames()
    );
  }

  private Set<String> getAuthorizedNamesFromNamedSchema(
      final AuthenticationResult authenticationResult,
      final NamedSchema schema,
      final Set<String> names
  )
  {
    if (schema != null) {
      return ImmutableSet.copyOf(
          AuthorizationUtils.filterAuthorizedResources(
              authenticationResult,
              names,
              name -> {
                final String resoureType = schema.getSchemaResourceType(name);
                if (resoureType != null) {
                  return Collections.singletonList(
                      new ResourceAction(new Resource(name, resoureType), Action.READ)
                  );
                } else {
                  return Collections.emptyList();
                }
              },
              authorizerMapper
          )
      );
    } else {
      // for schemas with no resource type, or that are not named schemas, we don't filter anything
      return names;
    }
  }
}

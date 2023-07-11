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
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
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

  private static class RowTypeBuilder
  {
    final RelDataTypeFactory typeFactory = DruidTypeSystem.TYPE_FACTORY;
    final RelDataTypeFactory.Builder builder = typeFactory.builder();

    public RowTypeBuilder add(String name, SqlTypeName type)
    {
      builder.add(name, Calcites.createSqlTypeWithNullability(typeFactory, type, false));
      return this;
    }

    public RowTypeBuilder add(String name, SqlTypeName type, boolean nullable)
    {
      builder.add(name, Calcites.createSqlTypeWithNullability(typeFactory, type, nullable));
      return this;
    }

    public RelDataType build()
    {
      return builder.build();
    }
  }

  private static final RelDataType SCHEMATA_SIGNATURE = new RowTypeBuilder()
      .add("CATALOG_NAME", SqlTypeName.VARCHAR)
      .add("SCHEMA_NAME", SqlTypeName.VARCHAR)
      .add("SCHEMA_OWNER", SqlTypeName.VARCHAR)
      .add("DEFAULT_CHARACTER_SET_CATALOG", SqlTypeName.VARCHAR)
      .add("DEFAULT_CHARACTER_SET_SCHEMA", SqlTypeName.VARCHAR)
      .add("DEFAULT_CHARACTER_SET_NAME", SqlTypeName.VARCHAR)
      .add("SQL_PATH", SqlTypeName.VARCHAR)
      .build();
  private static final RelDataType TABLES_SIGNATURE = new RowTypeBuilder()
      .add("TABLE_CATALOG", SqlTypeName.VARCHAR)
      .add("TABLE_SCHEMA", SqlTypeName.VARCHAR)
      .add("TABLE_NAME", SqlTypeName.VARCHAR)
      .add("TABLE_TYPE", SqlTypeName.VARCHAR)
      .add("IS_JOINABLE", SqlTypeName.VARCHAR)
      .add("IS_BROADCAST", SqlTypeName.VARCHAR)
      .build();
  private static final RelDataType COLUMNS_SIGNATURE = new RowTypeBuilder()
      .add("TABLE_CATALOG", SqlTypeName.VARCHAR)
      .add("TABLE_SCHEMA", SqlTypeName.VARCHAR)
      .add("TABLE_NAME", SqlTypeName.VARCHAR)
      .add("COLUMN_NAME", SqlTypeName.VARCHAR)
      .add("ORDINAL_POSITION", SqlTypeName.BIGINT)
      .add("COLUMN_DEFAULT", SqlTypeName.VARCHAR)
      .add("IS_NULLABLE", SqlTypeName.VARCHAR)
      .add("DATA_TYPE", SqlTypeName.VARCHAR)
      .add("CHARACTER_MAXIMUM_LENGTH", SqlTypeName.VARCHAR, true)
      .add("CHARACTER_OCTET_LENGTH", SqlTypeName.VARCHAR, true)
      .add("NUMERIC_PRECISION", SqlTypeName.BIGINT, true)
      .add("NUMERIC_PRECISION_RADIX", SqlTypeName.BIGINT, true)
      .add("NUMERIC_SCALE", SqlTypeName.BIGINT, true)
      .add("DATETIME_PRECISION", SqlTypeName.BIGINT, true)
      .add("CHARACTER_SET_NAME", SqlTypeName.VARCHAR, true)
      .add("COLLATION_NAME", SqlTypeName.VARCHAR, true)
      .add("JDBC_TYPE", SqlTypeName.BIGINT)
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
      return SCHEMATA_SIGNATURE;
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
      return TABLES_SIGNATURE;
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
                  final RelDataTypeFactory typeFactory = root.getTypeFactory();

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
                                      Table table = subSchema.getTable(tableName);
                                      if (table == null) {
                                        // Table just disappeared.
                                        return null;
                                      }
                                      return generateColumnMetadata(
                                          schemaName,
                                          tableName,
                                           table.getRowType(typeFactory),
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
                                      if (viewMacro != null) {
                                        try {
                                          return generateColumnMetadata(
                                              schemaName,
                                              functionName,
                                              viewMacro.apply(Collections.emptyList()).getRowType(typeFactory),
                                              typeFactory
                                          );
                                        }
                                        catch (Exception e) {
                                          log.error(e, "Encountered exception while handling view[%s].", functionName);
                                          return null;
                                        }
                                      } else {
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
      return COLUMNS_SIGNATURE;
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
        final RelDataType tableSchema,
        final RelDataTypeFactory typeFactory
    )
    {
      return FluentIterable
          .from(tableSchema.getFieldList())
          .transform(
              new Function<RelDataTypeField, Object[]>()
              {
                @Override
                public Object[] apply(final RelDataTypeField field)
                {
                  final RelDataType type = field.getType();
                  SqlTypeName sqlTypeName = type.getSqlTypeName();
                  boolean isNumeric = SqlTypeName.NUMERIC_TYPES.contains(sqlTypeName);
                  boolean isCharacter = SqlTypeName.CHAR_TYPES.contains(sqlTypeName);
                  boolean isDateTime = SqlTypeName.DATETIME_TYPES.contains(sqlTypeName);

                  final String typeName = type instanceof RowSignatures.ComplexSqlType ? ((RowSignatures.ComplexSqlType) type).asTypeString() : sqlTypeName.toString();
                  return new Object[]{
                      CATALOG_NAME, // TABLE_CATALOG
                      schemaName, // TABLE_SCHEMA
                      tableName, // TABLE_NAME
                      field.getName(), // COLUMN_NAME
                      (long) (field.getIndex() + 1), // ORDINAL_POSITION
                      "", // COLUMN_DEFAULT
                      type.isNullable() ? INFO_TRUE : INFO_FALSE, // IS_NULLABLE
                      typeName, // DATA_TYPE
                      null, // CHARACTER_MAXIMUM_LENGTH
                      null, // CHARACTER_OCTET_LENGTH
                      isNumeric ? (long) type.getPrecision() : null, // NUMERIC_PRECISION
                      isNumeric ? 10L : null, // NUMERIC_PRECISION_RADIX
                      isNumeric ? (long) type.getScale() : null, // NUMERIC_SCALE
                      isDateTime ? (long) type.getPrecision() : null, // DATETIME_PRECISION
                      isCharacter ? type.getCharset().name() : null, // CHARACTER_SET_NAME
                      isCharacter ? type.getCollation().getCollationName() : null, // COLLATION_NAME
                      (long) type.getSqlTypeName().getJdbcOrdinal() // JDBC_TYPE (Druid extension)
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
    if (schema == null) {
      // for schemas with no resource type, or that are not named schemas, we don't filter anything
      return names;
    }
    return ImmutableSet.copyOf(
        AuthorizationUtils.filterAuthorizedResources(
            authenticationResult,
            names,
            name -> {
              final String resourseType = schema.getSchemaResourceType(name);
              if (resourseType == null) {
                return Collections.emptyList();
              }
              return Collections.singletonList(
                  new ResourceAction(new Resource(name, resourseType), Action.READ)
              );
            },
            authorizerMapper
        )
    );
  }
}

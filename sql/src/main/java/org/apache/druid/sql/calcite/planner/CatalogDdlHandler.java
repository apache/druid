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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.catalog.model.ClusteredValueGroupsBaseTableMetadata;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.DatasourceProjectionMetadata;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.explain.ExplainAttributes;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.parser.DruidSqlAlterTable;
import org.apache.druid.sql.calcite.parser.DruidSqlColumnDeclaration;
import org.apache.druid.sql.calcite.parser.DruidSqlCreateTable;
import org.apache.druid.sql.calcite.parser.DruidSqlParser;
import org.apache.druid.sql.calcite.parser.DruidSqlPropertyAssignment;
import org.apache.druid.sql.calcite.parser.SqlGranularityLiteral;
import org.apache.druid.sql.calcite.parser.SqlProjectionSpec;
import org.apache.druid.sql.calcite.run.EngineFeature;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handles the catalog DDL statements: {@code CREATE TABLE} and {@code ALTER TABLE}.
 * <p>
 * These statements are metadata operations, not queries. They are validated here, converted to a catalog
 * {@link TableSpec} or column/property edit, and applied through {@link CatalogTableWriter}, which forwards them to
 * the Coordinator. No Calcite validation or query planning takes place, and no rows are returned.
 * <p>
 * Validation is deliberately split. This class checks what it can attribute to a position in the statement (type
 * spellings, duplicate columns, granularity, clustering) so that the error names the offending SQL. The Coordinator
 * remains authoritative: {@code DatasourceDefn.validate} runs on write, and its message is surfaced verbatim.
 */
public abstract class CatalogDdlHandler extends SqlStatementHandler.BaseStatementHandler
{
  /**
   * DDL produces no rows. A single-column type still has to be declared, because JDBC clients ask for a result set
   * signature when preparing the statement.
   */
  private static final RelDataType RESULT_TYPE = resultType();

  protected final SqlIdentifier tableIdentifier;
  protected TableId tableId;

  protected CatalogDdlHandler(SqlStatementHandler.HandlerContext handlerContext, SqlIdentifier tableIdentifier)
  {
    super(handlerContext);
    this.tableIdentifier = tableIdentifier;
  }

  /**
   * The runtime property that gates these statements. Read from {@link PlannerConfig} rather than the query context
   * so that a user cannot turn the feature on for their own statement.
   */
  public static final String ENABLE_CATALOG_DDL_PROPERTY = "druid.sql.planner.enableCatalogDdl";

  /**
   * The reserved name of the base-table projection, which describes the physical layout of the table itself. Handled
   * as a separate catalog property, not as one of the aggregate projections.
   */
  public static final String BASE_PROJECTION_NAME = "__base";

  @Override
  public void validate()
  {
    if (!handlerContext.plannerContext().featureAvailable(EngineFeature.CAN_DDL)) {
      throw InvalidSqlInput.exception(
          "[%s] is not supported by requested SQL engine [%s]. DDL executes immediately and returns no task, so it"
          + " is available only on interactive engines",
          operationName(),
          handlerContext.engine().name()
      );
    }
    if (!handlerContext.plannerContext().getPlannerConfig().isEnableCatalogDdl()) {
      throw DruidException.forPersona(DruidException.Persona.ADMIN)
                          .ofCategory(DruidException.Category.UNSUPPORTED)
                          .build(
                              "Catalog DDL statements are disabled. Set [%s] to true on the Broker to enable [%s].",
                              ENABLE_CATALOG_DDL_PROPERTY,
                              operationName()
                          );
    }
    if (!handlerContext.plannerContext().getParameters().isEmpty()) {
      throw InvalidSqlInput.exception("Dynamic parameters are not supported for [%s]", operationName());
    }
    tableId = TableId.datasource(resolveTableName());
    final Resource resource = new Resource(tableId.name(), ResourceType.DATASOURCE);
    resourceActions = ImmutableSet.of(
        new ResourceAction(resource, Action.READ),
        new ResourceAction(resource, Action.WRITE)
    );
    validateStatement();
  }

  /**
   * Statement-specific validation, which also prepares whatever {@link #execute} will apply.
   */
  protected abstract void validateStatement();

  protected abstract void execute(CatalogTableWriter writer);

  protected abstract String operationName();

  @Override
  public void prepare()
  {
    // Nothing to prepare: there is no query to plan.
  }

  @Override
  public PrepareResult prepareResult()
  {
    return new PrepareResult(RESULT_TYPE, RESULT_TYPE, DruidTypeSystem.TYPE_FACTORY.createStructType(
        Collections.emptyList(),
        Collections.emptyList()
    ));
  }

  @Override
  public PlannerResult plan()
  {
    return new PlannerResult(
        () -> {
          execute(handlerContext.plannerContext().getPlannerToolbox().catalogTableWriter());
          return QueryResponse.withEmptyContext(Sequences.empty());
        },
        RESULT_TYPE
    );
  }

  @Override
  public ExplainAttributes explainAttributes()
  {
    throw InvalidSqlInput.exception("EXPLAIN is not supported for [%s]", operationName());
  }

  /**
   * Resolve the table name, which may be unqualified or qualified by the Druid schema. Other schemas are rejected:
   * only datasources have catalog specs that DDL can write.
   */
  private String resolveTableName()
  {
    final String tableName;
    if (tableIdentifier.names.size() == 1) {
      tableName = tableIdentifier.names.get(0);
    } else if (tableIdentifier.names.size() == 2) {
      final String defaultSchemaName =
          Iterables.getOnlyElement(CalciteSchema.from(handlerContext.defaultSchema()).path(null));
      if (!defaultSchemaName.equals(tableIdentifier.names.get(0))) {
        throw InvalidSqlInput.exception(
            "Table [%s] does not support operation [%s] because it is not a Druid datasource",
            tableIdentifier,
            operationName()
        );
      }
      tableName = tableIdentifier.names.get(1);
    } else {
      throw InvalidSqlInput.exception(
          "Table name [%s] is not valid for operation [%s]",
          tableIdentifier,
          operationName()
      );
    }
    IdUtils.validateId("table", tableName);
    return tableName;
  }

  /**
   * Convert a parsed column declaration into its catalog form, checking that the type is one Druid can store.
   */
  protected static ColumnSpec toColumnSpec(DruidSqlColumnDeclaration declaration)
  {
    final String name = simpleName(declaration.getName(), "Column");
    final String type;
    try {
      type = CatalogColumnTypes.forCatalogColumn(name, declaration.getDataType());
    }
    catch (IAE e) {
      throw InvalidSqlInput.exception(e, "%s", e.getMessage());
    }
    if (Columns.isTimeColumn(name) && !ColumnType.LONG.equals(Columns.druidTypeFromString(type))) {
      throw InvalidSqlInput.exception(
          "Column [%s] must have type [%s] or [%s], but was [%s]",
          Columns.TIME_COLUMN,
          Columns.SQL_TIMESTAMP,
          Columns.SQL_BIGINT,
          type
      );
    }
    return new ColumnSpec(name, type, null);
  }

  /**
   * Translate one projection definition into the catalog form.
   * <p>
   * {@code __base} is reserved for the base-table projection, which is a different catalog entity: it describes the
   * physical layout of the table itself rather than an additional aggregate. It is rejected here rather than being
   * translated as an ordinary projection.
   */
  protected static DatasourceProjectionMetadata translateProjection(
      final SqlStatementHandler.HandlerContext handlerContext,
      final String tableName,
      final List<ColumnSpec> columns,
      final SqlProjectionSpec projection
  )
  {
    final String name = simpleName(projection.getName(), "Projection");
    try {
      Projections.validateProjectionName(name);
    }
    catch (DruidException e) {
      throw InvalidSqlInput.exception(e, "%s", e.getMessage());
    }
    if (projection.getClusteredBy() != null) {
      throw InvalidSqlInput.exception(
          "Projection [%s] cannot use CLUSTERED BY: an aggregate projection is ordered by its grouping columns."
          + " Only the [%s] projection, which describes the table's own layout, chooses a clustering",
          name,
          BASE_PROJECTION_NAME
      );
    }
    return new DatasourceProjectionMetadata(
        new ProjectionSpecTranslator(handlerContext.plannerFactory(), handlerContext.queryContextMap())
            .translate(tableName, columns, name, projection.getBody())
    );
  }

  /**
   * Translate the reserved {@code __base} projection, which describes the physical layout of the table rather than an
   * additional aggregate, and so becomes the {@code baseTable} property instead of one of the projections.
   */
  protected static ClusteredValueGroupsBaseTableMetadata translateBaseTable(
      final SqlStatementHandler.HandlerContext handlerContext,
      final String tableName,
      final List<ColumnSpec> columns,
      final SqlProjectionSpec projection
  )
  {
    return new ProjectionSpecTranslator(handlerContext.plannerFactory(), handlerContext.queryContextMap())
        .translateBaseTable(tableName, columns, projection.getBody(), projection.getClusteredBy());
  }

  protected static String simpleName(SqlIdentifier identifier, String what)
  {
    if (!identifier.isSimple()) {
      throw InvalidSqlInput.exception("%s name [%s] must be a simple name", what, identifier);
    }
    return identifier.getSimple();
  }

  /**
   * The catalog stores a segment granularity as either {@code ALL} or an ISO period string.
   */
  protected static String toGranularityString(SqlGranularityLiteral partitionedBy)
  {
    final Granularity granularity = partitionedBy.getGranularity();
    if (Granularities.ALL.equals(granularity)) {
      return DatasourceDefn.ALL_GRANULARITY;
    }
    if (granularity instanceof PeriodGranularity) {
      return ((PeriodGranularity) granularity).getPeriod().toString();
    }
    throw InvalidSqlInput.exception("Granularity [%s] is not supported by the catalog", partitionedBy);
  }

  /**
   * The catalog's clustering keys are plain ascending column references. Expressions, ordinals and DESC have no
   * catalog representation, so they are rejected here rather than silently dropped.
   */
  protected static List<ClusterKeySpec> toClusterKeys(SqlNodeList clusteredBy)
  {
    final List<ClusterKeySpec> keys = new ArrayList<>(clusteredBy.size());
    for (SqlNode node : clusteredBy) {
      if (!(node instanceof SqlIdentifier) || !((SqlIdentifier) node).isSimple()) {
        throw InvalidSqlInput.exception(
            "CLUSTERED BY column [%s] must be a column name; expressions, ordinals and DESC are not supported when"
            + " defining a table",
            node
        );
      }
      keys.add(new ClusterKeySpec(((SqlIdentifier) node).getSimple(), false));
    }
    return keys;
  }

  private static RelDataType resultType()
  {
    final RelDataTypeFactory typeFactory = DruidTypeSystem.TYPE_FACTORY;
    return typeFactory.createStructType(
        ImmutableList.of(Calcites.createSqlType(typeFactory, SqlTypeName.VARCHAR)),
        ImmutableList.of("RESULT")
    );
  }

  /**
   * {@code CREATE [OR REPLACE] TABLE [IF NOT EXISTS] ...}.
   */
  public static class CreateTableHandler extends CatalogDdlHandler
  {
    private final DruidSqlCreateTable createTable;
    private TableSpec tableSpec;

    public CreateTableHandler(SqlStatementHandler.HandlerContext handlerContext, DruidSqlCreateTable createTable)
    {
      super(handlerContext, createTable.getName());
      this.createTable = createTable;
    }

    @Override
    protected void validateStatement()
    {
      if (createTable.getReplace() && createTable.isIfNotExists()) {
        throw InvalidSqlInput.exception("Cannot specify both OR REPLACE and IF NOT EXISTS");
      }

      final List<ColumnSpec> columns = new ArrayList<>(createTable.getColumnList().size());
      final Set<String> seen = new HashSet<>();
      for (SqlNode node : createTable.getColumnList()) {
        final ColumnSpec column = toColumnSpec((DruidSqlColumnDeclaration) node);
        if (!seen.add(column.name())) {
          throw InvalidSqlInput.exception("Column [%s] is declared more than once", column.name());
        }
        columns.add(column);
      }

      final Map<String, Object> properties = new LinkedHashMap<>();
      if (createTable.getPartitionedBy() != null) {
        properties.put(
            DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY,
            toGranularityString(createTable.getPartitionedBy())
        );
      }
      if (createTable.getClusteredBy() != null) {
        properties.put(DatasourceDefn.CLUSTER_KEYS_PROPERTY, toClusterKeys(createTable.getClusteredBy()));
      }
      if (createTable.isSealed()) {
        properties.put(DatasourceDefn.SEALED_PROPERTY, true);
      }
      if (!createTable.getProjectionList().isEmpty()) {
        final List<DatasourceProjectionMetadata> projections =
            new ArrayList<>(createTable.getProjectionList().size());
        final Set<String> seenProjections = new HashSet<>();
        for (SqlNode node : createTable.getProjectionList()) {
          final SqlProjectionSpec projection = (SqlProjectionSpec) node;
          final String name = simpleName(projection.getName(), "Projection");
          if (!seenProjections.add(name)) {
            throw InvalidSqlInput.exception("Projection [%s] is declared more than once", name);
          }
          if (BASE_PROJECTION_NAME.equals(name)) {
            // SEALED is a choice, not a requirement: a column the table does not declare is appended after the
            // declared layout at ingest time; declaring SEALED rejects such columns instead.
            properties.put(
                DatasourceDefn.BASE_TABLE_PROPERTY,
                translateBaseTable(handlerContext, tableId.name(), columns, projection)
            );
          } else {
            projections.add(translateProjection(handlerContext, tableId.name(), columns, projection));
          }
        }
        if (!projections.isEmpty()) {
          properties.put(DatasourceDefn.PROJECTIONS_KEYS_PROPERTY, projections);
        }
      }

      tableSpec = new TableSpec(DatasourceDefn.TABLE_TYPE, properties, columns);
    }

    @Override
    protected void execute(CatalogTableWriter writer)
    {
      writer.createTable(tableId, tableSpec, createTable.isIfNotExists(), createTable.getReplace());
    }

    @Override
    protected String operationName()
    {
      return "CREATE TABLE";
    }
  }

  /**
   * {@code ALTER TABLE ... ADD COLUMN}. The Coordinator merges columns by name, which on its own would silently update
   * an existing column, so the write requires the column to be absent; that check belongs to the Coordinator's update
   * transaction rather than here, since checking first would race with a concurrent add of the same column.
   */
  public static class AddColumnHandler extends CatalogDdlHandler
  {
    private final DruidSqlAlterTable.AddColumn alterTable;
    private ColumnSpec column;

    public AddColumnHandler(SqlStatementHandler.HandlerContext handlerContext, DruidSqlAlterTable.AddColumn alterTable)
    {
      super(handlerContext, alterTable.getName());
      this.alterTable = alterTable;
    }

    @Override
    protected void validateStatement()
    {
      column = toColumnSpec(alterTable.getColumn());
    }

    @Override
    protected void execute(CatalogTableWriter writer)
    {
      writer.addColumns(tableId, Collections.singletonList(column));
    }

    @Override
    protected String operationName()
    {
      return "ALTER TABLE ADD COLUMN";
    }
  }

  /**
   * {@code ALTER TABLE ... DROP COLUMN}.
   */
  public static class DropColumnHandler extends CatalogDdlHandler
  {
    private final DruidSqlAlterTable.DropColumn alterTable;
    private String column;

    public DropColumnHandler(
        SqlStatementHandler.HandlerContext handlerContext,
        DruidSqlAlterTable.DropColumn alterTable
    )
    {
      super(handlerContext, alterTable.getName());
      this.alterTable = alterTable;
    }

    @Override
    protected void validateStatement()
    {
      column = simpleName(alterTable.getColumn(), "Column");
    }

    @Override
    protected void execute(CatalogTableWriter writer)
    {
      writer.dropColumns(tableId, Collections.singletonList(column));
    }

    @Override
    protected String operationName()
    {
      return "ALTER TABLE DROP COLUMN";
    }
  }

  /**
   * {@code ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE}. The mirror of {@link AddColumnHandler}: merging by name is
   * what changing a type requires, but a name the Coordinator does not find is appended, so a misspelled target would
   * create a column instead of failing. The write therefore requires the column to already exist.
   */
  public static class AlterColumnHandler extends CatalogDdlHandler
  {
    private final DruidSqlAlterTable.AlterColumn alterTable;
    private ColumnSpec column;

    public AlterColumnHandler(
        SqlStatementHandler.HandlerContext handlerContext,
        DruidSqlAlterTable.AlterColumn alterTable
    )
    {
      super(handlerContext, alterTable.getName());
      this.alterTable = alterTable;
    }

    @Override
    protected void validateStatement()
    {
      column = toColumnSpec(alterTable.getColumn());
    }

    @Override
    protected void execute(CatalogTableWriter writer)
    {
      writer.alterColumns(tableId, Collections.singletonList(column));
    }

    @Override
    protected String operationName()
    {
      return "ALTER TABLE ALTER COLUMN";
    }
  }

  /**
   * {@code ALTER TABLE ... ADD PROJECTION}. The body is translated against the table's current declared columns, so
   * the table must already have a catalog entry.
   */
  public static class AddProjectionHandler extends CatalogDdlHandler
  {
    private final DruidSqlAlterTable.AddProjection alterTable;
    private String projectionName;

    public AddProjectionHandler(
        SqlStatementHandler.HandlerContext handlerContext,
        DruidSqlAlterTable.AddProjection alterTable
    )
    {
      super(handlerContext, alterTable.getName());
      this.alterTable = alterTable;
    }

    @Override
    protected void validateStatement()
    {
      projectionName = simpleName(alterTable.getProjection().getName(), "Projection");
    }

    @Override
    protected void execute(CatalogTableWriter writer)
    {
      final TableMetadata existing = writer.readTable(tableId);
      if (existing == null) {
        throw InvalidSqlInput.exception("Table [%s] does not have a catalog entry", tableId.name());
      }
      final List<ColumnSpec> columns =
          existing.spec().columns() == null ? Collections.emptyList() : existing.spec().columns();

      if (BASE_PROJECTION_NAME.equals(projectionName)) {
        // The base table is a property of the table, not one of its projections, so it is set rather than appended.
        // Whether one is already defined is decided by the Coordinator inside its update transaction; checking it
        // from the read above would let two concurrent statements both find it absent.
        //
        // This read is still needed for the layout itself, which is derived from the declared columns. A concurrent
        // column change would make it stale, but the Coordinator validates the resulting spec against the columns it
        // commits against, so a stale layout is rejected rather than stored.
        writer.setBaseTable(
            tableId,
            translateBaseTable(handlerContext, tableId.name(), columns, alterTable.getProjection()),
            alterTable.isIfNotExists()
        );
        return;
      }

      writer.addProjection(
          tableId,
          translateProjection(handlerContext, tableId.name(), columns, alterTable.getProjection()),
          alterTable.isIfNotExists()
      );
    }

    @Override
    protected String operationName()
    {
      return "ALTER TABLE ADD PROJECTION";
    }
  }

  /**
   * {@code ALTER TABLE ... DROP PROJECTION}. Segments already built keep whatever projections they were built with;
   * this only stops future ingestion from building it.
   */
  public static class DropProjectionHandler extends CatalogDdlHandler
  {
    private final DruidSqlAlterTable.DropProjection alterTable;
    private String projectionName;

    public DropProjectionHandler(
        SqlStatementHandler.HandlerContext handlerContext,
        DruidSqlAlterTable.DropProjection alterTable
    )
    {
      super(handlerContext, alterTable.getName());
      this.alterTable = alterTable;
    }

    @Override
    protected void validateStatement()
    {
      projectionName = simpleName(alterTable.getProjectionName(), "Projection");
    }

    @Override
    protected void execute(CatalogTableWriter writer)
    {
      if (BASE_PROJECTION_NAME.equals(projectionName)) {
        // Removing the layout leaves the declared columns alone; only future segments are affected. Whether there is
        // one to remove is decided inside the Coordinator's update transaction, as for adding it.
        writer.dropBaseTable(tableId, alterTable.isIfExists());
        return;
      }
      writer.dropProjection(tableId, projectionName, alterTable.isIfExists());
    }

    @Override
    protected String operationName()
    {
      return "ALTER TABLE DROP PROJECTION";
    }
  }

  /**
   * {@code ALTER TABLE ... SET PROPERTIES}. A NULL value removes the property. The set of legal keys is not checked
   * here: the Coordinator's table definition registry is what knows them.
   */
  public static class SetPropertiesHandler extends CatalogDdlHandler
  {
    private final DruidSqlAlterTable.SetProperties alterTable;
    private Map<String, Object> properties;

    public SetPropertiesHandler(
        SqlStatementHandler.HandlerContext handlerContext,
        DruidSqlAlterTable.SetProperties alterTable
    )
    {
      super(handlerContext, alterTable.getName());
      this.alterTable = alterTable;
    }

    @Override
    protected void validateStatement()
    {
      properties = new LinkedHashMap<>();
      for (SqlNode node : alterTable.getProperties()) {
        final DruidSqlPropertyAssignment assignment = (DruidSqlPropertyAssignment) node;
        final String key = simpleName(assignment.getKey(), "Property");
        if (properties.containsKey(key)) {
          throw InvalidSqlInput.exception("Property [%s] is assigned more than once", key);
        }
        properties.put(key, propertyValue(key, assignment.getValue()));
      }
    }

    private static Object propertyValue(String key, SqlNode value)
    {
      if (!(value instanceof SqlLiteral)) {
        throw InvalidSqlInput.exception("Value for property [%s] must be a literal", key);
      }
      // A NULL literal coerces to null, which the catalog treats as "remove this property".
      return DruidSqlParser.sqlLiteralToJavaValue((SqlLiteral) value, "property " + key);
    }

    @Override
    protected void execute(CatalogTableWriter writer)
    {
      writer.updateProperties(tableId, properties);
    }

    @Override
    protected String operationName()
    {
      return "ALTER TABLE SET PROPERTIES";
    }
  }
}

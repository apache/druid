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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Iterables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.parser.DruidSqlIngest;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlParserUtils;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.parser.ExternalDestinationSqlIdentifier;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.destination.ExportDestination;
import org.apache.druid.sql.destination.IngestDestination;
import org.apache.druid.sql.destination.TableDestination;
import org.apache.druid.storage.ExportStorageProvider;

import java.util.List;
import java.util.regex.Pattern;

public abstract class IngestHandler extends QueryHandler
{
  private static final Pattern UNNAMED_COLUMN_PATTERN = Pattern.compile("^EXPR\\$\\d+$", Pattern.CASE_INSENSITIVE);

  protected final Granularity ingestionGranularity;
  protected IngestDestination targetDatasource;

  IngestHandler(
      HandlerContext handlerContext,
      DruidSqlIngest ingestNode,
      SqlNode queryNode,
      SqlExplain explain
  )
  {
    super(handlerContext, queryNode, explain);
    ingestionGranularity = ingestNode.getPartitionedBy() != null ? ingestNode.getPartitionedBy().getGranularity() : null;
    handlerContext.hook().captureInsert(ingestNode);
  }

  protected static SqlNode convertQuery(DruidSqlIngest sqlNode)
  {
    SqlNode query = sqlNode.getSource();

    // Check if ORDER BY clause is not provided to the underlying query
    if (query instanceof SqlOrderBy) {
      SqlOrderBy sqlOrderBy = (SqlOrderBy) query;
      SqlNodeList orderByList = sqlOrderBy.orderList;
      if (!(orderByList == null || orderByList.equals(SqlNodeList.EMPTY))) {
        throw InvalidSqlInput.exception(
            "Cannot use an ORDER BY clause on a Query of type [%s], use CLUSTERED BY instead",
            sqlNode.getOperator().getName()
        );
      }
    }
    if (sqlNode.getClusteredBy() != null) {
      query = DruidSqlParserUtils.convertClusterByToOrderBy(query, sqlNode.getClusteredBy());
    }

    if (!query.isA(SqlKind.QUERY)) {
      throw InvalidSqlInput.exception("Unexpected SQL statement type [%s], expected it to be a QUERY", query.getKind());
    }
    return query;
  }

  protected String operationName()
  {
    return ingestNode().getOperator().getName();
  }

  protected abstract DruidSqlIngest ingestNode();

  private void validateExport()
  {
    if (!handlerContext.plannerContext().featureAvailable(EngineFeature.WRITE_EXTERNAL_DATA)) {
      throw InvalidSqlInput.exception(
          "Writing to external sources are not supported by requested SQL engine [%s], consider using MSQ.",
          handlerContext.engine().name()
      );
    }

    if (ingestNode().getPartitionedBy() != null) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.UNSUPPORTED)
                          .build("Export statements do not support a PARTITIONED BY or CLUSTERED BY clause.");
    }

    final String exportFileFormat = ingestNode().getExportFileFormat();
    if (exportFileFormat == null) {
      throw InvalidSqlInput.exception(
          "Exporting rows into an EXTERN destination requires an AS clause to specify the format, but none was found.",
          operationName()
      );
    } else {
      handlerContext.plannerContext().queryContextMap().put(
          DruidSqlIngest.SQL_EXPORT_FILE_FORMAT,
          exportFileFormat
      );
    }
  }

  @Override
  public void validate()
  {
    if (ingestNode().getTargetTable() instanceof ExternalDestinationSqlIdentifier) {
      validateExport();
    } else {
      if (ingestNode().getPartitionedBy() == null) {
        throw InvalidSqlInput.exception(
            "Operation [%s] requires a PARTITIONED BY to be explicitly defined, but none was found.",
            operationName()
        );
      }

      if (ingestNode().getExportFileFormat() != null) {
        throw InvalidSqlInput.exception(
            "The AS <format> clause should only be specified while exporting rows into an EXTERN destination.",
            operationName()
        );
      }
    }

    try {
      PlannerContext plannerContext = handlerContext.plannerContext();
      if (ingestionGranularity != null) {
        plannerContext.queryContextMap().put(
            DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
            plannerContext.getJsonMapper().writeValueAsString(ingestionGranularity)
        );
      }
    }
    catch (JsonProcessingException e) {
      throw InvalidSqlInput.exception(e, "Invalid partition granularity [%s]", ingestionGranularity);
    }
    super.validate();
    // Check if CTX_SQL_OUTER_LIMIT is specified and fail the query if it is. CTX_SQL_OUTER_LIMIT being provided causes
    // the number of rows inserted to be limited which is likely to be confusing and unintended.
    if (handlerContext.queryContextMap().get(PlannerContext.CTX_SQL_OUTER_LIMIT) != null) {
      throw InvalidSqlInput.exception(
          "Context parameter [%s] cannot be provided on operator [%s]",
          PlannerContext.CTX_SQL_OUTER_LIMIT,
          operationName()
      );
    }
    targetDatasource = validateAndGetDataSourceForIngest();
  }

  @Override
  protected RelDataType returnedRowType()
  {
    final RelDataTypeFactory typeFactory = rootQueryRel.rel.getCluster().getTypeFactory();
    return handlerContext.engine().resultTypeForInsert(
        typeFactory,
        rootQueryRel.validatedRowType
    );
  }

  /**
   * Extract target destination from a {@link SqlInsert}, validates that the ingestion is of a form we support, and
   * adds the resource action required (if the destination is a druid datasource).
   * Expects the target datasource to be an unqualified name, a name qualified by the default schema or an external
   * destination.
   */
  private IngestDestination validateAndGetDataSourceForIngest()
  {
    final SqlInsert insert = ingestNode();
    if (insert.isUpsert()) {
      throw InvalidSqlInput.exception("UPSERT is not supported.");
    }

    if (insert.getTargetColumnList() != null) {
      throw InvalidSqlInput.exception(
          "Operation [%s] cannot be run with a target column list, given [%s (%s)]",
          operationName(),
          insert.getTargetTable(), insert.getTargetColumnList()
      );
    }

    final SqlIdentifier tableIdentifier = (SqlIdentifier) insert.getTargetTable();
    final IngestDestination dataSource;

    if (tableIdentifier.names.isEmpty()) {
      // I don't think this can happen, but include a branch for it just in case.
      throw DruidException.forPersona(DruidException.Persona.USER)
          .ofCategory(DruidException.Category.DEFENSIVE)
          .build("Operation [%s] requires a target table", operationName());
    } else if (tableIdentifier instanceof ExternalDestinationSqlIdentifier) {
      ExternalDestinationSqlIdentifier externalDestination = ((ExternalDestinationSqlIdentifier) tableIdentifier);
      ExportStorageProvider storageProvider = externalDestination.toExportStorageProvider(handlerContext.jsonMapper());
      dataSource = new ExportDestination(storageProvider);
      resourceActions.add(new ResourceAction(new Resource(externalDestination.getDestinationType(), ResourceType.EXTERNAL), Action.WRITE));
    } else if (tableIdentifier.names.size() == 1) {
      // Unqualified name.
      String tableName = Iterables.getOnlyElement(tableIdentifier.names);
      IdUtils.validateId("table", tableName);
      dataSource = new TableDestination(tableName);
      resourceActions.add(new ResourceAction(new Resource(tableName, ResourceType.DATASOURCE), Action.WRITE));
    } else {
      // Qualified name.
      final String defaultSchemaName =
          Iterables.getOnlyElement(CalciteSchema.from(handlerContext.defaultSchema()).path(null));

      if (tableIdentifier.names.size() == 2 && defaultSchemaName.equals(tableIdentifier.names.get(0))) {
        String tableName = tableIdentifier.names.get(1);
        IdUtils.validateId("table", tableName);
        dataSource = new TableDestination(tableName);
        resourceActions.add(new ResourceAction(new Resource(tableName, ResourceType.DATASOURCE), Action.WRITE));
      } else {
        throw InvalidSqlInput.exception(
            "Table [%s] does not support operation [%s] because it is not a Druid datasource",
            tableIdentifier,
            operationName()
        );
      }
    }

    return dataSource;
  }

  @Override
  protected PlannerResult planForDruid() throws ValidationException
  {
    return planWithDruidConvention();
  }

  @Override
  protected QueryMaker buildQueryMaker(final RelRoot rootQueryRel) throws ValidationException
  {
    validateColumnsForIngestion(rootQueryRel);
    return handlerContext.engine().buildQueryMakerForInsert(
        targetDatasource,
        rootQueryRel,
        handlerContext.plannerContext()
    );
  }

  private void validateColumnsForIngestion(RelRoot rootQueryRel)
  {
    // Check that there are no unnamed columns in the insert.
    for (Pair<Integer, String> field : rootQueryRel.fields) {
      if (UNNAMED_COLUMN_PATTERN.matcher(field.right).matches()) {
        throw InvalidSqlInput.exception(
            "Insertion requires columns to be named, but at least one of the columns was unnamed.  This is usually "
            + "the result of applying a function without having an AS clause, please ensure that all function calls"
            + "are named with an AS clause as in \"func(X) as myColumn\"."
        );
      }
    }
  }

  /**
   * Handler for the INSERT statement.
   */
  protected static class InsertHandler extends IngestHandler
  {
    private final DruidSqlInsert sqlNode;

    public InsertHandler(
        SqlStatementHandler.HandlerContext handlerContext,
        DruidSqlInsert sqlNode,
        SqlExplain explain
    )
    {
      super(
          handlerContext,
          sqlNode,
          convertQuery(sqlNode),
          explain
      );
      this.sqlNode = sqlNode;
    }

    @Override
    protected DruidSqlIngest ingestNode()
    {
      return sqlNode;
    }

    @Override
    public void validate()
    {
      if (!handlerContext.plannerContext().featureAvailable(EngineFeature.CAN_INSERT)) {
        throw InvalidSqlInput.exception(
            "INSERT operations are not supported by requested SQL engine [%s], consider using MSQ.",
            handlerContext.engine().name()
        );
      }
      super.validate();
    }

    @Override
    public ExplainAttributes explainAttributes()
    {
      return new ExplainAttributes(
          DruidSqlInsert.OPERATOR.getName(),
          targetDatasource,
          ingestionGranularity,
          DruidSqlParserUtils.resolveClusteredByColumnsToOutputColumns(sqlNode.getClusteredBy(), rootQueryRel.fields),
          null
      );
    }
  }

  /**
   * Handler for the REPLACE statement.
   */
  protected static class ReplaceHandler extends IngestHandler
  {
    private final DruidSqlReplace sqlNode;
    private String replaceIntervals;

    public ReplaceHandler(
        SqlStatementHandler.HandlerContext handlerContext,
        DruidSqlReplace sqlNode,
        SqlExplain explain
    )
    {
      super(
          handlerContext,
          sqlNode,
          convertQuery(sqlNode),
          explain
      );
      this.sqlNode = sqlNode;
    }

    @Override
    protected DruidSqlIngest ingestNode()
    {
      return sqlNode;
    }

    @Override
    public void validate()
    {
      if (ingestNode().getTargetTable() instanceof ExternalDestinationSqlIdentifier) {
        throw InvalidSqlInput.exception(
            "REPLACE operations do no support EXTERN destinations. Use INSERT statements to write to an external destination."
        );
      }
      if (!handlerContext.plannerContext().featureAvailable(EngineFeature.CAN_REPLACE)) {
        throw InvalidSqlInput.exception(
            "REPLACE operations are not supported by the requested SQL engine [%s].  Consider using MSQ.",
            handlerContext.engine().name()
        );
      }
      SqlNode replaceTimeQuery = sqlNode.getReplaceTimeQuery();
      if (replaceTimeQuery == null) {
        throw InvalidSqlInput.exception(
            "Missing time chunk information in OVERWRITE clause for REPLACE. Use "
            + "OVERWRITE WHERE <__time based condition> or OVERWRITE ALL to overwrite the entire table."
        );
      }

      List<String> replaceIntervalsList = DruidSqlParserUtils.validateQueryAndConvertToIntervals(
          replaceTimeQuery,
          ingestionGranularity,
          handlerContext.timeZone()
      );
      super.validate();
      if (replaceIntervalsList != null) {
        replaceIntervals = String.join(",", replaceIntervalsList);
        handlerContext.queryContextMap().put(
            DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS,
            replaceIntervals
        );
      }
    }

    @Override
    public ExplainAttributes explainAttributes()
    {
      return new ExplainAttributes(
          DruidSqlReplace.OPERATOR.getName(),
          targetDatasource,
          ingestionGranularity,
          DruidSqlParserUtils.resolveClusteredByColumnsToOutputColumns(sqlNode.getClusteredBy(), rootQueryRel.fields),
          replaceIntervals
      );
    }
  }
}

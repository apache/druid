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
import com.google.common.annotations.VisibleForTesting;
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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.parser.DruidSqlIngest;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlParserUtils;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;

import java.util.List;
import java.util.regex.Pattern;

public abstract class IngestHandler extends QueryHandler
{
  private static final Pattern UNNAMED_COLUMN_PATTERN = Pattern.compile("^EXPR\\$\\d+$", Pattern.CASE_INSENSITIVE);
  @VisibleForTesting
  public static final String UNNAMED_INGESTION_COLUMN_ERROR =
      "Cannot ingest expressions that do not have an alias "
          + "or columns with names like EXPR$[digit].\n"
          + "E.g. if you are ingesting \"func(X)\", then you can rewrite it as "
          + "\"func(X) as myColumn\"";

  protected final Granularity ingestionGranularity;
  protected String targetDatasource;

  IngestHandler(
      HandlerContext handlerContext,
      DruidSqlIngest ingestNode,
      SqlNode queryNode,
      SqlExplain explain
  )
  {
    super(handlerContext, queryNode, explain);
    this.ingestionGranularity = ingestNode.getPartitionedBy();
  }

  protected static SqlNode convertQuery(DruidSqlIngest sqlNode) throws ValidationException
  {
    SqlNode query = sqlNode.getSource();

    // Check if ORDER BY clause is not provided to the underlying query
    if (query instanceof SqlOrderBy) {
      SqlOrderBy sqlOrderBy = (SqlOrderBy) query;
      SqlNodeList orderByList = sqlOrderBy.orderList;
      if (!(orderByList == null || orderByList.equals(SqlNodeList.EMPTY))) {
        String opName = sqlNode.getOperator().getName();
        throw new ValidationException(StringUtils.format(
            "Cannot have ORDER BY on %s %s statement, use CLUSTERED BY instead.",
            "INSERT".equals(opName) ? "an" : "a",
            opName
        ));
      }
    }
    if (sqlNode.getClusteredBy() != null) {
      query = DruidSqlParserUtils.convertClusterByToOrderBy(query, sqlNode.getClusteredBy());
    }

    if (!query.isA(SqlKind.QUERY)) {
      throw new ValidationException(StringUtils.format("Cannot execute [%s].", query.getKind()));
    }
    return query;
  }

  protected String operationName()
  {
    return ingestNode().getOperator().getName();
  }

  protected abstract DruidSqlIngest ingestNode();

  @Override
  public void validate() throws ValidationException
  {
    if (ingestNode().getPartitionedBy() == null) {
      throw new ValidationException(StringUtils.format(
          "%s statements must specify PARTITIONED BY clause explicitly",
          operationName()
      ));
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
      throw new ValidationException("Unable to serialize partition granularity.");
    }
    super.validate();
    // Check if CTX_SQL_OUTER_LIMIT is specified and fail the query if it is. CTX_SQL_OUTER_LIMIT being provided causes
    // the number of rows inserted to be limited which is likely to be confusing and unintended.
    if (handlerContext.queryContextMap().get(PlannerContext.CTX_SQL_OUTER_LIMIT) != null) {
      throw new ValidationException(
          StringUtils.format(
              "%s cannot be provided with %s.",
              PlannerContext.CTX_SQL_OUTER_LIMIT,
              operationName()
          )
      );
    }
    targetDatasource = validateAndGetDataSourceForIngest();
    resourceActions.add(new ResourceAction(new Resource(targetDatasource, ResourceType.DATASOURCE), Action.WRITE));
  }

  @Override
  protected RelDataType returnedRowType()
  {
    final RelDataTypeFactory typeFactory = rootQueryRel.rel.getCluster().getTypeFactory();
    return handlerContext.engine().resultTypeForInsert(
        typeFactory,
        rootQueryRel.validatedRowType);
  }

  /**
   * Extract target datasource from a {@link SqlInsert}, and also validate that the ingestion is of a form we support.
   * Expects the target datasource to be either an unqualified name, or a name qualified by the default schema.
   */
  private String validateAndGetDataSourceForIngest() throws ValidationException
  {
    final SqlInsert insert = ingestNode();
    if (insert.isUpsert()) {
      throw new ValidationException("UPSERT is not supported.");
    }

    if (insert.getTargetColumnList() != null) {
      throw new ValidationException(operationName() + " with a target column list is not supported.");
    }

    final SqlIdentifier tableIdentifier = (SqlIdentifier) insert.getTargetTable();
    final String dataSource;

    if (tableIdentifier.names.isEmpty()) {
      // I don't think this can happen, but include a branch for it just in case.
      throw new ValidationException(operationName() + " requires a target table.");
    } else if (tableIdentifier.names.size() == 1) {
      // Unqualified name.
      dataSource = Iterables.getOnlyElement(tableIdentifier.names);
    } else {
      // Qualified name.
      final String defaultSchemaName =
          Iterables.getOnlyElement(CalciteSchema.from(handlerContext.defaultSchema()).path(null));

      if (tableIdentifier.names.size() == 2 && defaultSchemaName.equals(tableIdentifier.names.get(0))) {
        dataSource = tableIdentifier.names.get(1);
      } else {
        throw new ValidationException(
            StringUtils.format(
                "Cannot %s into %s because it is not a Druid datasource.",
                operationName(),
                tableIdentifier
            )
        );
      }
    }

    try {
      IdUtils.validateId(operationName() + " dataSource", dataSource);
    }
    catch (IllegalArgumentException e) {
      throw new ValidationException(e.getMessage());
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
        handlerContext.plannerContext());
  }

  private void validateColumnsForIngestion(RelRoot rootQueryRel) throws ValidationException
  {
    // Check that there are no unnamed columns in the insert.
    for (Pair<Integer, String> field : rootQueryRel.fields) {
      if (UNNAMED_COLUMN_PATTERN.matcher(field.right).matches()) {
        throw new ValidationException(UNNAMED_INGESTION_COLUMN_ERROR);
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
    ) throws ValidationException
    {
      super(
          handlerContext,
          sqlNode,
          convertQuery(sqlNode),
          explain);
      this.sqlNode = sqlNode;
    }

    @Override
    public SqlNode sqlNode()
    {
      return sqlNode;
    }

    @Override
    protected DruidSqlIngest ingestNode()
    {
      return sqlNode;
    }

    @Override
    public void validate() throws ValidationException
    {
      if (!handlerContext.plannerContext().engineHasFeature(EngineFeature.CAN_INSERT)) {
        throw new ValidationException(StringUtils.format(
            "Cannot execute INSERT with SQL engine '%s'.",
            handlerContext.engine().name())
        );
      }
      super.validate();
    }
  }

  /**
   * Handler for the REPLACE statement.
   */
  protected static class ReplaceHandler extends IngestHandler
  {
    private final DruidSqlReplace sqlNode;
    private List<String> replaceIntervals;

    public ReplaceHandler(
        SqlStatementHandler.HandlerContext handlerContext,
        DruidSqlReplace sqlNode,
        SqlExplain explain
    ) throws ValidationException
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
    public SqlNode sqlNode()
    {
      return sqlNode;
    }

    @Override
    protected DruidSqlIngest ingestNode()
    {
      return sqlNode;
    }

    @Override
    public void validate() throws ValidationException
    {
      if (!handlerContext.plannerContext().engineHasFeature(EngineFeature.CAN_REPLACE)) {
        throw new ValidationException(StringUtils.format(
            "Cannot execute REPLACE with SQL engine '%s'.",
            handlerContext.engine().name())
        );
      }
      SqlNode replaceTimeQuery = sqlNode.getReplaceTimeQuery();
      if (replaceTimeQuery == null) {
        throw new ValidationException("Missing time chunk information in OVERWRITE clause for REPLACE. Use "
            + "OVERWRITE WHERE <__time based condition> or OVERWRITE ALL to overwrite the entire table.");
      }

      replaceIntervals = DruidSqlParserUtils.validateQueryAndConvertToIntervals(
          replaceTimeQuery,
          ingestionGranularity,
          handlerContext.timeZone());
      super.validate();
      if (replaceIntervals != null) {
        handlerContext.queryContextMap().put(
            DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS,
            String.join(",", replaceIntervals)
        );
      }
    }
  }
}

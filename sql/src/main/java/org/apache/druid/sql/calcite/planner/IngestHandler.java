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
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlParserUtils;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.joda.time.DateTimeZone;

import java.util.List;

/**
 * Handler for ingestion: base class for INSERT and REPLACE.
 */
abstract class IngestHandler extends QueryHandler
{
  protected Granularity ingestionGranularity;
  protected String targetDataSource;

  IngestHandler(HandlerContext handlerContext, SqlInsert insertNode, SqlExplain explain)
  {
    super(handlerContext, insertNode.getSource(), explain);
  }

  @Override
  protected boolean allowsBindableExec()
  {
    return false;
  }

  protected abstract SqlInsert ingestNode();

  protected void analyzeIngest() throws ValidationException
  {
    try {
      if (ingestionGranularity != null) {
        handlerContext.queryContext().addSystemParam(
            DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
            handlerContext.jsonMapper().writeValueAsString(ingestionGranularity)
        );
      }
    }
    catch (JsonProcessingException e) {
      throw new ValidationException("Unable to serialize partition granularity.");
    }

    // Check if ORDER BY clause is not provided to the underlying query
    if (queryNode instanceof SqlOrderBy) {
      SqlOrderBy sqlOrderBy = (SqlOrderBy) queryNode;
      SqlNodeList orderByList = sqlOrderBy.orderList;
      if (!(orderByList == null || orderByList.equals(SqlNodeList.EMPTY))) {
        String queryType = ingestNode() instanceof DruidSqlReplace ? "a REPLACE" : "an INSERT";
        throw new ValidationException(
            "Cannot have ORDER BY on " + queryType + " query, use CLUSTERED BY instead.");
      }
    }
    targetDataSource = validateAndGetDataSourceForIngest(ingestNode());
    resourceActions.add(new ResourceAction(new Resource(targetDataSource, ResourceType.DATASOURCE), Action.WRITE));
  }

  /**
   * Extract target datasource from a {@link SqlInsert}, and also validate that the ingestion is of a form we support.
   * Expects the target datasource to be either an unqualified name, or a name qualified by the default schema.
   */
  private String validateAndGetDataSourceForIngest(final SqlInsert insert) throws ValidationException
  {
    final String operatorName = insert.getOperator().getName();
    if (insert.isUpsert()) {
      throw new ValidationException("UPSERT is not supported.");
    }

    if (insert.getTargetColumnList() != null) {
      throw new ValidationException(operatorName + " with target column list is not supported.");
    }

    final SqlIdentifier tableIdentifier = (SqlIdentifier) insert.getTargetTable();
    final String dataSource;

    if (tableIdentifier.names.isEmpty()) {
      // I don't think this can happen, but include a branch for it just in case.
      throw new ValidationException(operatorName + " requires target table.");
    } else if (tableIdentifier.names.size() == 1) {
      // Unqualified name.
      dataSource = Iterables.getOnlyElement(tableIdentifier.names);
    } else {
      // Qualified name.
      final String defaultSchemaName =
          Iterables.getOnlyElement(CalciteSchema.from(handlerContext.frameworkConfig().getDefaultSchema()).path(null));

      if (tableIdentifier.names.size() == 2 && defaultSchemaName.equals(tableIdentifier.names.get(0))) {
        dataSource = tableIdentifier.names.get(1);
      } else {
        throw new ValidationException(
            StringUtils.format("Cannot %s into [%s] because it is not a Druid datasource.", operatorName, tableIdentifier)
        );
      }
    }

    try {
      IdUtils.validateId(operatorName + " dataSource", dataSource);
    }
    catch (IllegalArgumentException e) {
      throw new ValidationException(e.getMessage());
    }

    return dataSource;
  }

  protected void verifyQuery(SqlNodeList clusteredBy) throws ValidationException
  {
    if (clusteredBy != null) {
      queryNode = DruidSqlParserUtils.convertClusterByToOrderBy(queryNode, clusteredBy);
    }

    if (!queryNode.isA(SqlKind.QUERY)) {
      throw new ValidationException(StringUtils.format("Cannot execute [%s].", queryNode.getKind()));
    }
  }

  @Override
  protected QueryMaker buildQueryMaker(final RelRoot rootQueryRel) throws ValidationException
  {
    validateColumnsForIngestion(rootQueryRel);
    return handlerContext.queryMakerFactory().buildForInsert(
        targetDataSource,
        rootQueryRel,
        handlerContext.plannerContext());
  }

  private void validateColumnsForIngestion(RelRoot rootQueryRel) throws ValidationException
  {
    // Check that there are no unnamed columns in the insert.
    for (Pair<Integer, String> field : rootQueryRel.fields) {
      if (DruidPlanner.UNNAMED_COLUMN_PATTERN.matcher(field.right).matches()) {
        throw new ValidationException(DruidPlanner.UNNAMED_INGESTION_COLUMN_ERROR);
      }
    }
  }

  /**
   * Handler for the INSERT statement.
   */
  protected static class InsertHandler extends IngestHandler
  {
    private final DruidSqlInsert sqlNode;

    public InsertHandler(HandlerContext handlerContext, DruidSqlInsert sqlNode, SqlExplain explain)
    {
      super(handlerContext, sqlNode, explain);
      this.sqlNode = sqlNode;
    }

    @Override
    protected SqlNode sqlNode()
    {
      return sqlNode;
    }

    @Override
    protected SqlInsert ingestNode()
    {
      return sqlNode;
    }

    @Override
    public void analyze() throws ValidationException
    {
      ingestionGranularity = sqlNode.getPartitionedBy();
      analyzeIngest();
      verifyQuery(sqlNode.getClusteredBy());
      validateQuery();
    }
  }

  /**
   * Handler for the REPLACE statement.
   */
  protected static class ReplaceHandler extends IngestHandler
  {
    private final DruidSqlReplace sqlNode;
    private final DateTimeZone timeZone;
    private List<String> replaceIntervals;

    public ReplaceHandler(
        HandlerContext handlerContext,
        DruidSqlReplace sqlNode,
        DateTimeZone timeZone,
        SqlExplain explain
    )
    {
      super(handlerContext, sqlNode, explain);
      this.sqlNode = sqlNode;
      this.timeZone = timeZone;
    }

    @Override
    protected SqlNode sqlNode()
    {
      return sqlNode;
    }

    @Override
    protected SqlInsert ingestNode()
    {
      return sqlNode;
    }

    @Override
    public void analyze() throws ValidationException
    {
      ingestionGranularity = sqlNode.getPartitionedBy();
      analyzeIngest();

      SqlNode replaceTimeQuery = sqlNode.getReplaceTimeQuery();
      if (replaceTimeQuery == null) {
        throw new ValidationException(
            "Missing time chunk information in OVERWRITE clause for REPLACE, " +
            "set it to OVERWRITE WHERE <__time based condition> or set it to " +
            "overwrite the entire table with OVERWRITE ALL.");
      }

      replaceIntervals = DruidSqlParserUtils.validateQueryAndConvertToIntervals(
          replaceTimeQuery,
          ingestionGranularity,
          timeZone);
      if (replaceIntervals != null) {
        handlerContext.queryContext().addSystemParam(
            DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS,
            String.join(",", replaceIntervals)
        );
      }

      verifyQuery(sqlNode.getClusteredBy());
      validateQuery();
    }
  }
}

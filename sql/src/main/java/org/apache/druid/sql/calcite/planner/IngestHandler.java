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

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.sql.calcite.parser.DruidSqlIngest;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlParserUtils;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;

import java.util.List;

public abstract class IngestHandler extends QueryHandler
{
  protected Granularity ingestionGranularity;
  protected String targetDatasource;
  private SqlNode validatedQueryNode;
  private RelDataType targetType;

  IngestHandler(
      final HandlerContext handlerContext,
      final SqlExplain explain
  )
  {
    super(handlerContext, explain);
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

  @Override
  public void validate()
  {
    // Check if CTX_SQL_OUTER_LIMIT is specified and fail the query if it is. CTX_SQL_OUTER_LIMIT being provided causes
    // the number of rows inserted to be limited which is likely to be confusing and unintended.
    if (handlerContext.queryContextMap().get(PlannerContext.CTX_SQL_OUTER_LIMIT) != null) {
      throw InvalidSqlInput.exception(
          "Context parameter [%s] cannot be provided on operator [%s]",
          PlannerContext.CTX_SQL_OUTER_LIMIT,
          operationName()
      );
    }
    DruidSqlIngest ingestNode = ingestNode();
    DruidSqlIngest validatedNode = (DruidSqlIngest) validate(ingestNode);
    validatedQueryNode = validatedNode.getSource();
    CalcitePlanner planner = handlerContext.planner();
    final SqlValidator validator = planner.getValidator();
    targetType = validator.getValidatedNodeType(validatedNode);
    ingestionGranularity = DruidSqlParserUtils.convertSqlNodeToGranularity(ingestNode().getPartitionedBy());
    final SqlIdentifier tableIdentifier = (SqlIdentifier) ingestNode().getTargetTable();
    targetDatasource = tableIdentifier.names.get(tableIdentifier.names.size() - 1);
  }

  @Override
  protected SqlNode validatedQueryNode()
  {
    return validatedQueryNode;
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

  @Override
  protected PlannerResult planForDruid() throws ValidationException
  {
    return planWithDruidConvention();
  }

  @Override
  protected QueryMaker buildQueryMaker(final RelRoot rootQueryRel) throws ValidationException
  {
    final SqlIdentifier tableIdentifier = (SqlIdentifier) ingestNode().getTargetTable();
    String targetDatasource = tableIdentifier.names.get(tableIdentifier.names.size() - 1);
    return handlerContext.engine().buildQueryMakerForInsert(
        targetDatasource,
        rootQueryRel,
        handlerContext.plannerContext(),
        targetType
    );
  }

  /**
   * Handler for the INSERT statement.
   */
  protected static class InsertHandler extends IngestHandler
  {

    private final DruidSqlInsert sqlNode;

    public InsertHandler(
        SqlStatementHandler.HandlerContext handlerContext,
        DruidSqlInsert insertNode,
        SqlExplain explain
    )
    {
      super(handlerContext, explain);
      this.sqlNode = insertNode;
      handlerContext.hook().captureInsert(insertNode);
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
          DruidSqlIngestOperator.INSERT_OPERATOR.getName(),
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
      super(handlerContext, explain);
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
          DruidSqlIngestOperator.REPLACE_OPERATOR.getName(),
          targetDatasource,
          ingestionGranularity,
          DruidSqlParserUtils.resolveClusteredByColumnsToOutputColumns(sqlNode.getClusteredBy(), rootQueryRel.fields),
          replaceIntervals
      );
    }
  }
}

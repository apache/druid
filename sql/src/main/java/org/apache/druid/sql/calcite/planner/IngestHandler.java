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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.java.util.common.StringUtils;
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
  private SqlNode validatedQueryNode;
  private RelDataType targetType;

  IngestHandler(
      final HandlerContext handlerContext,
      final SqlExplain explain
  )
  {
    super(handlerContext, explain);
  }

  protected String operationName()
  {
    return ingestNode().getOperator().getName();
  }

  protected abstract DruidSqlIngest ingestNode();

  @Override
  public void validate() throws ValidationException
  {
    // Check if CTX_SQL_OUTER_LIMIT is specified and fail the query if it is. CTX_SQL_OUTER_LIMIT being provided causes
    // the number of rows inserted to be limited which is likely to be confusing and unintended.
    if (handlerContext.queryContextMap().get(PlannerContext.CTX_SQL_OUTER_LIMIT) != null) {
      throw new ValidationException(
          StringUtils.format(
              "%s context parameter cannot be provided with %s.",
              PlannerContext.CTX_SQL_OUTER_LIMIT,
              operationName()
          )
      );
    }
    DruidSqlIngest ingestNode = ingestNode();
    DruidSqlIngest validatedNode = (DruidSqlIngest) validate(ingestNode);
    validatedQueryNode = validatedNode.getSource();
    CalcitePlanner planner = handlerContext.planner();
    final SqlValidator validator = planner.getValidator();
    targetType = validator.getValidatedNodeType(validatedNode);
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
        rootQueryRel.validatedRowType);
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
        handlerContext.plannerContext()
    );
  }

  /**
   * Handler for the INSERT statement.
   */
  protected static class InsertHandler extends IngestHandler
  {
    private final DruidSqlInsert sqlNode;

    public InsertHandler(
        HandlerContext handlerContext,
        DruidSqlInsert insertNode,
        SqlExplain explain
    )
    {
      super(
          handlerContext,
          sqlNode,
          convertQuery(sqlNode),
          explain);
      this.sqlNode = sqlNode;
      handlerContext.hook().captureInsert(insertNode);
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
        DruidSqlReplace replaceNode,
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
      handlerContext.hook().captureInsert(replaceNode);
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

      final Granularity ingestionGranularity = DruidSqlParserUtils.convertSqlNodeToGranularity(ingestNode().getPartitionedBy());
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

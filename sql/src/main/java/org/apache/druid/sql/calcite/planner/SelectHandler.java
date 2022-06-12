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
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.sql.calcite.run.QueryMaker;

/**
 * Handler for the SELECT statement.
 */
class SelectHandler extends QueryHandler
{
  private final SqlNode sqlNode;

  public SelectHandler(HandlerContext handlerContext, SqlNode sqlNode, SqlExplain explain)
  {
    super(handlerContext, sqlNode, explain);
    this.sqlNode = sqlNode;
  }

  @Override
  protected boolean allowsBindableExec()
  {
    return true;
  }

  @Override
  protected SqlNode sqlNode()
  {
    return sqlNode;
  }

  @Override
  public void analyze() throws ValidationException
  {
    validateQuery();
  }

  @Override
  protected QueryMaker buildQueryMaker(final RelRoot rootQueryRel) throws ValidationException
  {
    return handlerContext.queryMakerFactory().buildForSelect(
        rootQueryRel,
        handlerContext.plannerContext());
  }
}

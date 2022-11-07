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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.query.QueryContext;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.joda.time.DateTimeZone;

import java.util.Map;
import java.util.Set;

/**
 * Handler for a SQL statement. Follows the same lifecycle as the planner,
 * however this class handles one specific kind of SQL statement.
 */
public interface SqlStatementHandler
{
  SqlNode sqlNode();
  void validate() throws ValidationException;
  Set<ResourceAction> resourceActions();
  void prepare();
  PrepareResult prepareResult();
  PlannerResult plan() throws ValidationException;

  /**
   * Context available to statement handlers.
   */
  interface HandlerContext
  {
    PlannerContext plannerContext();
    SqlEngine engine();
    CalcitePlanner planner();
    QueryContext queryContext();
    Map<String, Object> queryContextMap();
    SchemaPlus defaultSchema();
    ObjectMapper jsonMapper();
    DateTimeZone timeZone();
  }

  abstract class BaseStatementHandler implements SqlStatementHandler
  {
    protected final HandlerContext handlerContext;
    protected Set<ResourceAction> resourceActions;

    protected BaseStatementHandler(HandlerContext handlerContext)
    {
      this.handlerContext = handlerContext;
    }

    @Override
    public Set<ResourceAction> resourceActions()
    {
      return resourceActions;
    }
  }
}

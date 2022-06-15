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

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.planner.DruidPlanner.SqlStatementHandler;

import java.util.HashSet;
import java.util.Set;

/**
 * SQL-statement-specific behavior. Each statement follows the same
 * lifecycle: analyze, followed by either prepare or plan (execute).
 */
abstract class BaseStatementHandler implements SqlStatementHandler
{
  protected final HandlerContext handlerContext;
  protected final Set<ResourceAction> resourceActions = new HashSet<>();

  protected BaseStatementHandler(HandlerContext handlerContext)
  {
    this.handlerContext = handlerContext;
  }

  protected abstract SqlNode sqlNode();

  protected SqlNode validateNode(SqlNode node) throws ValidationException
  {
    try {
      return handlerContext.planner().validate(node);
    }
    catch (RuntimeException e) {
      throw new ValidationException(e);
    }
  }

  /**
   * Uses {@link SqlParameterizerShuttle} to rewrite {@link SqlNode} to swap out any
   * {@link org.apache.calcite.sql.SqlDynamicParam} early for their {@link SqlLiteral}
   * replacement
   * @throws ValidationException
   */
  protected SqlNode rewriteDynamicParameters(SqlNode parsed) throws ValidationException
  {
    if (handlerContext.parameters().isEmpty()) {
      return parsed;
    }
    try {
      // Uses {@link SqlParameterizerShuttle} to rewrite {@link SqlNode} to swap out any
      // {@link org.apache.calcite.sql.SqlDynamicParam} early for their {@link SqlLiteral}
      // replacement.
      //
      // Parameter replacement is done only if the client provides parameter values.
      // If this is a PREPARE-only, then there will be no values even if the statement contains
      // parameters. If this is a PLAN, then we'll catch later the case that the statement
      // contains parameters, but no values were provided.
      return parsed.accept(
          new SqlParameterizerShuttle(
              handlerContext.plannerContext()));
    }
    catch (RuntimeException e) {
      throw new ValidationException(e);
    }
  }

  @Override
  public Set<ResourceAction> resourceActions()
  {
    return resourceActions;
  }
}

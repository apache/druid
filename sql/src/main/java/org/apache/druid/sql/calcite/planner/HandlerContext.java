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
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.druid.query.QueryContext;
import org.apache.druid.sql.calcite.run.QueryMakerFactory;

import java.util.List;

/**
 * Resources required by statement handlers.
 */
class HandlerContext
{
  private final PlannerContext plannerContext;
  private final CalcitePlanner planner;
  private final QueryMakerFactory queryMakerFactory;

  public HandlerContext(
      final PlannerContext plannerContext,
      final CalcitePlanner planner,
      final QueryMakerFactory queryMakerFactory)
  {
    this.plannerContext = plannerContext;
    this.planner = planner;
    this.queryMakerFactory = queryMakerFactory;
  }

  protected PlannerContext plannerContext()
  {
    return plannerContext;
  }

  protected FrameworkConfig frameworkConfig()
  {
    return planner.frameworkConfig();
  }

  protected CalcitePlanner planner()
  {
    return planner;
  }

  public List<TypedValue> parameters()
  {
    return plannerContext.getParameters();
  }

  public QueryContext queryContext()
  {
    return plannerContext.getQueryContext();
  }

  public ObjectMapper jsonMapper()
  {
    return plannerContext.getJsonMapper();
  }

  public QueryMakerFactory queryMakerFactory()
  {
    return queryMakerFactory;
  }
}

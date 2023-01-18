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
import com.google.common.base.Preconditions;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;

public class SimplePlannerToolbox implements PlannerToolbox
{
  private final DruidOperatorTable operatorTable;
  private final ExprMacroTable macroTable;
  private final JoinableFactoryWrapper joinableFactoryWrapper;
  private final ObjectMapper jsonMapper;
  private final PlannerConfig plannerConfig;
  private final DruidSchemaCatalog rootSchema;
  private final CatalogResolver catalog;

  public SimplePlannerToolbox(
      final DruidOperatorTable operatorTable,
      final ExprMacroTable macroTable,
      final ObjectMapper jsonMapper,
      final PlannerConfig plannerConfig,
      final DruidSchemaCatalog rootSchema,
      final JoinableFactoryWrapper joinableFactoryWrapper,
      final CatalogResolver catalog
  )
  {
    this.operatorTable = operatorTable;
    this.macroTable = macroTable;
    this.jsonMapper = jsonMapper;
    this.plannerConfig = Preconditions.checkNotNull(plannerConfig, "plannerConfig");
    this.rootSchema = rootSchema;
    this.joinableFactoryWrapper = joinableFactoryWrapper;
    this.catalog = catalog;
  }

  @Override
  public DruidOperatorTable operatorTable()
  {
    return operatorTable;
  }

  @Override
  public ExprMacroTable exprMacroTable()
  {
    return macroTable;
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return jsonMapper;
  }

  @Override
  public PlannerConfig plannerConfig()
  {
    return plannerConfig;
  }

  @Override
  public DruidSchemaCatalog rootSchema()
  {
    return rootSchema;
  }

  @Override
  public JoinableFactoryWrapper joinableFactoryWrapper()
  {
    return joinableFactoryWrapper;
  }

  @Override
  public CatalogResolver catalogResolver()
  {
    return catalog;
  }

  @Override
  public String druidSchemaName()
  {
    return "druid";
  }
}

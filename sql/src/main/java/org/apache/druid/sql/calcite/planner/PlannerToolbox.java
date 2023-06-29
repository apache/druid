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
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;

public class PlannerToolbox
{
  protected final DruidOperatorTable operatorTable;
  protected final ExprMacroTable macroTable;
  protected final JoinableFactoryWrapper joinableFactoryWrapper;
  protected final ObjectMapper jsonMapper;
  protected final PlannerConfig plannerConfig;
  protected final DruidSchemaCatalog rootSchema;
  protected final CatalogResolver catalog;
  protected final String druidSchemaName;
  protected final CalciteRulesManager calciteRuleManager;
  protected final AuthorizerMapper authorizerMapper;
  protected final AuthConfig authConfig;

  public PlannerToolbox(
      final DruidOperatorTable operatorTable,
      final ExprMacroTable macroTable,
      final ObjectMapper jsonMapper,
      final PlannerConfig plannerConfig,
      final DruidSchemaCatalog rootSchema,
      final JoinableFactoryWrapper joinableFactoryWrapper,
      final CatalogResolver catalog,
      final String druidSchemaName,
      final CalciteRulesManager calciteRuleManager,
      final AuthorizerMapper authorizerMapper,
      final AuthConfig authConfig
  )
  {
    this.operatorTable = operatorTable;
    this.macroTable = macroTable;
    this.jsonMapper = jsonMapper;
    this.plannerConfig = Preconditions.checkNotNull(plannerConfig, "plannerConfig");
    this.rootSchema = rootSchema;
    this.joinableFactoryWrapper = joinableFactoryWrapper;
    this.catalog = catalog;
    this.druidSchemaName = druidSchemaName;
    this.calciteRuleManager = calciteRuleManager;
    this.authorizerMapper = authorizerMapper;
    this.authConfig = authConfig;
  }

  public DruidOperatorTable operatorTable()
  {
    return operatorTable;
  }

  public ExprMacroTable exprMacroTable()
  {
    return macroTable;
  }

  public ObjectMapper jsonMapper()
  {
    return jsonMapper;
  }

  public DruidSchemaCatalog rootSchema()
  {
    return rootSchema;
  }

  public JoinableFactoryWrapper joinableFactoryWrapper()
  {
    return joinableFactoryWrapper;
  }

  public CatalogResolver catalogResolver()
  {
    return catalog;
  }

  public String druidSchemaName()
  {
    return druidSchemaName;
  }

  public CalciteRulesManager calciteRuleManager()
  {
    return calciteRuleManager;
  }

  public PlannerConfig plannerConfig()
  {
    return plannerConfig;
  }

  public AuthConfig getAuthConfig()
  {
    return authConfig;
  }
}

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
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalogProvider;
import org.apache.druid.sql.hook.DruidHookDispatcher;

public class PlannerToolbox
{
  protected final DruidOperatorTable operatorTable;
  protected final ExprMacroTable macroTable;
  protected final JoinableFactoryWrapper joinableFactoryWrapper;
  protected final ObjectMapper jsonMapper;
  protected final PlannerConfig plannerConfig;
  protected final DruidSchemaCatalogProvider rootSchemaProvider;
  protected final CatalogResolver catalog;
  protected final CatalogTableWriter catalogTableWriter;
  protected final String druidSchemaName;
  protected final CalciteRulesManager calciteRuleManager;
  protected final AuthorizerMapper authorizerMapper;
  protected final AuthConfig authConfig;
  protected final PolicyEnforcer policyEnforcer;
  protected final DruidHookDispatcher hookDispatcher;

  /**
   * Convenience for callers that never execute catalog DDL, such as tests and benchmarks.
   */
  public PlannerToolbox(
      final DruidOperatorTable operatorTable,
      final ExprMacroTable macroTable,
      final ObjectMapper jsonMapper,
      final PlannerConfig plannerConfig,
      final DruidSchemaCatalogProvider rootSchemaProvider,
      final JoinableFactoryWrapper joinableFactoryWrapper,
      final CatalogResolver catalog,
      final String druidSchemaName,
      final CalciteRulesManager calciteRuleManager,
      final AuthorizerMapper authorizerMapper,
      final AuthConfig authConfig,
      final PolicyEnforcer policyEnforcer,
      final DruidHookDispatcher hookDispatcher
  )
  {
    this(
        operatorTable,
        macroTable,
        jsonMapper,
        plannerConfig,
        rootSchemaProvider,
        joinableFactoryWrapper,
        catalog,
        CatalogTableWriter.NOT_AVAILABLE,
        druidSchemaName,
        calciteRuleManager,
        authorizerMapper,
        authConfig,
        policyEnforcer,
        hookDispatcher
    );
  }

  public PlannerToolbox(
      final DruidOperatorTable operatorTable,
      final ExprMacroTable macroTable,
      final ObjectMapper jsonMapper,
      final PlannerConfig plannerConfig,
      final DruidSchemaCatalogProvider rootSchemaProvider,
      final JoinableFactoryWrapper joinableFactoryWrapper,
      final CatalogResolver catalog,
      final CatalogTableWriter catalogTableWriter,
      final String druidSchemaName,
      final CalciteRulesManager calciteRuleManager,
      final AuthorizerMapper authorizerMapper,
      final AuthConfig authConfig,
      final PolicyEnforcer policyEnforcer,
      final DruidHookDispatcher hookDispatcher
  )
  {
    this.operatorTable = operatorTable;
    this.macroTable = macroTable;
    this.jsonMapper = jsonMapper;
    this.plannerConfig = Preconditions.checkNotNull(plannerConfig, "plannerConfig");
    this.rootSchemaProvider = rootSchemaProvider;
    this.joinableFactoryWrapper = joinableFactoryWrapper;
    this.catalog = catalog;
    this.catalogTableWriter = catalogTableWriter;
    this.druidSchemaName = druidSchemaName;
    this.calciteRuleManager = calciteRuleManager;
    this.authorizerMapper = authorizerMapper;
    this.authConfig = authConfig;
    this.policyEnforcer = policyEnforcer;
    this.hookDispatcher = hookDispatcher;
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

  public JoinableFactoryWrapper joinableFactoryWrapper()
  {
    return joinableFactoryWrapper;
  }

  public CatalogResolver catalogResolver()
  {
    return catalog;
  }

  public CatalogTableWriter catalogTableWriter()
  {
    return catalogTableWriter;
  }

  public String druidSchemaName()
  {
    return druidSchemaName;
  }

  public PlannerConfig plannerConfig()
  {
    return plannerConfig;
  }

  public AuthConfig getAuthConfig()
  {
    return authConfig;
  }

  public PolicyEnforcer getPolicyEnforcer()
  {
    return policyEnforcer;
  }

  public DruidHookDispatcher getHookDispatcher()
  {
    return hookDispatcher;
  }
}

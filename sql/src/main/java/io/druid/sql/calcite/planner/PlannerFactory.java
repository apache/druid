/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.planner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.druid.guice.annotations.Json;
import io.druid.math.expr.ExprMacroTable;
import io.druid.server.QueryLifecycleFactory;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthenticatorMapper;
import io.druid.server.security.AuthorizerMapper;
import io.druid.sql.calcite.rel.QueryMaker;
import io.druid.sql.calcite.schema.DruidSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.util.Map;

public class PlannerFactory
{
  private static final SqlParser.Config PARSER_CONFIG = SqlParser
      .configBuilder()
      .setCaseSensitive(true)
      .setUnquotedCasing(Casing.UNCHANGED)
      .setQuotedCasing(Casing.UNCHANGED)
      .setQuoting(Quoting.DOUBLE_QUOTE)
      .setConformance(DruidConformance.instance())
      .build();

  private final DruidSchema druidSchema;
  private final QueryLifecycleFactory queryLifecycleFactory;
  private final DruidOperatorTable operatorTable;
  private final ExprMacroTable macroTable;
  private final PlannerConfig plannerConfig;
  private final ObjectMapper jsonMapper;

  private final AuthConfig authConfig;
  private final AuthorizerMapper authorizerMapper;
  private final AuthenticatorMapper authenticatorMapper;

  @Inject
  public PlannerFactory(
      final DruidSchema druidSchema,
      final QueryLifecycleFactory queryLifecycleFactory,
      final DruidOperatorTable operatorTable,
      final ExprMacroTable macroTable,
      final PlannerConfig plannerConfig,
      final AuthConfig authConfig,
      final AuthenticatorMapper authenticatorMapper,
      final AuthorizerMapper authorizerMapper,
      final @Json ObjectMapper jsonMapper
  )
  {
    this.druidSchema = druidSchema;
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.operatorTable = operatorTable;
    this.macroTable = macroTable;
    this.plannerConfig = plannerConfig;
    this.authConfig = authConfig;
    this.authorizerMapper = authorizerMapper;
    this.authenticatorMapper = authenticatorMapper;
    this.jsonMapper = jsonMapper;
  }

  public DruidPlanner createPlanner(final Map<String, Object> queryContext)
  {
    final SchemaPlus rootSchema = Calcites.createRootSchema(druidSchema, authorizerMapper);
    final PlannerContext plannerContext = PlannerContext.create(
        operatorTable,
        macroTable,
        plannerConfig,
        authorizerMapper,
        queryContext
    );
    final QueryMaker queryMaker = new QueryMaker(queryLifecycleFactory, plannerContext, jsonMapper);

    final FrameworkConfig frameworkConfig = Frameworks
        .newConfigBuilder()
        .parserConfig(PARSER_CONFIG)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .convertletTable(new DruidConvertletTable(plannerContext))
        .operatorTable(operatorTable)
        .programs(Rules.programs(plannerContext, queryMaker))
        .executor(new DruidRexExecutor(plannerContext))
        .context(Contexts.EMPTY_CONTEXT)
        .typeSystem(RelDataTypeSystem.DEFAULT)
        .defaultSchema(rootSchema.getSubSchema(DruidSchema.NAME))
        .typeSystem(DruidTypeSystem.INSTANCE)
        .build();

    return new DruidPlanner(
        Frameworks.getPlanner(frameworkConfig),
        plannerContext,
        authConfig,
        authorizerMapper,
        authenticatorMapper
    );
  }
}

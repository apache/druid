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
import com.google.inject.Inject;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.rel.QueryMaker;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.SystemSchema;

import java.util.Map;
import java.util.Properties;

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
  private final SystemSchema systemSchema;
  private final QueryLifecycleFactory queryLifecycleFactory;
  private final DruidOperatorTable operatorTable;
  private final ExprMacroTable macroTable;
  private final PlannerConfig plannerConfig;
  private final ObjectMapper jsonMapper;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public PlannerFactory(
      final DruidSchema druidSchema,
      final SystemSchema systemSchema,
      final QueryLifecycleFactory queryLifecycleFactory,
      final DruidOperatorTable operatorTable,
      final ExprMacroTable macroTable,
      final PlannerConfig plannerConfig,
      final AuthorizerMapper authorizerMapper,
      final @Json ObjectMapper jsonMapper
  )
  {
    this.druidSchema = druidSchema;
    this.systemSchema = systemSchema;
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.operatorTable = operatorTable;
    this.macroTable = macroTable;
    this.plannerConfig = plannerConfig;
    this.authorizerMapper = authorizerMapper;
    this.jsonMapper = jsonMapper;
  }

  public DruidPlanner createPlanner(
      final Map<String, Object> queryContext,
      final AuthenticationResult authenticationResult
  )
  {
    final SchemaPlus rootSchema = Calcites.createRootSchema(
        druidSchema,
        systemSchema,
        authorizerMapper
    );
    final PlannerContext plannerContext = PlannerContext.create(
        operatorTable,
        macroTable,
        plannerConfig,
        queryContext,
        authenticationResult
    );
    final QueryMaker queryMaker = new QueryMaker(queryLifecycleFactory, plannerContext, jsonMapper);
    final SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter
        .configBuilder()
        .withExpand(false)
        .withDecorrelationEnabled(false)
        .withTrimUnusedFields(false)
        .withInSubQueryThreshold(Integer.MAX_VALUE)
        .build();
    final FrameworkConfig frameworkConfig = Frameworks
        .newConfigBuilder()
        .parserConfig(PARSER_CONFIG)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .convertletTable(new DruidConvertletTable(plannerContext))
        .operatorTable(operatorTable)
        .programs(Rules.programs(plannerContext, queryMaker))
        .executor(new DruidRexExecutor(plannerContext))
        .context(Contexts.EMPTY_CONTEXT)
        .typeSystem(DruidTypeSystem.INSTANCE)
        .defaultSchema(rootSchema.getSubSchema(DruidSchema.NAME))
        .sqlToRelConverterConfig(sqlToRelConverterConfig)
        .context(new Context()
        {
          @Override
          @SuppressWarnings("unchecked")
          public <C> C unwrap(final Class<C> aClass)
          {
            if (aClass.equals(CalciteConnectionConfig.class)) {
              // This seems to be the best way to provide our own SqlConformance instance. Otherwise, Calcite's
              // validator will not respect it.
              final Properties props = new Properties();
              return (C) new CalciteConnectionConfigImpl(props)
              {
                @Override
                public SqlConformance conformance()
                {
                  return DruidConformance.instance();
                }
              };
            } else {
              return null;
            }
          }
        })
        .build();

    return new DruidPlanner(
        Frameworks.getPlanner(frameworkConfig),
        plannerContext
    );
  }

  public AuthorizerMapper getAuthorizerMapper()
  {
    return authorizerMapper;
  }
}

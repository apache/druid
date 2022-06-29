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
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.sql.calcite.parser.DruidSqlParserImplFactory;
import org.apache.druid.sql.calcite.planner.convertlet.DruidConvertletTable;
import org.apache.druid.sql.calcite.run.QueryMakerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.DruidSchemaName;

import java.util.Map;
import java.util.Properties;

public class PlannerFactory
{
  static final SqlParser.Config PARSER_CONFIG = SqlParser
      .configBuilder()
      .setCaseSensitive(true)
      .setUnquotedCasing(Casing.UNCHANGED)
      .setQuotedCasing(Casing.UNCHANGED)
      .setQuoting(Quoting.DOUBLE_QUOTE)
      .setConformance(DruidConformance.instance())
      .setParserFactory(new DruidSqlParserImplFactory()) // Custom sql parser factory
      .build();

  private final DruidSchemaCatalog rootSchema;
  private final QueryMakerFactory queryMakerFactory;
  private final DruidOperatorTable operatorTable;
  private final ExprMacroTable macroTable;
  private final PlannerConfig plannerConfig;
  private final ObjectMapper jsonMapper;
  private final AuthorizerMapper authorizerMapper;
  private final String druidSchemaName;

  @Inject
  public PlannerFactory(
      final DruidSchemaCatalog rootSchema,
      final QueryMakerFactory queryMakerFactory,
      final DruidOperatorTable operatorTable,
      final ExprMacroTable macroTable,
      final PlannerConfig plannerConfig,
      final AuthorizerMapper authorizerMapper,
      final @Json ObjectMapper jsonMapper,
      final @DruidSchemaName String druidSchemaName
  )
  {
    this.rootSchema = rootSchema;
    this.queryMakerFactory = queryMakerFactory;
    this.operatorTable = operatorTable;
    this.macroTable = macroTable;
    this.plannerConfig = plannerConfig;
    this.authorizerMapper = authorizerMapper;
    this.jsonMapper = jsonMapper;
    this.druidSchemaName = druidSchemaName;
  }

  /**
   * Create a Druid query planner from an initial query context
   */
  public DruidPlanner createPlanner(final String sql, final QueryContext queryContext)
  {
    final PlannerContext context = PlannerContext.create(
        sql,
        operatorTable,
        macroTable,
        jsonMapper,
        plannerConfig,
        rootSchema,
        queryContext
    );

    return createPlannerWithContext(context);
  }

  /**
   * Create a new Druid query planner, re-using a previous {@link PlannerContext}
   */
  public DruidPlanner createPlannerWithContext(final PlannerContext plannerContext)
  {
    return new DruidPlanner(buildFrameworkConfig(plannerContext), plannerContext, queryMakerFactory);
  }

  /**
   * Not just visible for, but only for testing. Create a planner pre-loaded with an escalated authentication result
   * and ready to go authorization result.
   */
  @VisibleForTesting
  public DruidPlanner createPlannerForTesting(final Map<String, Object> queryContext, String query)
  {
    final DruidPlanner thePlanner = createPlanner(query, new QueryContext(queryContext));
    thePlanner.getPlannerContext()
              .setAuthenticationResult(NoopEscalator.getInstance().createEscalatedAuthenticationResult());
    try {
      thePlanner.validate(false);
    }
    catch (SqlParseException | ValidationException e) {
      throw new RuntimeException(e);
    }
    thePlanner.getPlannerContext().setAuthorizationResult(Access.OK);
    return thePlanner;
  }

  public AuthorizerMapper getAuthorizerMapper()
  {
    return authorizerMapper;
  }

  private FrameworkConfig buildFrameworkConfig(PlannerContext plannerContext)
  {
    final SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter
        .configBuilder()
        .withExpand(false)
        .withDecorrelationEnabled(false)
        .withTrimUnusedFields(false)
        .withInSubQueryThreshold(
            QueryContexts.getInSubQueryThreshold(plannerContext.getQueryContext().getMergedParams())
        )
        .build();
    return Frameworks
        .newConfigBuilder()
        .parserConfig(PARSER_CONFIG)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .convertletTable(new DruidConvertletTable(plannerContext))
        .operatorTable(operatorTable)
        .programs(Rules.programs(plannerContext))
        .executor(new DruidRexExecutor(plannerContext))
        .typeSystem(DruidTypeSystem.INSTANCE)
        .defaultSchema(rootSchema.getSubSchema(druidSchemaName))
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
                public <T> T typeSystem(Class<T> typeSystemClass, T defaultTypeSystem)
                {
                  return (T) DruidTypeSystem.INSTANCE;
                }

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
  }
}

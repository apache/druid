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

import com.google.inject.Inject;
import io.druid.query.QuerySegmentWalker;
import io.druid.sql.calcite.rel.QueryMaker;
import io.druid.sql.calcite.schema.DruidSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.util.Map;

public class PlannerFactory
{
  private final SchemaPlus rootSchema;
  private final QuerySegmentWalker walker;
  private final DruidOperatorTable operatorTable;
  private final PlannerConfig plannerConfig;

  @Inject
  public PlannerFactory(
      final SchemaPlus rootSchema,
      final QuerySegmentWalker walker,
      final DruidOperatorTable operatorTable,
      final PlannerConfig plannerConfig
  )
  {
    this.rootSchema = rootSchema;
    this.walker = walker;
    this.operatorTable = operatorTable;
    this.plannerConfig = plannerConfig;
  }

  public DruidPlanner createPlanner(final Map<String, Object> queryContext)
  {
    final PlannerContext plannerContext = PlannerContext.create(plannerConfig, queryContext);
    final QueryMaker queryMaker = new QueryMaker(walker, plannerContext);
    final FrameworkConfig frameworkConfig = Frameworks
        .newConfigBuilder()
        .parserConfig(
            SqlParser.configBuilder()
                     .setCaseSensitive(true)
                     .setUnquotedCasing(Casing.UNCHANGED)
                     .setQuotedCasing(Casing.UNCHANGED)
                     .setQuoting(Quoting.DOUBLE_QUOTE)
                     .build()
        )
        .defaultSchema(rootSchema)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .convertletTable(new DruidConvertletTable(plannerContext))
        .operatorTable(operatorTable)
        .programs(Rules.programs(queryMaker, operatorTable))
        .executor(new RexExecutorImpl(Schemas.createDataContext(null)))
        .context(Contexts.EMPTY_CONTEXT)
        .typeSystem(RelDataTypeSystem.DEFAULT)
        .defaultSchema(rootSchema.getSubSchema(DruidSchema.NAME))
        .build();

    return new DruidPlanner(Frameworks.getPlanner(frameworkConfig), plannerContext);
  }
}

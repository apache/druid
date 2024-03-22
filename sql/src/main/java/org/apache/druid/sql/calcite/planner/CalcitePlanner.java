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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;

import javax.annotation.Nullable;
import java.io.Reader;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * Calcite planner. Clone of Calcite's
 * {@link  org.apache.calcite.prepare.PlannerImpl}, as of version 1.35,
 * but with the validator made accessible, and with the minimum of formatting
 * changes needed to pass Druid's static checks. Note that the resulting code
 * is more Calcite-like than Druid-like. There seemed no value in restructuring
 * the code just to be more Druid-like.
 *
 * Changes in 1.35:
 *
 * Allowing user-defined config and appending default values to the config
 * frameworkConfig is now replaced by costFactory
 */
public class CalcitePlanner implements Planner, ViewExpander
{
  private final SqlOperatorTable operatorTable;
  private final ImmutableList<Program> programs;
  private final @Nullable RelOptCostFactory costFactory;
  private final Context context;
  private final CalciteConnectionConfig connectionConfig;
  private final RelDataTypeSystem typeSystem;

  /**
   * Holds the trait definitions to be registered with planner. May be null.
   */
  private final @Nullable ImmutableList<RelTraitDef> traitDefs;

  private final SqlParser.Config parserConfig;
  private final SqlToRelConverter.Config sqlToRelConverterConfig;
  private final SqlRexConvertletTable convertletTable;

  private CalcitePlanner.State state;

  // set in STATE_1_RESET
  @SuppressWarnings("unused")
  private boolean open;

  // set in STATE_2_READY
  private @Nullable SchemaPlus defaultSchema;
  private @Nullable JavaTypeFactory typeFactory;
  private @Nullable RelOptPlanner planner;
  private @Nullable RexExecutor executor;

  // set in STATE_4_VALIDATE
  private @Nullable SqlValidator validator;
  private @Nullable SqlNode validatedSqlNode;

  /**
   * Creates a planner. Not a public API; call
   * {@link org.apache.calcite.tools.Frameworks#getPlanner} instead.
   */
  @SuppressWarnings("method.invocation.invalid")
  public CalcitePlanner(FrameworkConfig config)
  {
    this.costFactory = config.getCostFactory();
    this.defaultSchema = config.getDefaultSchema();
    this.operatorTable = config.getOperatorTable();
    this.programs = config.getPrograms();
    this.parserConfig = config.getParserConfig();
    this.sqlToRelConverterConfig = config.getSqlToRelConverterConfig();
    this.state = CalcitePlanner.State.STATE_0_CLOSED;
    this.traitDefs = config.getTraitDefs();
    this.convertletTable = config.getConvertletTable();
    this.executor = config.getExecutor();
    this.context = config.getContext();
    this.connectionConfig = connConfig(context);
    this.typeSystem = config.getTypeSystem();
    reset();
  }

  /**
   * Gets a user-defined config and appends default connection values.
   */
  private static CalciteConnectionConfig connConfig(Context context)
  {
    return context.maybeUnwrap(CalciteConnectionConfig.class)
                  .orElse(new PlannerFactory.DruidCalciteConnectionConfigImpl(new Properties()));
  }

  /**
   * Makes sure that the state is at least the given state.
   */
  private void ensure(CalcitePlanner.State state)
  {
    if (state == this.state) {
      return;
    }
    if (state.ordinal() < this.state.ordinal()) {
      throw new IllegalArgumentException("cannot move to " + state + " from "
                                         + this.state);
    }
    state.from(this);
  }

  @Override
  public RelTraitSet getEmptyTraitSet()
  {
    return Objects.requireNonNull(planner, "planner").emptyTraitSet();
  }

  @Override
  public void close()
  {
    open = false;
    typeFactory = null;
    state = CalcitePlanner.State.STATE_0_CLOSED;
  }

  @Override
  public void reset()
  {
    ensure(CalcitePlanner.State.STATE_0_CLOSED);
    open = true;
    state = CalcitePlanner.State.STATE_1_RESET;
  }

  private void ready()
  {
    switch (state) {
      case STATE_0_CLOSED:
        reset();
        break;
      default:
        break;
    }
    ensure(CalcitePlanner.State.STATE_1_RESET);

    typeFactory = new JavaTypeFactoryImpl(typeSystem);
    RelOptPlanner planner = this.planner = new VolcanoPlanner(costFactory, context);
    planner.setExecutor(executor);

    state = CalcitePlanner.State.STATE_2_READY;

    // If user specify own traitDef, instead of default default trait,
    // register the trait def specified in traitDefs.
    if (this.traitDefs == null) {
      planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
      if (CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()) {
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
      }
    } else {
      for (RelTraitDef def : this.traitDefs) {
        planner.addRelTraitDef(def);
      }
    }
  }

  @Override
  public SqlNode parse(final Reader reader) throws SqlParseException
  {
    switch (state) {
      case STATE_0_CLOSED:
      case STATE_1_RESET:
        ready();
        break;
      default:
        break;
    }
    ensure(CalcitePlanner.State.STATE_2_READY);
    SqlParser parser = SqlParser.create(reader, parserConfig);
    SqlNode sqlNode = parser.parseStmt();
    state = CalcitePlanner.State.STATE_3_PARSED;
    return sqlNode;
  }

  @Override
  public SqlNode validate(SqlNode sqlNode) throws ValidationException
  {
    ensure(CalcitePlanner.State.STATE_3_PARSED);
    this.validator = createSqlValidator(createCatalogReader());
    try {
      validatedSqlNode = validator.validate(sqlNode);
    }
    catch (RuntimeException e) {
      throw new ValidationException(e);
    }
    state = CalcitePlanner.State.STATE_4_VALIDATED;
    return validatedSqlNode;
  }

  public SqlValidator getValidator()
  {
    return validator;
  }

  @Override
  public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode sqlNode)
      throws ValidationException
  {
    final SqlNode validatedNode = this.validate(sqlNode);
    final RelDataType type =
        this.validator.getValidatedNodeType(validatedNode);
    return Pair.of(validatedNode, type);
  }

  @Override
  public RelDataType getParameterRowType()
  {
    if (state.ordinal() < CalcitePlanner.State.STATE_4_VALIDATED.ordinal()) {
      throw new RuntimeException("Need to call #validate() first");
    }

    return Objects.requireNonNull(validator, "validator")
                  .getParameterRowType(Objects.requireNonNull(validatedSqlNode, "validatedSqlNode"));
  }

  @SuppressWarnings("deprecation")
  @Override
  public final RelNode convert(SqlNode sql)
  {
    return rel(sql).rel;
  }

  @Override
  public RelRoot rel(SqlNode sql)
  {
    ensure(CalcitePlanner.State.STATE_4_VALIDATED);
    Objects.requireNonNull(
        this.validatedSqlNode,
        "validatedSqlNode is null. Need to call #validate() first"
    );
    final RexBuilder rexBuilder = createRexBuilder();
    final RelOptCluster cluster = RelOptCluster.create(
        Objects.requireNonNull(planner, "planner"),
        rexBuilder
    );
    final SqlToRelConverter.Config config =
        sqlToRelConverterConfig.withTrimUnusedFields(false);
    final SqlToRelConverter sqlToRelConverter =
        new DruidSqlToRelConverter(this, validator,
                              createCatalogReader(), cluster, convertletTable, config
        );
    RelRoot root =
        sqlToRelConverter.convertQuery(sql, false, true);
    root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
    final RelBuilder relBuilder =
        config.getRelBuilderFactory().create(cluster, null);
    root = root.withRel(
        RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
    state = CalcitePlanner.State.STATE_5_CONVERTED;
    return root;
  }

  // CHECKSTYLE: IGNORE 2

  /**
   * @deprecated Now {@link CalcitePlanner} implements {@link ViewExpander}
   * directly.
   */
  @Deprecated // to be removed before 2.0
  public class ViewExpanderImpl implements ViewExpander
  {
    ViewExpanderImpl()
    {
    }

    @Override
    public RelRoot expandView(
        RelDataType rowType,
        String queryString,
        List<String> schemaPath,
        @Nullable List<String> viewPath
    )
    {
      return CalcitePlanner.this.expandView(rowType, queryString, schemaPath,
                                            viewPath
      );
    }
  }

  @Override
  public RelRoot expandView(
      RelDataType rowType,
      String queryString,
      List<String> schemaPath,
      @Nullable List<String> viewPath
  )
  {
    RelOptPlanner planner = this.planner;
    if (planner == null) {
      ready();
      planner = Objects.requireNonNull(this.planner, "planner");
    }
    SqlParser parser = SqlParser.create(queryString, parserConfig);
    SqlNode sqlNode;
    try {
      sqlNode = parser.parseQuery();
    }
    catch (SqlParseException e) {
      throw new RuntimeException("parse failed", e);
    }

    final CalciteCatalogReader catalogReader =
        createCatalogReader().withSchemaPath(schemaPath);
    final SqlValidator validator = createSqlValidator(catalogReader);

    final RexBuilder rexBuilder = createRexBuilder();
    final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    final SqlToRelConverter.Config config =
        sqlToRelConverterConfig.withTrimUnusedFields(false);
    final SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter(this, validator,
                              catalogReader, cluster, convertletTable, config
        );

    final RelRoot root =
        sqlToRelConverter.convertQuery(sqlNode, true, false);
    final RelRoot root2 =
        root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
    final RelBuilder relBuilder =
        config.getRelBuilderFactory().create(cluster, null);
    return root2.withRel(
        RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
  }

  // CalciteCatalogReader is stateless; no need to store one
  private CalciteCatalogReader createCatalogReader()
  {
    SchemaPlus defaultSchema = Objects.requireNonNull(this.defaultSchema, "defaultSchema");
    final SchemaPlus rootSchema = rootSchema(defaultSchema);

    return new CalciteCatalogReader(
        CalciteSchema.from(rootSchema),
        CalciteSchema.from(defaultSchema).path(null),
        getTypeFactory(),
        connectionConfig
    );
  }

  private SqlValidator createSqlValidator(CalciteCatalogReader catalogReader)
  {
    final SqlOperatorTable opTab =
        SqlOperatorTables.chain(operatorTable, catalogReader);
    final SqlValidator.Config validatorConfig =
        SqlValidator.Config.DEFAULT.withConformance(connectionConfig.conformance())
                                   .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
                                   .withIdentifierExpansion(true);
    return new DruidSqlValidator(
        opTab,
        catalogReader,
        getTypeFactory(),
        validatorConfig,
        context.unwrapOrThrow(PlannerContext.class)
    );
  }

  private static SchemaPlus rootSchema(SchemaPlus schema)
  {
    for (; ; ) {
      SchemaPlus parentSchema = schema.getParentSchema();
      if (parentSchema == null) {
        return schema;
      }
      schema = parentSchema;
    }
  }

  // RexBuilder is stateless; no need to store one
  private RexBuilder createRexBuilder()
  {
    return new RexBuilder(getTypeFactory());
  }

  @Override
  public JavaTypeFactory getTypeFactory()
  {
    return Objects.requireNonNull(typeFactory, "typeFactory");
  }

  @SuppressWarnings("deprecation")
  @Override
  public RelNode transform(
      int ruleSetIndex,
      RelTraitSet requiredOutputTraits,
      RelNode rel
  )
  {
    ensure(CalcitePlanner.State.STATE_5_CONVERTED);
    rel.getCluster().setMetadataProvider(
        new CachingRelMetadataProvider(
            Objects.requireNonNull(rel.getCluster().getMetadataProvider(), "metadataProvider"),
            rel.getCluster().getPlanner()
        ));
    Program program = programs.get(ruleSetIndex);
    return program.run(
        Objects.requireNonNull(planner, "planner"),
        rel,
        requiredOutputTraits,
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  /**
   * Stage of a statement in the query-preparation lifecycle.
   */
  private enum State
  {
    STATE_0_CLOSED {
      @Override
      void from(CalcitePlanner planner)
      {
        planner.close();
      }
    },
    STATE_1_RESET {
      @Override
      void from(CalcitePlanner planner)
      {
        planner.ensure(STATE_0_CLOSED);
        planner.reset();
      }
    },
    STATE_2_READY {
      @Override
      void from(CalcitePlanner planner)
      {
        STATE_1_RESET.from(planner);
        planner.ready();
      }
    },
    STATE_3_PARSED,
    STATE_4_VALIDATED,
    STATE_5_CONVERTED;

    /**
     * Moves planner's state to this state. This must be a higher state.
     */
    void from(CalcitePlanner planner)
    {
      throw new IllegalArgumentException("cannot move from " + planner.state
                                         + " to " + this);
    }
  }
}

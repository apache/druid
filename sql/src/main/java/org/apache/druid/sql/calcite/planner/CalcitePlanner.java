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
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.BaseDruidSqlValidator;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
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
import java.util.Properties;

/**
 * Calcite planner. Clone of Calcite's
 * {@link  org.apache.calcite.prepare.PlannerImpl}, as of version 1.21,
 * but with the validator made accessible, and with the minimum of formatting
 * changes needed to pass Druid's static checks. Note that the resulting code
 * is more Calcite-like than Druid-like. There seemed no value in restructuring
 * the code just to be more Druid-like.
 */
public class CalcitePlanner implements Planner, ViewExpander
{
  private final SqlOperatorTable operatorTable;
  private final ImmutableList<Program> programs;
  private final FrameworkConfig frameworkConfig;
  private final Context context;
  private final CalciteConnectionConfig connectionConfig;

  /** Holds the trait definitions to be registered with planner. May be null. */
  private @Nullable final List<RelTraitDef> traitDefs;

  private final SqlParser.Config parserConfig;
  private final SqlToRelConverter.Config sqlToRelConverterConfig;
  private final SqlRexConvertletTable convertletTable;

  private State state;

  // set in STATE_2_READY
  private SchemaPlus defaultSchema;
  private JavaTypeFactory typeFactory;
  private RelOptPlanner planner;
  private RexExecutor executor;

  // set in STATE_4_VALIDATE
  private SqlValidator validator;
  private SqlNode validatedSqlNode;

  // set in STATE_5_CONVERT
  private RelRoot root;

  public CalcitePlanner(FrameworkConfig config)
  {
    this.frameworkConfig = config;
    this.defaultSchema = config.getDefaultSchema();
    this.operatorTable = config.getOperatorTable();
    this.programs = config.getPrograms();
    this.parserConfig = config.getParserConfig();
    this.sqlToRelConverterConfig = config.getSqlToRelConverterConfig();
    this.state = State.STATE_0_CLOSED;
    this.traitDefs = config.getTraitDefs();
    this.convertletTable = config.getConvertletTable();
    this.executor = config.getExecutor();
    this.context = config.getContext();
    this.connectionConfig = connConfig();
    reset();
  }

  private CalciteConnectionConfig connConfig()
  {
    CalciteConnectionConfig unwrapped = context.unwrap(CalciteConnectionConfig.class);
    if (unwrapped != null) {
      return unwrapped;
    }
    Properties properties = new Properties();
    properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
        String.valueOf(parserConfig.caseSensitive()));
    properties.setProperty(CalciteConnectionProperty.CONFORMANCE.camelName(),
        String.valueOf(frameworkConfig.getParserConfig().conformance()));
    return new CalciteConnectionConfigImpl(properties);
  }

  /** Makes sure that the state is at least the given state. */
  private void ensure(State state)
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
    return planner.emptyTraitSet();
  }

  public FrameworkConfig frameworkConfig()
  {
    return frameworkConfig;
  }

  @Override
  public void close()
  {
    typeFactory = null;
    state = State.STATE_0_CLOSED;
  }

  @Override
  public void reset()
  {
    ensure(State.STATE_0_CLOSED);
    state = State.STATE_1_RESET;
  }

  private void ready()
  {
    switch (state) {
      case STATE_0_CLOSED:
        reset();
        break;
      default:
    }
    ensure(State.STATE_1_RESET);

    RelDataTypeSystem typeSystem =
        connectionConfig.typeSystem(RelDataTypeSystem.class,
            RelDataTypeSystem.DEFAULT);
    typeFactory = new JavaTypeFactoryImpl(typeSystem);
    planner = new VolcanoPlanner(frameworkConfig.getCostFactory(), context);
    RelOptUtil.registerDefaultRules(planner,
        connectionConfig.materializationsEnabled(),
        Hook.ENABLE_BINDABLE.get(false));
    planner.setExecutor(executor);

    state = State.STATE_2_READY;

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
    }
    ensure(State.STATE_2_READY);
    SqlParser parser = SqlParser.create(reader, parserConfig);
    SqlNode sqlNode = parser.parseStmt();
    state = State.STATE_3_PARSED;
    return sqlNode;
  }

  @Override
  public SqlNode validate(SqlNode sqlNode) throws ValidationException
  {
    ensure(State.STATE_3_PARSED);
    final SqlConformance conformance = conformance();
    final CalciteCatalogReader catalogReader = createCatalogReader();
    this.validator =
        new BaseDruidSqlValidator(operatorTable, catalogReader, typeFactory,
            conformance);
    this.validator.setIdentifierExpansion(true);
    try {
      validatedSqlNode = validator.validate(sqlNode);
    }
    catch (RuntimeException e) {
      throw new ValidationException(e);
    }
    state = State.STATE_4_VALIDATED;
    return validatedSqlNode;
  }

  public SqlValidator getValidator()
  {
    return validator;
  }

  private SqlConformance conformance()
  {
    return connectionConfig.conformance();
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
  public final RelNode convert(SqlNode sql)
  {
    return rel(sql).rel;
  }

  @Override
  public RelRoot rel(SqlNode sql)
  {
    ensure(State.STATE_4_VALIDATED);
    assert validatedSqlNode != null;
    final RexBuilder rexBuilder = createRexBuilder();
    final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
        .withConfig(sqlToRelConverterConfig)
        .withTrimUnusedFields(false)
        .withConvertTableAccess(false)
        .build();
    final SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter(this, validator,
            createCatalogReader(), cluster, convertletTable, config);
    root =
        sqlToRelConverter.convertQuery(validatedSqlNode, false, true);
    root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
    final RelBuilder relBuilder =
        config.getRelBuilderFactory().create(cluster, null);
    root = root.withRel(
        RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
    state = State.STATE_5_CONVERTED;
    return root;
  }

  @Override public RelRoot expandView(
      RelDataType rowType,
      String queryString,
      List<String> schemaPath,
      List<String> viewPath)
  {
    if (planner == null) {
      ready();
    }
    SqlParser parser = SqlParser.create(queryString, parserConfig);
    SqlNode sqlNode;
    try {
      sqlNode = parser.parseQuery();
    }
    catch (SqlParseException e) {
      throw new RuntimeException("parse failed", e);
    }

    final SqlConformance conformance = conformance();
    final CalciteCatalogReader catalogReader =
        createCatalogReader().withSchemaPath(schemaPath);
    final SqlValidator validator =
        new BaseDruidSqlValidator(operatorTable, catalogReader, typeFactory,
            conformance);
    validator.setIdentifierExpansion(true);

    final RexBuilder rexBuilder = createRexBuilder();
    final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    final SqlToRelConverter.Config config = SqlToRelConverter
        .configBuilder()
        .withConfig(sqlToRelConverterConfig)
        .withTrimUnusedFields(false)
        .withConvertTableAccess(false)
        .build();
    final SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter(this, validator,
            catalogReader, cluster, convertletTable, config);

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
    return new CalciteCatalogReader(
        CalciteSchema.from(rootSchema(defaultSchema)),
        CalciteSchema.from(defaultSchema).path(null),
        typeFactory,
        connectionConfig
    );
  }

  private static SchemaPlus rootSchema(SchemaPlus schema)
  {
    for (;;) {
      if (schema.getParentSchema() == null) {
        return schema;
      }
      schema = schema.getParentSchema();
    }
  }

  // RexBuilder is stateless; no need to store one
  private RexBuilder createRexBuilder()
  {
    return new RexBuilder(typeFactory);
  }

  @Override
  public JavaTypeFactory getTypeFactory()
  {
    return typeFactory;
  }

  @Override
  public RelNode transform(
      int ruleSetIndex,
      RelTraitSet requiredOutputTraits,
      RelNode rel
  )
  {
    ensure(State.STATE_5_CONVERTED);
    rel.getCluster().setMetadataProvider(
        new CachingRelMetadataProvider(
            rel.getCluster().getMetadataProvider(),
            rel.getCluster().getPlanner()));
    Program program = programs.get(ruleSetIndex);
    return program.run(planner, rel, requiredOutputTraits, ImmutableList.of(),
        ImmutableList.of());
  }

  /** Stage of a statement in the query-preparation lifecycle. */
  private enum State
  {
    STATE_0_CLOSED {
      @Override void from(CalcitePlanner planner)
      {
        planner.close();
      }
    },
    STATE_1_RESET {
      @Override void from(CalcitePlanner planner)
      {
        planner.ensure(STATE_0_CLOSED);
        planner.reset();
      }
    },
    STATE_2_READY {
      @Override void from(CalcitePlanner planner)
      {
        STATE_1_RESET.from(planner);
        planner.ready();
      }
    },
    STATE_3_PARSED,
    STATE_4_VALIDATED,
    STATE_5_CONVERTED;

    /** Moves planner's state to this state. This must be a higher state. */
    void from(CalcitePlanner planner)
    {
      throw new IllegalArgumentException("cannot move from " + planner.state
          + " to " + this);
    }
  }
}

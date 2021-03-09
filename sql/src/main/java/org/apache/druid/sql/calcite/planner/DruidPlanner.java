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

import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.com.google.common.base.Supplier;
import org.apache.druid.com.google.common.base.Suppliers;
import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.sql.calcite.rel.DruidConvention;
import org.apache.druid.sql.calcite.rel.DruidRel;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class DruidPlanner implements Closeable
{
  private final FrameworkConfig frameworkConfig;
  private final Planner planner;
  private final PlannerContext plannerContext;
  private RexBuilder rexBuilder;

  public DruidPlanner(
      final FrameworkConfig frameworkConfig,
      final PlannerContext plannerContext
  )
  {
    this.frameworkConfig = frameworkConfig;
    this.planner = Frameworks.getPlanner(frameworkConfig);
    this.plannerContext = plannerContext;
  }

  /**
   * Validates an SQL query and collects a {@link ValidationResult} which contains a set of
   * {@link org.apache.druid.server.security.Resource} corresponding to any Druid datasources or views which are taking
   * part in the query
   */
  public ValidationResult validate(final String sql) throws SqlParseException, ValidationException
  {
    reset();
    SqlNode parsed = planner.parse(sql);
    if (parsed.getKind() == SqlKind.EXPLAIN) {
      SqlExplain explain = (SqlExplain) parsed;
      parsed = explain.getExplicandum();
    }
    SqlValidator validator = getValidator();
    SqlNode validated;
    try {
      validated = validator.validate(rewriteDynamicParameters(parsed));
    }
    catch (RuntimeException e) {
      throw new ValidationException(e);
    }
    SqlResourceCollectorShuttle resourceCollectorShuttle =
        new SqlResourceCollectorShuttle(validator, frameworkConfig.getDefaultSchema().getName());
    validated.accept(resourceCollectorShuttle);
    plannerContext.setResources(resourceCollectorShuttle.getResources());
    return new ValidationResult(resourceCollectorShuttle.getResources());
  }

  /**
   * Prepare an SQL query for execution, including some initial parsing and validation and any dyanmic parameter type
   * resolution, to support prepared statements via JDBC.
   *
   * In some future this could perhaps re-use some of the work done by {@link #validate(String)}
   * instead of repeating it, but that day is not today.
   */
  public PrepareResult prepare(final String sql) throws SqlParseException, ValidationException, RelConversionException
  {
    reset();
    SqlNode parsed = planner.parse(sql);
    SqlExplain explain = null;
    if (parsed.getKind() == SqlKind.EXPLAIN) {
      explain = (SqlExplain) parsed;
      parsed = explain.getExplicandum();
    }
    final SqlNode validated = planner.validate(parsed);
    RelRoot root = planner.rel(validated);
    RelDataType rowType = root.validatedRowType;

    SqlValidator validator = getValidator();
    RelDataType parameterTypes = validator.getParameterRowType(validator.validate(parsed));

    if (explain != null) {
      final RelDataTypeFactory typeFactory = root.rel.getCluster().getTypeFactory();
      return new PrepareResult(getExplainStructType(typeFactory), parameterTypes);
    }
    return new PrepareResult(rowType, parameterTypes);
  }

  /**
   * Plan an SQL query for execution, returning a {@link PlannerResult} which can be used to actually execute the query.
   *
   * Ideally, the query can be planned into a native Druid query, using
   * {@link #planWithDruidConvention(SqlExplain, RelRoot)}, but will fall-back to
   * {@link #planWithBindableConvention(SqlExplain, RelRoot)} if this is not possible.
   *
   * In some future this could perhaps re-use some of the work done by {@link #validate(String)}
   * instead of repeating it, but that day is not today.
   */
  public PlannerResult plan(final String sql) throws SqlParseException, ValidationException, RelConversionException
  {
    reset();
    SqlExplain explain = null;
    SqlNode parsed = planner.parse(sql);
    if (parsed.getKind() == SqlKind.EXPLAIN) {
      explain = (SqlExplain) parsed;
      parsed = explain.getExplicandum();
    }
    // the planner's type factory is not available until after parsing
    this.rexBuilder = new RexBuilder(planner.getTypeFactory());
    SqlNode parametized = rewriteDynamicParameters(parsed);

    final SqlNode validated = planner.validate(parametized);
    final RelRoot root = planner.rel(validated);

    try {
      return planWithDruidConvention(explain, root);
    }
    catch (RelOptPlanner.CannotPlanException e) {
      // Try again with BINDABLE convention. Used for querying Values and metadata tables.
      try {
        return planWithBindableConvention(explain, root);
      }
      catch (Exception e2) {
        e.addSuppressed(e2);
        throw e;
      }
    }
  }

  public PlannerContext getPlannerContext()
  {
    return plannerContext;
  }

  @Override
  public void close()
  {
    planner.close();
  }

  /**
   * While the actual query might not have changed, if the druid planner is re-used, we still have the need to reset the
   * {@link #planner} since we do not re-use artifacts or keep track of state between
   * {@link #validate}, {@link #prepare}, and {@link #plan} and instead repeat parsing and validation
   * for each step.
   *
   * Currently, that state tracking is done in {@link org.apache.druid.sql.SqlLifecycle}, which will create a new
   * planner for each of the corresponding steps so this isn't strictly necessary at this time, this method is here as
   * much to make this situation explicit and provide context for a future refactor as anything else (and some tests
   * do re-use the planner between validate, prepare, and plan, which will run into this issue).
   *
   * This could be improved by tying {@link org.apache.druid.sql.SqlLifecycle} and {@link DruidPlanner} states more
   * closely with the state of {@link #planner}, instead of repeating parsing and validation between each of these
   * steps.
   */
  private void reset()
  {
    planner.close();
    planner.reset();
  }

  /**
   * Construct a {@link PlannerResult} for a {@link RelNode} that is directly translatable to a native Druid query.
   */
  private PlannerResult planWithDruidConvention(
      final SqlExplain explain,
      final RelRoot root
  ) throws RelConversionException
  {
    final RelNode possiblyWrappedRootRel = possiblyWrapRootWithOuterLimitFromContext(root);

    RelNode parametized = rewriteRelDynamicParameters(possiblyWrappedRootRel);
    final DruidRel<?> druidRel = (DruidRel<?>) planner.transform(
        Rules.DRUID_CONVENTION_RULES,
        planner.getEmptyTraitSet()
               .replace(DruidConvention.instance())
               .plus(root.collation),
        parametized
    );

    if (explain != null) {
      return planExplanation(druidRel, explain);
    } else {
      final Supplier<Sequence<Object[]>> resultsSupplier = () -> {
        // sanity check
        Preconditions.checkState(
            plannerContext.getResources().isEmpty() == druidRel.getDataSourceNames().isEmpty(),
            "Authorization sanity check failed"
        );
        if (root.isRefTrivial()) {
          return druidRel.runQuery();
        } else {
          // Add a mapping on top to accommodate root.fields.
          return Sequences.map(
              druidRel.runQuery(),
              input -> {
                final Object[] retVal = new Object[root.fields.size()];
                for (int i = 0; i < root.fields.size(); i++) {
                  retVal[i] = input[root.fields.get(i).getKey()];
                }
                return retVal;
              }
          );
        }
      };

      return new PlannerResult(resultsSupplier, root.validatedRowType);
    }
  }

  /**
   * Construct a {@link PlannerResult} for a fall-back 'bindable' rel, for things that are not directly translatable
   * to native Druid queries such as system tables and just a general purpose (but definitely not optimized) fall-back.
   *
   * See {@link #planWithDruidConvention(SqlExplain, RelRoot)} which will handle things which are directly translatable
   * to native Druid queries.
   */
  private PlannerResult planWithBindableConvention(
      final SqlExplain explain,
      final RelRoot root
  ) throws RelConversionException
  {
    BindableRel bindableRel = (BindableRel) planner.transform(
        Rules.BINDABLE_CONVENTION_RULES,
        planner.getEmptyTraitSet().replace(BindableConvention.INSTANCE).plus(root.collation),
        root.rel
    );

    if (!root.isRefTrivial()) {
      // Add a projection on top to accommodate root.fields.
      final List<RexNode> projects = new ArrayList<>();
      final RexBuilder rexBuilder = bindableRel.getCluster().getRexBuilder();
      for (int field : Pair.left(root.fields)) {
        projects.add(rexBuilder.makeInputRef(bindableRel, field));
      }
      bindableRel = new Bindables.BindableProject(
          bindableRel.getCluster(),
          bindableRel.getTraitSet(),
          bindableRel,
          projects,
          root.validatedRowType
      );
    }

    if (explain != null) {
      return planExplanation(bindableRel, explain);
    } else {
      final BindableRel theRel = bindableRel;
      final DataContext dataContext = plannerContext.createDataContext(
          (JavaTypeFactory) planner.getTypeFactory(),
          plannerContext.getParameters()
      );
      final Supplier<Sequence<Object[]>> resultsSupplier = () -> {
        final Enumerable<?> enumerable = theRel.bind(dataContext);
        final Enumerator<?> enumerator = enumerable.enumerator();
        return Sequences.withBaggage(new BaseSequence<>(
            new BaseSequence.IteratorMaker<Object[], EnumeratorIterator<Object[]>>()
            {
              @Override
              public EnumeratorIterator<Object[]> make()
              {
                return new EnumeratorIterator<>(new Iterator<Object[]>()
                {
                  @Override
                  public boolean hasNext()
                  {
                    return enumerator.moveNext();
                  }

                  @Override
                  public Object[] next()
                  {
                    return (Object[]) enumerator.current();
                  }
                });
              }

              @Override
              public void cleanup(EnumeratorIterator<Object[]> iterFromMake)
              {

              }
            }
        ), enumerator::close);
      };
      return new PlannerResult(resultsSupplier, root.validatedRowType);
    }
  }

  /**
   * Construct a {@link PlannerResult} for an 'explain' query from a {@link RelNode}
   */
  private PlannerResult planExplanation(
      final RelNode rel,
      final SqlExplain explain
  )
  {
    final String explanation = RelOptUtil.dumpPlan("", rel, explain.getFormat(), explain.getDetailLevel());
    final Supplier<Sequence<Object[]>> resultsSupplier = Suppliers.ofInstance(
        Sequences.simple(ImmutableList.of(new Object[]{explanation})));
    return new PlannerResult(resultsSupplier, getExplainStructType(rel.getCluster().getTypeFactory()));
  }

  /**
   * This method wraps the root with a {@link LogicalSort} that applies a limit (no ordering change). If the outer rel
   * is already a {@link Sort}, we can merge our outerLimit into it, similar to what is going on in
   * {@link org.apache.druid.sql.calcite.rule.SortCollapseRule}.
   *
   * The {@link PlannerContext#CTX_SQL_OUTER_LIMIT} flag that controls this wrapping is meant for internal use only by
   * the web console, allowing it to apply a limit to queries without rewriting the original SQL.
   *
   * @param root root node
   *
   * @return root node wrapped with a limiting logical sort if a limit is specified in the query context.
   */
  @Nullable
  private RelNode possiblyWrapRootWithOuterLimitFromContext(RelRoot root)
  {
    Object outerLimitObj = plannerContext.getQueryContext().get(PlannerContext.CTX_SQL_OUTER_LIMIT);
    Long outerLimit = DimensionHandlerUtils.convertObjectToLong(outerLimitObj, true);
    if (outerLimit == null) {
      return root.rel;
    }

    if (root.rel instanceof Sort) {
      Sort sort = (Sort) root.rel;

      final OffsetLimit originalOffsetLimit = OffsetLimit.fromSort(sort);
      final OffsetLimit newOffsetLimit = originalOffsetLimit.andThen(new OffsetLimit(0, outerLimit));

      if (newOffsetLimit.equals(originalOffsetLimit)) {
        // nothing to do, don't bother to make a new sort
        return root.rel;
      }

      return LogicalSort.create(
          sort.getInput(),
          sort.collation,
          newOffsetLimit.getOffsetAsRexNode(rexBuilder),
          newOffsetLimit.getLimitAsRexNode(rexBuilder)
      );
    } else {
      return LogicalSort.create(
          root.rel,
          root.collation,
          null,
          new OffsetLimit(0, outerLimit).getLimitAsRexNode(rexBuilder)
      );
    }
  }

  private static RelDataType getExplainStructType(RelDataTypeFactory typeFactory)
  {
    return typeFactory.createStructType(
        ImmutableList.of(Calcites.createSqlType(typeFactory, SqlTypeName.VARCHAR)),
        ImmutableList.of("PLAN")
    );
  }

  /**
   * Constructs an SQL validator, just like papa {@link #planner} uses.
   */
  private SqlValidator getValidator()
  {
    // this is sort of lame, planner won't cough up its validator, which is nice and seeded after validating a query,
    // but it is private and has no accessors, so make another one so we can get the parameter types... but i suppose
    // beats creating our own Prepare and Planner implementations
    Preconditions.checkNotNull(planner.getTypeFactory());

    final CalciteConnectionConfig connectionConfig;

    if (frameworkConfig.getContext() != null) {
      connectionConfig = frameworkConfig.getContext().unwrap(CalciteConnectionConfig.class);
    } else {
      Properties properties = new Properties();
      properties.setProperty(
          CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
          String.valueOf(PlannerFactory.PARSER_CONFIG.caseSensitive())
      );
      connectionConfig = new CalciteConnectionConfigImpl(properties);
    }

    Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
        CalciteSchema.from(frameworkConfig.getDefaultSchema().getParentSchema()),
        CalciteSchema.from(frameworkConfig.getDefaultSchema()).path(null),
        planner.getTypeFactory(),
        connectionConfig
    );

    return SqlValidatorUtil.newValidator(
        frameworkConfig.getOperatorTable(),
        catalogReader,
        planner.getTypeFactory(),
        DruidConformance.instance()
    );
  }

  /**
   * Uses {@link SqlParameterizerShuttle} to rewrite {@link SqlNode} to swap out any
   * {@link org.apache.calcite.sql.SqlDynamicParam} early for their {@link org.apache.calcite.sql.SqlLiteral}
   * replacement
   */
  private SqlNode rewriteDynamicParameters(SqlNode parsed)
  {
    if (!plannerContext.getParameters().isEmpty()) {
      SqlParameterizerShuttle sshuttle = new SqlParameterizerShuttle(plannerContext);
      return parsed.accept(sshuttle);
    }
    return parsed;
  }

  /**
   * Fall-back dynamic parameter substitution using {@link RelParameterizerShuttle} in the event that
   * {@link #rewriteDynamicParameters(SqlNode)} was unable to successfully substitute all parameter values, and will
   * cause a failure if any dynamic a parameters are not bound.
   */
  private RelNode rewriteRelDynamicParameters(RelNode rootRel)
  {
    RelParameterizerShuttle parameterizer = new RelParameterizerShuttle(plannerContext);
    return rootRel.accept(parameterizer);
  }

  private static class EnumeratorIterator<T> implements Iterator<T>
  {
    private final Iterator<T> it;

    EnumeratorIterator(Iterator<T> it)
    {
      this.it = it;
    }

    @Override
    public boolean hasNext()
    {
      return it.hasNext();
    }

    @Override
    public T next()
    {
      return it.next();
    }
  }
}

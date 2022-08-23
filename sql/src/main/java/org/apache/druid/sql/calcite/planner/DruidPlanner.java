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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.Query;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlParserUtils;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.rel.DruidConvention;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidUnionRel;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.utils.Throwables;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Druid SQL planner. Wraps the underlying Calcite planner with Druid-specific
 * actions around resource validation and conversion of the Calcite logical
 * plan into a Druid native query.
 * <p>
 * The planner is designed to use once: it makes one trip through its
 * lifecycle defined as:
 * <p>
 * start --> validate [--> prepare] --> plan
 */
public class DruidPlanner implements Closeable
{
  public enum State
  {
    START, VALIDATED, PREPARED, PLANNED
  }

  private static final EmittingLogger log = new EmittingLogger(DruidPlanner.class);
  private static final Pattern UNNAMED_COLUMN_PATTERN = Pattern.compile("^EXPR\\$\\d+$", Pattern.CASE_INSENSITIVE);
  @VisibleForTesting
  public static final String UNNAMED_INGESTION_COLUMN_ERROR =
      "Cannot ingest expressions that do not have an alias "
          + "or columns with names like EXPR$[digit].\n"
          + "E.g. if you are ingesting \"func(X)\", then you can rewrite it as "
          + "\"func(X) as myColumn\"";

  private final FrameworkConfig frameworkConfig;
  private final CalcitePlanner planner;
  private final PlannerContext plannerContext;
  private final SqlEngine engine;
  private State state = State.START;
  private ParsedNodes parsed;
  private SqlNode validatedQueryNode;
  private boolean authorized;
  private PrepareResult prepareResult;
  private Set<ResourceAction> resourceActions;
  private RelRoot rootQueryRel;
  private RexBuilder rexBuilder;

  DruidPlanner(
      final FrameworkConfig frameworkConfig,
      final PlannerContext plannerContext,
      final SqlEngine engine
  )
  {
    this.frameworkConfig = frameworkConfig;
    this.planner = new CalcitePlanner(frameworkConfig);
    this.plannerContext = plannerContext;
    this.engine = engine;
  }

  /**
   * Validates a SQL query and populates {@link PlannerContext#getResourceActions()}.
   *
   * @return set of {@link Resource} corresponding to any Druid datasources
   * or views which are taking part in the query.
   */
  public void validate() throws SqlParseException, ValidationException
  {
    Preconditions.checkState(state == State.START);

    // Validate query context.
    engine.validateContext(plannerContext.getQueryContext());

    // Parse the query string.
    SqlNode root = planner.parse(plannerContext.getSql());
    parsed = ParsedNodes.create(root, plannerContext.getTimeZone());

    if (parsed.isSelect() && !plannerContext.engineHasFeature(EngineFeature.CAN_SELECT)) {
      throw new ValidationException(StringUtils.format("Cannot execute SELECT with SQL engine '%s'.", engine.name()));
    } else if (parsed.isInsert() && !plannerContext.engineHasFeature(EngineFeature.CAN_INSERT)) {
      throw new ValidationException(StringUtils.format("Cannot execute INSERT with SQL engine '%s'.", engine.name()));
    } else if (parsed.isReplace() && !plannerContext.engineHasFeature(EngineFeature.CAN_REPLACE)) {
      throw new ValidationException(StringUtils.format("Cannot execute REPLACE with SQL engine '%s'.", engine.name()));
    }

    try {
      if (parsed.getIngestionGranularity() != null) {
        plannerContext.getQueryContext().addSystemParam(
            DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
            plannerContext.getJsonMapper().writeValueAsString(parsed.getIngestionGranularity())
        );
      }
    }
    catch (JsonProcessingException e) {
      throw new ValidationException("Unable to serialize partition granularity.");
    }

    if (parsed.getReplaceIntervals() != null) {
      plannerContext.getQueryContext().addSystemParam(
          DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS,
          String.join(",", parsed.getReplaceIntervals())
      );
    }

    try {
      // Uses {@link SqlParameterizerShuttle} to rewrite {@link SqlNode} to swap out any
      // {@link org.apache.calcite.sql.SqlDynamicParam} early for their {@link SqlLiteral}
      // replacement.
      //
      // Parameter replacement is done only if the client provides parameter values.
      // If this is a PREPARE-only, then there will be no values even if the statement contains
      // parameters. If this is a PLAN, then we'll catch later the case that the statement
      // contains parameters, but no values were provided.
      SqlNode queryNode = parsed.getQueryNode();
      if (!plannerContext.getParameters().isEmpty()) {
        queryNode = queryNode.accept(new SqlParameterizerShuttle(plannerContext));
      }
      validatedQueryNode = planner.validate(queryNode);
    }
    catch (RuntimeException e) {
      throw new ValidationException(e);
    }

    final SqlValidator validator = planner.getValidator();
    SqlResourceCollectorShuttle resourceCollectorShuttle = new SqlResourceCollectorShuttle(validator, plannerContext);
    validatedQueryNode.accept(resourceCollectorShuttle);

    resourceActions = new HashSet<>(resourceCollectorShuttle.getResourceActions());

    if (parsed.isInsert() || parsed.isReplace()) {
      // Check if CTX_SQL_OUTER_LIMIT is specified and fail the query if it is. CTX_SQL_OUTER_LIMIT being provided causes
      // the number of rows inserted to be limited which is likely to be confusing and unintended.
      if (plannerContext.getQueryContext().get(PlannerContext.CTX_SQL_OUTER_LIMIT) != null) {
        throw new ValidationException(
            StringUtils.format(
                "%s cannot be provided with %s.",
                PlannerContext.CTX_SQL_OUTER_LIMIT,
                parsed.getInsertOrReplace().getOperator().getName()
            )
        );
      }
      final String targetDataSource = validateAndGetDataSourceForIngest(parsed.getInsertOrReplace());
      resourceActions.add(new ResourceAction(new Resource(targetDataSource, ResourceType.DATASOURCE), Action.WRITE));
    }

    state = State.VALIDATED;
    plannerContext.setResourceActions(resourceActions);
  }

  /**
   * Prepare a SQL query for execution, including some initial parsing and
   * validation and any dynamic parameter type resolution, to support prepared
   * statements via JDBC.
   *
   * Prepare reuses the validation done in {@link #validate()} which must be
   * called first.
   *
   * A query can be prepared on a data source without having permissions on
   * that data source. This odd state of affairs is necessary because
   * {@link org.apache.druid.sql.calcite.view.DruidViewMacro} prepares
   * a view while having no information about the user of that view.
   */
  public PrepareResult prepare()
  {
    Preconditions.checkState(state == State.VALIDATED);

    rootQueryRel = planner.rel(validatedQueryNode);
    doPrepare();
    state = State.PREPARED;
    return prepareResult;
  }

  private void doPrepare()
  {
    final RelDataTypeFactory typeFactory = rootQueryRel.rel.getCluster().getTypeFactory();
    final SqlValidator validator = planner.getValidator();
    final RelDataType parameterTypes = validator.getParameterRowType(validatedQueryNode);
    final RelDataType returnedRowType;

    if (parsed.getExplainNode() != null) {
      returnedRowType = getExplainStructType(typeFactory);
    } else if (parsed.isSelect()) {
      returnedRowType = engine.resultTypeForSelect(typeFactory, rootQueryRel.validatedRowType);
    } else {
      assert parsed.insertOrReplace != null;
      returnedRowType = engine.resultTypeForInsert(typeFactory, rootQueryRel.validatedRowType);
    }

    prepareResult = new PrepareResult(rootQueryRel.validatedRowType, returnedRowType, parameterTypes);
  }

  /**
   * Authorizes the statement. Done within the planner to enforce the authorization
   * step within the planner's state machine.
   *
   * @param authorizer a function from resource actions to a {@link Access} result.
   *
   * @return the return value from the authorizer
   */
  public Access authorize(Function<Set<ResourceAction>, Access> authorizer, boolean authorizeContextParams)
  {
    Preconditions.checkState(state == State.VALIDATED);
    Access access = authorizer.apply(resourceActions(authorizeContextParams));
    plannerContext.setAuthorizationResult(access);

    // Authorization is done as a flag, not a state, alas.
    // Views do prepare without authorize, Avatica does authorize, then prepare,
    // so the only constraint is that authorize be done after validation, before plan.
    authorized = true;
    return access;
  }

  /**
   * Return the resource actions corresponding to the datasources and views which
   * an authenticated request must be authorized for to process the
   * query. The actions will be {@code null} if the
   * planner has not yet advanced to the validation step. This may occur if
   * validation fails and the caller accesses the resource
   * actions as part of clean-up.
   */
  public Set<ResourceAction> resourceActions(boolean includeContext)
  {
    if (includeContext) {
      Set<ResourceAction> actions = new HashSet<>(resourceActions);
      plannerContext.getQueryContext().getUserParams().keySet().forEach(contextParam -> actions.add(
          new ResourceAction(new Resource(contextParam, ResourceType.QUERY_CONTEXT), Action.WRITE)
      ));
      return actions;
    } else {
      return resourceActions;
    }
  }

  /**
   * Plan an SQL query for execution, returning a {@link PlannerResult} which can be used to actually execute the query.
   *
   * Ideally, the query can be planned into a native Druid query, using {@link #planWithDruidConvention}, but will
   * fall-back to {@link #planWithBindableConvention} if this is not possible.
   *
   * Planning reuses the validation done in `validate()` which must be called first.
   */
  public PlannerResult plan() throws ValidationException
  {
    Preconditions.checkState(state == State.VALIDATED || state == State.PREPARED);
    Preconditions.checkState(authorized);
    if (state == State.VALIDATED) {
      rootQueryRel = planner.rel(validatedQueryNode);
    }

    final Set<RelOptTable> bindableTables = getBindableTables(rootQueryRel.rel);

    // the planner's type factory is not available until after parsing
    this.rexBuilder = new RexBuilder(planner.getTypeFactory());
    state = State.PLANNED;

    try {
      if (!bindableTables.isEmpty()) {
        // Consider BINDABLE convention when necessary. Used for metadata tables.

        if (parsed.isInsert() || parsed.isReplace()) {
          // Throws ValidationException if the target table is itself bindable.
          validateAndGetDataSourceForIngest(parsed.getInsertOrReplace());
        }

        if (!plannerContext.engineHasFeature(EngineFeature.ALLOW_BINDABLE_PLAN)) {
          throw new ValidationException(
              StringUtils.format(
                  "Cannot query table%s [%s] with SQL engine '%s'.",
                  bindableTables.size() != 1 ? "s" : "",
                  bindableTables.stream()
                                .map(table -> Joiner.on(".").join(table.getQualifiedName()))
                                .collect(Collectors.joining(", ")),
                  engine.name()
              )
          );
        }

        return planWithBindableConvention(rootQueryRel, parsed.getExplainNode());
      } else {
        // DRUID convention is used whenever there are no tables that require BINDABLE.
        return planWithDruidConvention(rootQueryRel, parsed.getExplainNode(), parsed.getInsertOrReplace());
      }
    }
    catch (Exception e) {
      Throwable cannotPlanException = Throwables.getCauseOfType(e, RelOptPlanner.CannotPlanException.class);
      if (null == cannotPlanException) {
        // Not a CannotPlanException, rethrow without logging.
        throw e;
      }

      Logger logger = log;
      if (!plannerContext.getQueryContext().isDebug()) {
        logger = log.noStackTrace();
      }
      String errorMessage = buildSQLPlanningErrorMessage(cannotPlanException);
      logger.warn(e, errorMessage);
      throw new UnsupportedSQLQueryException(errorMessage);
    }
  }

  public PlannerContext getPlannerContext()
  {
    return plannerContext;
  }

  public PrepareResult prepareResult()
  {
    return prepareResult;
  }

  @Override
  public void close()
  {
    planner.close();
  }

  /**
   * Construct a {@link PlannerResult} for a {@link RelNode} that is directly translatable to a native Druid query.
   */
  private PlannerResult planWithDruidConvention(
      final RelRoot root,
      @Nullable final SqlExplain explain,
      @Nullable final SqlInsert insertOrReplace
  ) throws ValidationException
  {
    final RelRoot possiblyLimitedRoot = possiblyWrapRootWithOuterLimitFromContext(root);
    final QueryMaker queryMaker = buildQueryMaker(possiblyLimitedRoot, insertOrReplace);
    plannerContext.setQueryMaker(queryMaker);
    if (prepareResult == null) {
      doPrepare();
    }

    // Fall-back dynamic parameter substitution using {@link RelParameterizerShuttle}
    // in the event that {@link #rewriteDynamicParameters(SqlNode)} was unable to
    // successfully substitute all parameter values, and will cause a failure if any
    // dynamic a parameters are not bound. This occurs at least for DATE parameters
    // with integer values.
    //
    // This check also catches the case where we did not do a parameter check earlier
    // because no values were provided. (Values are not required in the PREPARE case
    // but now that we're planning, we require them.)
    RelNode parameterized = possiblyLimitedRoot.rel.accept(
        new RelParameterizerShuttle(plannerContext)
    );
    final DruidRel<?> druidRel = (DruidRel<?>) planner.transform(
        CalciteRulesManager.DRUID_CONVENTION_RULES,
        planner.getEmptyTraitSet()
               .replace(DruidConvention.instance())
               .plus(root.collation),
        parameterized
    );

    if (explain != null) {
      return planExplanation(druidRel, explain, true);
    } else {
      // Compute row type.
      final RelDataTypeFactory typeFactory = rootQueryRel.rel.getCluster().getTypeFactory();
      final RelDataType rowType;

      if (parsed.isSelect()) {
        rowType = engine.resultTypeForSelect(typeFactory, rootQueryRel.validatedRowType);
      } else {
        assert parsed.insertOrReplace != null;
        rowType = engine.resultTypeForInsert(typeFactory, rootQueryRel.validatedRowType);
      }

      // Start the query.
      final Supplier<Sequence<Object[]>> resultsSupplier = () -> {
        // sanity check
        final Set<ResourceAction> readResourceActions =
            plannerContext.getResourceActions()
                          .stream()
                          .filter(action -> action.getAction() == Action.READ)
                          .collect(Collectors.toSet());
        Preconditions.checkState(
            readResourceActions.isEmpty() == druidRel.getDataSourceNames().isEmpty()
            // The resources found in the plannerContext can be less than the datasources in
            // the query plan, because the query planner can eliminate empty tables by replacing
            // them with InlineDataSource of empty rows.
            || readResourceActions.size() >= druidRel.getDataSourceNames().size(),
            "Authorization sanity check failed"
        );

        return druidRel.runQuery();
      };

      return new PlannerResult(resultsSupplier, rowType);
    }
  }

  /**
   * Construct a {@link PlannerResult} for a fall-back 'bindable' rel, for
   * things that are not directly translatable to native Druid queries such
   * as system tables and just a general purpose (but definitely not optimized)
   * fall-back.
   *
   * See {@link #planWithDruidConvention} which will handle things which are
   * directly translatable to native Druid queries.
   *
   * The bindable path handles parameter substitution of any values not
   * bound by the earlier steps.
   */
  private PlannerResult planWithBindableConvention(
      final RelRoot root,
      @Nullable final SqlExplain explain
  )
  {
    if (prepareResult == null) {
      doPrepare();
    }

    BindableRel bindableRel = (BindableRel) planner.transform(
        CalciteRulesManager.BINDABLE_CONVENTION_RULES,
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
      return planExplanation(bindableRel, explain, false);
    } else {
      final BindableRel theRel = bindableRel;
      final DataContext dataContext = plannerContext.createDataContext(
              planner.getTypeFactory(),
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
      final SqlExplain explain,
      final boolean isDruidConventionExplanation
  )
  {
    String explanation = RelOptUtil.dumpPlan("", rel, explain.getFormat(), explain.getDetailLevel());
    String resourcesString;
    try {
      if (isDruidConventionExplanation && rel instanceof DruidRel) {
        // Show the native queries instead of Calcite's explain if the legacy flag is turned off
        if (plannerContext.getPlannerConfig().isUseNativeQueryExplain()) {
          DruidRel<?> druidRel = (DruidRel<?>) rel;
          try {
            explanation = explainSqlPlanAsNativeQueries(druidRel);
          }
          catch (Exception ex) {
            log.warn(ex, "Unable to translate to a native Druid query. Resorting to legacy Druid explain plan");
          }
        }
      }
      final Set<Resource> resources =
          plannerContext.getResourceActions().stream().map(ResourceAction::getResource).collect(Collectors.toSet());
      resourcesString = plannerContext.getJsonMapper().writeValueAsString(resources);
    }
    catch (JsonProcessingException jpe) {
      // this should never happen, we create the Resources here, not a user
      log.error(jpe, "Encountered exception while serializing Resources for explain output");
      resourcesString = null;
    }
    final Supplier<Sequence<Object[]>> resultsSupplier = Suppliers.ofInstance(
        Sequences.simple(ImmutableList.of(new Object[]{explanation, resourcesString})));
    return new PlannerResult(resultsSupplier, getExplainStructType(rel.getCluster().getTypeFactory()));
  }

  /**
   * This method doesn't utilize the Calcite's internal {@link RelOptUtil#dumpPlan} since that tends to be verbose
   * and not indicative of the native Druid Queries which will get executed
   * This method assumes that the Planner has converted the RelNodes to DruidRels, and thereby we can implicitly cast it
   *
   * @param rel Instance of the root {@link DruidRel} which is formed by running the planner transformations on it
   * @return A string representing an array of native queries that correspond to the given SQL query, in JSON format
   * @throws JsonProcessingException
   */
  private String explainSqlPlanAsNativeQueries(DruidRel<?> rel) throws JsonProcessingException
  {
    ObjectMapper jsonMapper = plannerContext.getJsonMapper();
    List<DruidQuery> druidQueryList;
    druidQueryList = flattenOutermostRel(rel)
        .stream()
        .map(druidRel -> druidRel.toDruidQuery(false))
        .collect(Collectors.toList());


    // Putting the queries as object node in an ArrayNode, since directly returning a list causes issues when
    // serializing the "queryType". Another method would be to create a POJO containing query and signature, and then
    // serializing it using normal list method.
    ArrayNode nativeQueriesArrayNode = jsonMapper.createArrayNode();

    for (DruidQuery druidQuery : druidQueryList) {
      Query<?> nativeQuery = druidQuery.getQuery();
      ObjectNode objectNode = jsonMapper.createObjectNode();
      objectNode.put("query", jsonMapper.convertValue(nativeQuery, ObjectNode.class));
      objectNode.put("signature", jsonMapper.convertValue(druidQuery.getOutputRowSignature(), ArrayNode.class));
      nativeQueriesArrayNode.add(objectNode);
    }

    return jsonMapper.writeValueAsString(nativeQueriesArrayNode);
  }

  /**
   * Given a {@link DruidRel}, this method recursively flattens the Rels if they are of the type {@link DruidUnionRel}
   * It is implicitly assumed that the {@link DruidUnionRel} can never be the child of a non {@link DruidUnionRel}
   * node
   * For eg, a DruidRel structure of kind:
   * DruidUnionRel
   *  DruidUnionRel
   *    DruidRel (A)
   *    DruidRel (B)
   *  DruidRel(C)
   * will return [DruidRel(A), DruidRel(B), DruidRel(C)]
   *
   * @param outermostDruidRel The outermost rel which is to be flattened
   * @return a list of DruidRel's which donot have a DruidUnionRel nested in between them
   */
  private List<DruidRel<?>> flattenOutermostRel(DruidRel<?> outermostDruidRel)
  {
    List<DruidRel<?>> druidRels = new ArrayList<>();
    flattenOutermostRel(outermostDruidRel, druidRels);
    return druidRels;
  }

  /**
   * Recursive function (DFS) which traverses the nodes and collects the corresponding {@link DruidRel} into a list if
   * they are not of the type {@link DruidUnionRel} or else calls the method with the child nodes. The DFS order of the
   * nodes are retained, since that is the order in which they will actually be called in {@link DruidUnionRel#runQuery()}
   *
   * @param druidRel                The current relNode
   * @param flattendListAccumulator Accumulator list which needs to be appended by this method
   */
  private void flattenOutermostRel(DruidRel<?> druidRel, List<DruidRel<?>> flattendListAccumulator)
  {
    if (druidRel instanceof DruidUnionRel) {
      DruidUnionRel druidUnionRel = (DruidUnionRel) druidRel;
      druidUnionRel.getInputs().forEach(innerRelNode -> {
        DruidRel<?> innerDruidRelNode = (DruidRel<?>) innerRelNode; // This type conversion should always be possible
        flattenOutermostRel(innerDruidRelNode, flattendListAccumulator);
      });
    } else {
      flattendListAccumulator.add(druidRel);
    }
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
  private RelRoot possiblyWrapRootWithOuterLimitFromContext(RelRoot root)
  {
    Object outerLimitObj = plannerContext.getQueryContext().get(PlannerContext.CTX_SQL_OUTER_LIMIT);
    Long outerLimit = DimensionHandlerUtils.convertObjectToLong(outerLimitObj, true);
    if (outerLimit == null) {
      return root;
    }

    final LogicalSort newRootRel;

    if (root.rel instanceof Sort) {
      Sort sort = (Sort) root.rel;

      final OffsetLimit originalOffsetLimit = OffsetLimit.fromSort(sort);
      final OffsetLimit newOffsetLimit = originalOffsetLimit.andThen(new OffsetLimit(0, outerLimit));

      if (newOffsetLimit.equals(originalOffsetLimit)) {
        // nothing to do, don't bother to make a new sort
        return root;
      }

      newRootRel = LogicalSort.create(
          sort.getInput(),
          sort.collation,
          newOffsetLimit.getOffsetAsRexNode(rexBuilder),
          newOffsetLimit.getLimitAsRexNode(rexBuilder)
      );
    } else {
      newRootRel = LogicalSort.create(
          root.rel,
          root.collation,
          null,
          new OffsetLimit(0, outerLimit).getLimitAsRexNode(rexBuilder)
      );
    }

    return new RelRoot(newRootRel, root.validatedRowType, root.kind, root.fields, root.collation);
  }

  private QueryMaker buildQueryMaker(
      final RelRoot rootQueryRel,
      @Nullable final SqlInsert insertOrReplace
  ) throws ValidationException
  {
    if (insertOrReplace != null) {
      final String targetDataSource = validateAndGetDataSourceForIngest(insertOrReplace);
      validateColumnsForIngestion(rootQueryRel);
      return engine.buildQueryMakerForInsert(targetDataSource, rootQueryRel, plannerContext);
    } else {
      return engine.buildQueryMakerForSelect(rootQueryRel, plannerContext);
    }
  }

  private static RelDataType getExplainStructType(RelDataTypeFactory typeFactory)
  {
    return typeFactory.createStructType(
        ImmutableList.of(
            Calcites.createSqlType(typeFactory, SqlTypeName.VARCHAR),
            Calcites.createSqlType(typeFactory, SqlTypeName.VARCHAR)
        ),
        ImmutableList.of("PLAN", "RESOURCES")
    );
  }

  /**
   * Extract target datasource from a {@link SqlInsert}, and also validate that the ingestion is of a form we support.
   * Expects the target datasource to be either an unqualified name, or a name qualified by the default schema.
   */
  private String validateAndGetDataSourceForIngest(final SqlInsert insert) throws ValidationException
  {
    final String operatorName = insert.getOperator().getName();
    if (insert.isUpsert()) {
      throw new ValidationException("UPSERT is not supported.");
    }

    if (insert.getTargetColumnList() != null) {
      throw new ValidationException(operatorName + " with target column list is not supported.");
    }

    final SqlIdentifier tableIdentifier = (SqlIdentifier) insert.getTargetTable();
    final String dataSource;

    if (tableIdentifier.names.isEmpty()) {
      // I don't think this can happen, but include a branch for it just in case.
      throw new ValidationException(operatorName + " requires target table.");
    } else if (tableIdentifier.names.size() == 1) {
      // Unqualified name.
      dataSource = Iterables.getOnlyElement(tableIdentifier.names);
    } else {
      // Qualified name.
      final String defaultSchemaName =
          Iterables.getOnlyElement(CalciteSchema.from(frameworkConfig.getDefaultSchema()).path(null));

      if (tableIdentifier.names.size() == 2 && defaultSchemaName.equals(tableIdentifier.names.get(0))) {
        dataSource = tableIdentifier.names.get(1);
      } else {
        throw new ValidationException(
            StringUtils.format(
                "Cannot %s into [%s] because it is not a Druid datasource (schema = %s).",
                operatorName,
                tableIdentifier,
                defaultSchemaName
            )
        );
      }
    }

    try {
      IdUtils.validateId(operatorName + " dataSource", dataSource);
    }
    catch (IllegalArgumentException e) {
      throw new ValidationException(e.getMessage());
    }

    return dataSource;
  }

  private void validateColumnsForIngestion(RelRoot rootQueryRel) throws ValidationException
  {
    // Check that there are no unnamed columns in the insert.
    for (Pair<Integer, String> field : rootQueryRel.fields) {
      if (UNNAMED_COLUMN_PATTERN.matcher(field.right).matches()) {
        throw new ValidationException(UNNAMED_INGESTION_COLUMN_ERROR);
      }
    }
  }

  private String buildSQLPlanningErrorMessage(Throwable exception)
  {
    String errorMessage = plannerContext.getPlanningError();
    if (null == errorMessage && exception instanceof UnsupportedSQLQueryException) {
      errorMessage = exception.getMessage();
    }
    if (null == errorMessage) {
      errorMessage = "Please check Broker logs for more details.";
    } else {
      // Re-phrase since planning errors are more like hints
      errorMessage = "Possible error: " + errorMessage;
    }
    // Finally, add the query itself to error message that user will get.
    return StringUtils.format("Cannot build plan for query. %s", errorMessage);
  }

  private static Set<RelOptTable> getBindableTables(final RelNode relNode)
  {
    class HasBindableVisitor extends RelVisitor
    {
      private final Set<RelOptTable> found = new HashSet<>();

      @Override
      public void visit(RelNode node, int ordinal, RelNode parent)
      {
        if (node instanceof TableScan) {
          RelOptTable table = node.getTable();
          if (table.unwrap(ScannableTable.class) != null && table.unwrap(DruidTable.class) == null) {
            found.add(table);
            return;
          }
        }

        super.visit(node, ordinal, parent);
      }
    }

    final HasBindableVisitor visitor = new HasBindableVisitor();
    visitor.go(relNode);
    return visitor.found;
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

  private static class ParsedNodes
  {
    @Nullable
    private final SqlExplain explain;

    @Nullable
    private final SqlInsert insertOrReplace;

    private final SqlNode query;

    @Nullable
    private final Granularity ingestionGranularity;

    @Nullable
    private final List<String> replaceIntervals;

    private ParsedNodes(
        @Nullable SqlExplain explain,
        @Nullable SqlInsert insertOrReplace,
        SqlNode query,
        @Nullable Granularity ingestionGranularity,
        @Nullable List<String> replaceIntervals
    )
    {
      this.explain = explain;
      this.insertOrReplace = insertOrReplace;
      this.query = query;
      this.ingestionGranularity = ingestionGranularity;
      this.replaceIntervals = replaceIntervals;
    }

    static ParsedNodes create(final SqlNode node, DateTimeZone dateTimeZone) throws ValidationException
    {
      SqlNode query = node;
      SqlExplain explain = null;
      if (query.getKind() == SqlKind.EXPLAIN) {
        explain = (SqlExplain) query;
        query = explain.getExplicandum();
      }

      if (query.getKind() == SqlKind.INSERT) {
        if (query instanceof DruidSqlInsert) {
          return handleInsert(explain, (DruidSqlInsert) query);
        } else if (query instanceof DruidSqlReplace) {
          return handleReplace(explain, (DruidSqlReplace) query, dateTimeZone);
        }
      }

      if (!query.isA(SqlKind.QUERY)) {
        throw new ValidationException(StringUtils.format("Cannot execute [%s].", query.getKind()));
      }

      return new ParsedNodes(explain, null, query, null, null);
    }

    static ParsedNodes handleInsert(SqlExplain explain, DruidSqlInsert druidSqlInsert) throws ValidationException
    {
      SqlNode query = druidSqlInsert.getSource();

      // Check if ORDER BY clause is not provided to the underlying query
      if (query instanceof SqlOrderBy) {
        SqlOrderBy sqlOrderBy = (SqlOrderBy) query;
        SqlNodeList orderByList = sqlOrderBy.orderList;
        if (!(orderByList == null || orderByList.equals(SqlNodeList.EMPTY))) {
          throw new ValidationException("Cannot have ORDER BY on an INSERT query, use CLUSTERED BY instead.");
        }
      }

      Granularity ingestionGranularity = druidSqlInsert.getPartitionedBy();

      if (druidSqlInsert.getClusteredBy() != null) {
        query = DruidSqlParserUtils.convertClusterByToOrderBy(query, druidSqlInsert.getClusteredBy());
      }

      if (!query.isA(SqlKind.QUERY)) {
        throw new ValidationException(StringUtils.format("Cannot execute [%s].", query.getKind()));
      }

      return new ParsedNodes(explain, druidSqlInsert, query, ingestionGranularity, null);
    }

    static ParsedNodes handleReplace(SqlExplain explain, DruidSqlReplace druidSqlReplace, DateTimeZone dateTimeZone)
        throws ValidationException
    {
      SqlNode query = druidSqlReplace.getSource();

      // Check if ORDER BY clause is not provided to the underlying query
      if (query instanceof SqlOrderBy) {
        SqlOrderBy sqlOrderBy = (SqlOrderBy) query;
        SqlNodeList orderByList = sqlOrderBy.orderList;
        if (!(orderByList == null || orderByList.equals(SqlNodeList.EMPTY))) {
          throw new ValidationException("Cannot have ORDER BY on a REPLACE query, use CLUSTERED BY instead.");
        }
      }

      SqlNode replaceTimeQuery = druidSqlReplace.getReplaceTimeQuery();
      if (replaceTimeQuery == null) {
        throw new ValidationException("Missing time chunk information in OVERWRITE clause for REPLACE, set it to OVERWRITE WHERE <__time based condition> or set it to overwrite the entire table with OVERWRITE ALL.");
      }

      Granularity ingestionGranularity = druidSqlReplace.getPartitionedBy();
      List<String> replaceIntervals = DruidSqlParserUtils.validateQueryAndConvertToIntervals(replaceTimeQuery, ingestionGranularity, dateTimeZone);

      if (druidSqlReplace.getClusteredBy() != null) {
        query = DruidSqlParserUtils.convertClusterByToOrderBy(query, druidSqlReplace.getClusteredBy());
      }

      if (!query.isA(SqlKind.QUERY)) {
        throw new ValidationException(StringUtils.format("Cannot execute [%s].", query.getKind()));
      }

      return new ParsedNodes(explain, druidSqlReplace, query, ingestionGranularity, replaceIntervals);
    }

    @Nullable
    public SqlExplain getExplainNode()
    {
      return explain;
    }

    public boolean isSelect()
    {
      return insertOrReplace == null;
    }

    public boolean isInsert()
    {
      return insertOrReplace != null && !isReplace();
    }

    public boolean isReplace()
    {
      return insertOrReplace instanceof DruidSqlReplace;
    }

    @Nullable
    public SqlInsert getInsertOrReplace()
    {
      return insertOrReplace;
    }

    @Nullable
    public List<String> getReplaceIntervals()
    {
      return replaceIntervals;
    }

    public SqlNode getQueryNode()
    {
      return query;
    }

    @Nullable
    public Granularity getIngestionGranularity()
    {
      return ingestionGranularity;
    }
  }
}

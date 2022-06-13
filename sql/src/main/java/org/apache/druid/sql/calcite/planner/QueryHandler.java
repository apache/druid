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
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.rel.DruidConvention;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidUnionRel;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.utils.Throwables;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract base class for handlers that revolve around queries: SELECT,
 * INSERT and REPLACE. This class handles the common SELECT portion of the statement.
 */
abstract class QueryHandler extends BaseStatementHandler
{
  protected SqlNode queryNode;
  protected SqlExplain explain;
  protected SqlNode validatedQueryNode;
  private RelRoot rootQueryRel;

  public QueryHandler(HandlerContext handlerContext, SqlNode sqlNode, SqlExplain explain)
  {
    super(handlerContext);
    this.queryNode = sqlNode;
    this.explain = explain;
  }

  protected abstract boolean allowsBindableExec();

  protected SqlNode validateQuery() throws ValidationException
  {
    validatedQueryNode = validateNode(rewriteDynamicParameters(queryNode));
    SqlResourceCollectorShuttle resourceCollectorShuttle =
        new SqlResourceCollectorShuttle(
            handlerContext.planner().getValidator(),
            handlerContext.plannerContext());
    validatedQueryNode.accept(resourceCollectorShuttle);
    resourceActions.addAll(resourceCollectorShuttle.getResourceActions());
    return validatedQueryNode;
  }

  @Override
  public PrepareResult prepare() throws RelConversionException, ValidationException
  {
    rootQueryRel = handlerContext.planner().rel(validatedQueryNode);

    final RelDataTypeFactory typeFactory = rootQueryRel.rel.getCluster().getTypeFactory();
    final SqlValidator validator = handlerContext.planner().getValidator();
    final RelDataType parameterTypes = validator.getParameterRowType(validatedQueryNode);
    final RelDataType returnedRowType;

    if (explain != null) {
      returnedRowType = getExplainStructType(typeFactory);
    } else {
      returnedRowType = buildQueryMaker(rootQueryRel).getResultType();
    }
    return new PrepareResult(returnedRowType, parameterTypes);
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

  protected abstract QueryMaker buildQueryMaker(RelRoot rootQueryRel) throws ValidationException;

  /**
   * Plan an SQL query for execution, returning a {@link PlannerResult} which
   * can be used to actually execute the query.
   *
   * Ideally, the query can be planned into a native Druid query, using
   * {@link #planWithDruidConvention}, but will fall-back to
   * {@link #planWithBindableConvention} if this is not possible.
   */
  @Override
  public PlannerResult plan() throws ValidationException, RelConversionException
  {
    CalcitePlanner planner = handlerContext.planner();
    if (rootQueryRel == null) {
      // Set if the prepare step was done, null if jump straight to plan.
      rootQueryRel = planner.rel(validatedQueryNode);
    }

    // the planner's type factory is not available until after parsing
    RexBuilder rexBuilder = new RexBuilder(planner.getTypeFactory());

    try {
      return planWithDruidConvention(rootQueryRel, rexBuilder);
    }
    catch (Exception e) {
      Throwable cannotPlanException = Throwables.getCauseOfType(e, RelOptPlanner.CannotPlanException.class);
      if (null == cannotPlanException) {
        // Not a CannotPlanException, rethrow without trying with bindable
        throw e;
      }

      // If there isn't any ingestion clause, then we should try again with BINDABLE convention. And return without
      // any error, if it is plannable by the bindable convention
      if (allowsBindableExec()) {
        // Try again with BINDABLE convention. Used for querying Values and metadata tables.
        try {
          return planWithBindableConvention(rootQueryRel);
        }
        catch (Exception e2) {
          e.addSuppressed(e2);
        }
      }
      Logger logger = DruidPlanner.log;
      if (!handlerContext.queryContext().isDebug()) {
        logger = DruidPlanner.log.noStackTrace();
      }
      String errorMessage = buildSQLPlanningErrorMessage(cannotPlanException);
      logger.warn(e, errorMessage);
      throw new UnsupportedSQLQueryException(errorMessage);
    }
  }

  /**
   * Construct a {@link PlannerResult} for a {@link RelNode} that is directly translatable to a native Druid query.
   */
  private PlannerResult planWithDruidConvention(
      final RelRoot root,
      final RexBuilder rexBuilder
  ) throws ValidationException, RelConversionException
  {
    CalcitePlanner planner = handlerContext.planner();
    final RelRoot possiblyLimitedRoot = possiblyWrapRootWithOuterLimitFromContext(root, rexBuilder);

    // Create query maker before applying the Druid rules. The rules refer
    // to the query maker via the planner context.
    final QueryMaker queryMaker = buildQueryMaker(possiblyLimitedRoot);
    handlerContext.plannerContext().setQueryMaker(queryMaker);

    RelNode parameterized = rewriteRelDynamicParameters(possiblyLimitedRoot.rel);
    final DruidRel<?> druidRel = (DruidRel<?>) planner.transform(
        Rules.DRUID_CONVENTION_RULES,
        planner.getEmptyTraitSet()
               .replace(DruidConvention.instance())
               .plus(root.collation),
        parameterized
    );

    if (explain != null) {
      return planExplanation(druidRel, true);
    } else {
      return planDruidExecution(druidRel, possiblyLimitedRoot, queryMaker);
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
   * @return root node wrapped with a limiting logical sort if a limit is specified in the query context.
   */
  @Nullable
  private RelRoot possiblyWrapRootWithOuterLimitFromContext(RelRoot root, RexBuilder rexBuilder)
  {
    Object outerLimitObj = handlerContext.queryContext().get(PlannerContext.CTX_SQL_OUTER_LIMIT);
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

  /**
   * Fall-back dynamic parameter substitution using {@link RelParameterizerShuttle} in the event that
   * {@link #rewriteDynamicParameters(SqlNode)} was unable to successfully substitute all parameter values, and will
   * cause a failure if any dynamic a parameters are not bound.
   */
  private RelNode rewriteRelDynamicParameters(RelNode rootRel)
  {
    RelParameterizerShuttle parameterizer = new RelParameterizerShuttle(
        handlerContext.plannerContext());
    return rootRel.accept(parameterizer);
  }

  private PlannerResult planDruidExecution(
      final DruidRel<?> druidRel,
      RelRoot possiblyLimitedRoot,
      QueryMaker queryMaker
  )
  {
    final Supplier<Sequence<Object[]>> resultsSupplier = () -> {
      // sanity check
      final Set<ResourceAction> readResourceActions =
          handlerContext
                        .plannerContext()
                        .getResourceActions()
                        .stream()
                        .filter(action -> action.getAction() == Action.READ)
                        .collect(Collectors.toSet());

      // TODO: This is not really a state check since there is a race condition.
      // This can be seen as verifying that a check was done, or as redoing the
      // check with the latest info (if the permissions are updated in between.)
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

    return new PlannerResult(resultsSupplier, queryMaker.getResultType());
  }

  /**
   * Construct a {@link PlannerResult} for a fall-back 'bindable' rel, for things that are not directly translatable
   * to native Druid queries such as system tables and just a general purpose (but definitely not optimized) fall-back.
   *
   * See {@link #planWithDruidConvention} which will handle things which are directly translatable
   * to native Druid queries.
   */
  private PlannerResult planWithBindableConvention(
      final RelRoot root
  ) throws RelConversionException
  {
    CalcitePlanner planner = handlerContext.planner();
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
      return planExplanation(bindableRel, false);
    } else {
      return planBindableExecution(root, bindableRel);
    }
  }

  /**
   * Construct a {@link PlannerResult} for an 'explain' query from a {@link RelNode}
   */
  private PlannerResult planExplanation(
      final RelNode rel,
      final boolean isDruidConventionExplanation
  )
  {
    PlannerContext plannerContext = handlerContext.plannerContext();
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
            DruidPlanner.log.warn(ex, "Unable to translate to a native Druid query. Resorting to legacy Druid explain plan");
          }
        }
      }
      final Set<Resource> resources =
          plannerContext.getResourceActions().stream().map(ResourceAction::getResource).collect(Collectors.toSet());
      resourcesString = plannerContext.getJsonMapper().writeValueAsString(resources);
    }
    catch (JsonProcessingException jpe) {
      // this should never happen, we create the Resources here, not a user
      DruidPlanner.log.error(jpe, "Encountered exception while serializing Resources for explain output");
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
    ObjectMapper jsonMapper = handlerContext.plannerContext().getJsonMapper();
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

  private PlannerResult planBindableExecution(
      final RelRoot root,
      final BindableRel bindableRel
  )
  {
    CalcitePlanner planner = handlerContext.planner();
    final BindableRel theRel = bindableRel;
    final DataContext dataContext = handlerContext.plannerContext().createDataContext(
        (JavaTypeFactory) planner.getTypeFactory(),
        handlerContext.parameters()
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

  private String buildSQLPlanningErrorMessage(Throwable exception)
  {
    PlannerContext plannerContext = handlerContext.plannerContext();
    String errorMessage = plannerContext.getPlanningError();
    if (null == errorMessage && exception instanceof UnsupportedSQLQueryException) {
      errorMessage = exception.getMessage();
    }
    if (null == errorMessage) {
      errorMessage = "Please check broker logs for more details";
    } else {
      // Re-phrase since planning errors are more like hints
      errorMessage = "Possible error: " + errorMessage;
    }
    // Finally, add the query itself to error message that user will get.
    return StringUtils.format("Cannot build plan for query: %s. %s", plannerContext.getSql(), errorMessage);
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

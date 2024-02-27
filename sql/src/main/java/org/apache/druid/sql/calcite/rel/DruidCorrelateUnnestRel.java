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

package org.apache.druid.sql.calcite.rel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.builtin.MultiValueStringToArrayOperatorConversion;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is the DruidRel to handle correlated rel nodes to be used for unnest.
 * Each correlate can be perceived as a join with the join type being inner
 * the left of a correlate as seen in the rule {@link org.apache.druid.sql.calcite.rule.DruidCorrelateUnnestRule}
 * is the {@link DruidQueryRel} while the right will always be an {@link DruidUnnestRel}.
 * <p>
 * Since this is a subclass of DruidRel it is automatically considered by other rules that involves DruidRels.
 * Some example being SELECT_PROJECT and SORT_PROJECT rules in {@link org.apache.druid.sql.calcite.rule.DruidRules.DruidQueryRule}
 */
public class DruidCorrelateUnnestRel extends DruidRel<DruidCorrelateUnnestRel>
{
  private static final TableDataSource DUMMY_DATA_SOURCE = new TableDataSource("__correlate_unnest__");
  private static final String BASE_UNNEST_OUTPUT_COLUMN = "unnest";

  private final Correlate correlateRel;
  private final RelNode left;
  private final RelNode right;
  private final PartialDruidQuery partialQuery;

  private DruidCorrelateUnnestRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      Correlate correlateRel,
      PartialDruidQuery partialQuery,
      PlannerContext plannerContext
  )
  {
    super(cluster, traitSet, plannerContext);
    this.correlateRel = correlateRel;
    this.partialQuery = partialQuery;
    this.left = correlateRel.getLeft();
    this.right = correlateRel.getRight();
  }

  /**
   * Create an instance from a Correlate that is based on a {@link DruidRel} and a {@link DruidUnnestRel} inputs.
   */
  public static DruidCorrelateUnnestRel create(
      final Correlate correlateRel,
      final PlannerContext plannerContext
  )
  {
    return new DruidCorrelateUnnestRel(
        correlateRel.getCluster(),
        correlateRel.getTraitSet(),
        correlateRel,
        PartialDruidQuery.create(correlateRel),
        plannerContext
    );
  }

  @Nullable
  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialQuery;
  }

  @Override
  public DruidCorrelateUnnestRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    return new DruidCorrelateUnnestRel(
        getCluster(),
        newQueryBuilder.getTraitSet(getConvention()),
        correlateRel,
        newQueryBuilder,
        getPlannerContext()
    );
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    final DruidRel<?> leftDruidRel = (DruidRel<?>) left;
    final PartialDruidQuery leftPartialQuery = leftDruidRel.getPartialDruidQuery();
    final DruidQuery leftQuery = Preconditions.checkNotNull(leftDruidRel.toDruidQuery(false), "leftQuery");
    final DruidUnnestRel unnestDatasourceRel = (DruidUnnestRel) right;
    final DataSource leftDataSource;
    DataSource leftDataSource1;
    final RowSignature leftDataSourceSignature;
    final Filter unnestFilter = unnestDatasourceRel.getUnnestFilter();

    if (right.getRowType().getFieldNames().size() != 1) {
      throw new CannotBuildQueryException("Cannot perform correlated join + UNNEST with more than one column");
    }

    // Compute the expression to unnest.
    final RexNode rexNodeToUnnest = getRexNodeToUnnest(correlateRel, unnestDatasourceRel);
    if (unnestDatasourceRel.getInputRexNode().getKind() == SqlKind.OTHER_FUNCTION) {
      // Left side is doing more than simple scan: generate a subquery.
      leftDataSourceSignature = leftQuery.getOutputRowSignature();
    } else {
      if (leftDruidRel instanceof DruidOuterQueryRel) {
        leftDataSourceSignature = DruidRels.dataSourceSignature((DruidRel<?>) leftDruidRel.getInputs().get(0));
      } else {
        leftDataSourceSignature = DruidRels.dataSourceSignature(leftDruidRel);
      }
    }
    final DruidExpression expressionToUnnest = Expressions.toDruidExpression(
        getPlannerContext(),
        leftDataSourceSignature,
        rexNodeToUnnest
    );


    if (expressionToUnnest == null) {
      throw new CannotBuildQueryException(unnestDatasourceRel, unnestDatasourceRel.getInputRexNode());
    }


    // Final output row signature.
    final RowSignature correlateRowSignature = getCorrelateRowSignature(correlateRel, leftQuery);
    final DimFilter unnestFilterOnDataSource;
    if (unnestFilter != null) {
      RowSignature filterRowSignature = RowSignatures.fromRelDataType(ImmutableList.of(correlateRowSignature.getColumnName(
          correlateRowSignature.size() - 1)), unnestFilter.getInput().getRowType());
      unnestFilterOnDataSource = Filtration.create(DruidQuery.getDimFilter(
                                               getPlannerContext(),
                                               filterRowSignature,
                                               null,
                                               unnestFilter
                                           ))
                                           .optimizeFilterOnly(filterRowSignature).getDimFilter();
    } else {
      unnestFilterOnDataSource = null;
    }

    final DruidRel<?> newLeftDruidRel;
    final DruidQuery updatedLeftQuery;
    // For some queries, with Calcite 1.35
    // The plan has an MV_TO_ARRAY on the left side
    // with a reference to it on the right side
    // IN such a case we use a RexShuttle to remove the reference on the left
    // And rewrite the left project
    // Added the isSimpleExtraction() check to prevent a NPE
    // When using an expression in the value to be unnested
    // If the simpleExtraction is null, we do not create the shuttle to update the projects
    if (unnestDatasourceRel.getInputRexNode().getKind() == SqlKind.FIELD_ACCESS && expressionToUnnest.isSimpleExtraction()) {
      final PartialDruidQuery leftPartialQueryToBeUpdated;
      if (leftDruidRel instanceof DruidOuterQueryRel) {
        leftPartialQueryToBeUpdated = ((DruidRel) leftDruidRel.getInputs().get(0)).getPartialDruidQuery();
      } else {
        leftPartialQueryToBeUpdated = leftPartialQuery;
      }
      final Project leftProject = leftPartialQueryToBeUpdated.getSelectProject();
      final String dimensionToUpdate = expressionToUnnest.getDirectColumn();
      final LogicalProject newProject;
      if (leftProject == null) {
        newProject = null;
      } else {
        // Use shuttle to update left project
        final ProjectUpdateShuttle pus = new ProjectUpdateShuttle(
            unwrapMvToArray(rexNodeToUnnest),
            leftProject,
            dimensionToUpdate
        );
        final List<RexNode> out = pus.visitList(leftProject.getProjects());
        final RelDataType structType = RexUtil.createStructType(getCluster().getTypeFactory(), out, pus.getTypeNames());
        newProject = LogicalProject.create(
            leftProject.getInput(),
            leftProject.getHints(),
            out,
            structType
        );
      }

      final DruidRel leftFinalRel;
      if (leftDruidRel instanceof DruidOuterQueryRel) {
        leftFinalRel = (DruidRel) leftDruidRel.getInputs().get(0);
      } else {
        leftFinalRel = leftDruidRel;
      }
      final PartialDruidQuery pq = PartialDruidQuery.create(leftPartialQueryToBeUpdated.getScan())
                                                    .withWhereFilter(leftPartialQueryToBeUpdated.getWhereFilter())
                                                    .withSelectProject(newProject)
                                                    .withSort(leftPartialQueryToBeUpdated.getSort());

      // The same MV_TO_ARRAY on left and reference to the right can happen during
      // SORT_PROJECT and SELECT_PROJECT stages
      // We use the same methodology of using a shuttle to rewrite the projects.
      if (leftPartialQuery.stage() == PartialDruidQuery.Stage.SORT_PROJECT) {
        final Project sortProject = leftPartialQueryToBeUpdated.getSortProject();
        final ProjectUpdateShuttle pus = new ProjectUpdateShuttle(
            unwrapMvToArray(rexNodeToUnnest),
            sortProject,
            dimensionToUpdate
        );
        final List<RexNode> out = pus.visitList(sortProject.getProjects());
        final RelDataType structType = RexUtil.createStructType(getCluster().getTypeFactory(), out, pus.getTypeNames());
        final Project newSortProject = LogicalProject.create(
            sortProject.getInput(),
            sortProject.getHints(),
            out,
            structType
        );
        newLeftDruidRel = leftFinalRel.withPartialQuery(pq.withSortProject(newSortProject));
      } else {
        newLeftDruidRel = leftFinalRel.withPartialQuery(pq);
      }
    } else {
      newLeftDruidRel = leftDruidRel;
    }
    updatedLeftQuery = Preconditions.checkNotNull(newLeftDruidRel.toDruidQuery(false), "leftQuery");

    if (newLeftDruidRel.getPartialDruidQuery().stage().compareTo(PartialDruidQuery.Stage.SELECT_PROJECT) <= 0) {
      final Filter whereFilter = newLeftDruidRel.getPartialDruidQuery().getWhereFilter();
      final RowSignature leftSignature = DruidRels.dataSourceSignature(newLeftDruidRel);
      if (whereFilter == null) {
        if (computeLeftRequiresSubquery(newLeftDruidRel)) {
          // Left side is doing more than simple scan: generate a subquery.
          leftDataSource1 = new QueryDataSource(updatedLeftQuery.getQuery());
        } else {
          leftDataSource1 = updatedLeftQuery.getDataSource();
        }
      } else {
        final DimFilter dimFilter = Filtration.create(DruidQuery.getDimFilter(
                                                  getPlannerContext(),
                                                  leftSignature,
                                                  null,
                                                  whereFilter
                                              ))
                                              .optimizeFilterOnly(leftSignature).getDimFilter();
        leftDataSource1 = FilteredDataSource.create(updatedLeftQuery.getDataSource(), dimFilter);
      }
    } else {
      leftDataSource1 = new QueryDataSource(updatedLeftQuery.getQuery());
    }

    leftDataSource = leftDataSource1;
    return partialQuery.build(
        UnnestDataSource.create(
            leftDataSource,
            expressionToUnnest.toVirtualColumn(
                correlateRowSignature.getColumnName(correlateRowSignature.size() - 1),
                Calcites.getColumnTypeForRelDataType(rexNodeToUnnest.getType()),
                getPlannerContext().getExpressionParser()
            ),
            unnestFilterOnDataSource
        ),
        correlateRowSignature,
        getPlannerContext(),
        getCluster().getRexBuilder(),
        finalizeAggregations,
        null
    );
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.getRowType();
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return partialQuery.build(
        DUMMY_DATA_SOURCE,
        RowSignatures.fromRelDataType(
            correlateRel.getRowType().getFieldNames(),
            correlateRel.getRowType()
        ),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        false
    );
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    final String queryString;
    final DruidQuery druidQuery = toDruidQueryForExplaining();

    try {
      queryString = getPlannerContext().getJsonMapper().writeValueAsString(druidQuery.getQuery());
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return correlateRel.explainTerms(pw)
                       .item("query", queryString)
                       .item("signature", druidQuery.getOutputRowSignature());
  }

  // This is called from the DruidRelToDruidRule which converts from the NONE convention to the DRUID convention
  @Override
  public DruidCorrelateUnnestRel asDruidConvention()
  {
    return new DruidCorrelateUnnestRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        correlateRel.copy(
            correlateRel.getTraitSet(),
            correlateRel.getInputs()
                        .stream()
                        .map(input -> RelOptRule.convert(input, DruidConvention.instance()))
                        .collect(Collectors.toList())
        ),
        partialQuery,
        getPlannerContext()
    );
  }

  @Override
  public List<RelNode> getInputs()
  {
    return ImmutableList.of(left, right);
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidCorrelateUnnestRel(
        getCluster(),
        traitSet,
        correlateRel.copy(correlateRel.getTraitSet(), inputs),
        getPartialDruidQuery(),
        getPlannerContext()
    );
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    double cost = partialQuery.estimateCost();

    if (computeLeftRequiresSubquery(DruidJoinQueryRel.getSomeDruidChild(left))) {
      cost += CostEstimates.COST_SUBQUERY;
    }

    return planner.getCostFactory().makeCost(cost, 0, 0);
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    final Set<String> retVal = new HashSet<>();
    retVal.addAll(((DruidRel<?>) left).getDataSourceNames());
    retVal.addAll(((DruidRel<?>) right).getDataSourceNames());
    return retVal;
  }

  /**
   * Computes whether a particular left-side rel requires a subquery, or if we can operate on its underlying
   * datasource directly.
   * <p>
   * Stricter than {@link DruidJoinQueryRel#computeLeftRequiresSubquery}: this method only allows scans (not mappings).
   * This is OK because any mapping or other simple projection would have been pulled above the {@link Correlate} by
   * {@link org.apache.druid.sql.calcite.rule.DruidCorrelateUnnestRule}.
   */
  public static boolean computeLeftRequiresSubquery(final DruidRel<?> left)
  {
    return left == null || left.getPartialDruidQuery().stage() != PartialDruidQuery.Stage.SCAN;
  }

  /**
   * Whether an expr is MV_TO_ARRAY of an input reference.
   */
  private static boolean isMvToArrayOfInputRef(final RexNode expr)
  {
    return expr.isA(SqlKind.OTHER_FUNCTION)
           && ((RexCall) expr).op.equals(MultiValueStringToArrayOperatorConversion.SQL_FUNCTION)
           && ((RexCall) expr).getOperands().get(0).isA(SqlKind.INPUT_REF);
  }

  /**
   * Unwrap MV_TO_ARRAY at the outer layer of an expr, if it refers to an input ref.
   */
  private static RexNode unwrapMvToArray(final RexNode expr)
  {
    if (isMvToArrayOfInputRef(expr)) {
      return ((RexCall) expr).getOperands().get(0);
    } else {
      return expr;
    }
  }

  /**
   * Compute the row signature of this rel, given a particular left-hand {@link DruidQuery}.
   * The right-hand side is assumed to have a single column with the name {@link #BASE_UNNEST_OUTPUT_COLUMN}.
   */
  private static RowSignature getCorrelateRowSignature(
      final Correlate correlate,
      final DruidQuery leftQuery
  )
  {
    // Compute signature of the correlation operation. It's like a join: the left and right sides are concatenated.
    // On the native query side, this is what is ultimately emitted by the UnnestStorageAdapter.
    //
    // Ignore prefix (lhs) from computeJoinRowSignature; we don't need this since we will declare the name of the
    // single output column directly. (And we know it's the last column in the signature.)
    final RelDataType unnestedType =
        correlate.getRowType().getFieldList().get(correlate.getRowType().getFieldCount() - 1).getType();

    return DruidJoinQueryRel.computeJoinRowSignature(
        leftQuery.getOutputRowSignature(),
        RowSignature.builder().add(
            BASE_UNNEST_OUTPUT_COLUMN,
            Calcites.getColumnTypeForRelDataType(unnestedType)
        ).build(),
        DruidJoinQueryRel.findExistingJoinPrefixes(leftQuery.getDataSource())
    ).rhs;
  }

  /**
   * Return the expression to unnest from the left-hand side. Correlation variable references are rewritten to
   * regular field accesses, i.e., {@link RexInputRef}.
   */
  private static RexNode getRexNodeToUnnest(
      final Correlate correlate,
      final DruidUnnestRel unnestDatasourceRel
  )
  {
    // Update unnestDatasourceRel.getUnnestProject() so it refers to the left-hand side rather than the correlation
    // variable. This is the expression to unnest.
    final RexNode unnestRexNode;
    final PartialDruidQuery partialDruidQuery;
    if (correlate.getLeft() instanceof DruidOuterQueryRel) {
      partialDruidQuery = ((DruidRel) correlate.getLeft().getInputs().get(0)).getPartialDruidQuery();
    } else if (correlate.getLeft() instanceof DruidQueryRel) {
      partialDruidQuery = ((DruidQueryRel) correlate.getLeft()).getPartialDruidQuery();
    } else {
      partialDruidQuery = ((DruidRel) correlate.getLeft()).getPartialDruidQuery();
    }
    // The mv_to_array can appear either in
    // 1. select project for the left
    // 2. sort project for the left
    final Project leftProject = partialDruidQuery.getSelectProject();
    final Project sortProject = partialDruidQuery.getSortProject();

    if (leftProject == null && sortProject == null) {
      unnestRexNode = unnestDatasourceRel.getInputRexNode();
    } else {
      if (unnestDatasourceRel.getInputRexNode().getKind() == SqlKind.FIELD_ACCESS) {
        final int indexRef = ((RexFieldAccess) unnestDatasourceRel.getInputRexNode()).getField().getIndex();
        if (leftProject != null) {
          unnestRexNode = leftProject
              .getProjects()
              .get(indexRef);
        } else {
          unnestRexNode = sortProject
              .getProjects()
              .get(indexRef);
        }
      } else {
        unnestRexNode = unnestDatasourceRel.getInputRexNode();
      }
    }

    final RexNode rexNodeToUnnest =
        new CorrelatedFieldAccessToInputRef(correlate.getCorrelationId())
            .apply(unnestRexNode);

    return unwrapMvToArray(rexNodeToUnnest);
  }

  private static class ProjectUpdateShuttle extends RexShuttle
  {
    private RexNode nodeToBeAdded;
    private List<String> types;
    private Project project;
    private String dim;

    public ProjectUpdateShuttle(final RexNode node, Project project, String dimension)
    {
      nodeToBeAdded = node;
      this.project = project;
      types = new ArrayList<>(project.getProjects().size());
      dim = dimension;
    }

    public List<String> getTypeNames()
    {
      return types;
    }

    @Override
    public void visitList(
        Iterable<? extends RexNode> exprs,
        List<RexNode> out
    )
    {
      final List<String> typeNames = project.getRowType().getFieldNames();
      int i = 0;
      for (RexNode r : exprs) {
        RexNode updatedExpr = unwrapMvToArray(r);
        if (!Objects.equals(updatedExpr, nodeToBeAdded)) {
          out.add(updatedExpr);
          types.add(typeNames.get(i));
        }
        i++;
      }
      // add the replaced one here
      out.add(nodeToBeAdded);
      types.add(dim);
    }
  }

  /**
   * Shuttle that replaces correlating variables with regular field accesses to the left-hand side.
   */
  private static class CorrelatedFieldAccessToInputRef extends RexShuttle
  {
    private final CorrelationId correlationId;

    public CorrelatedFieldAccessToInputRef(final CorrelationId correlationId)
    {
      this.correlationId = correlationId;
    }

    @Override
    public RexNode visitFieldAccess(final RexFieldAccess fieldAccess)
    {
      if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
        final RexCorrelVariable encounteredCorrelationId = (RexCorrelVariable) fieldAccess.getReferenceExpr();
        if (encounteredCorrelationId.id.equals(correlationId)) {
          return new RexInputRef(fieldAccess.getField().getIndex(), fieldAccess.getType());
        }
      }

      return super.visitFieldAccess(fieldAccess);
    }
  }
}

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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.table.RowSignatures;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DruidRel that uses a {@link JoinDataSource}.
 */
public class DruidJoinQueryRel extends DruidRel<DruidJoinQueryRel>
{
  private static final TableDataSource DUMMY_DATA_SOURCE = new TableDataSource("__join__");

  private final PartialDruidQuery partialQuery;
  private final Join joinRel;
  private RelNode left;
  private RelNode right;

  /**
   * True if {@link #left} requires a subquery.
   *
   * This is useful to store in a variable because {@link #left} is sometimes not actually a {@link DruidRel} when
   * {@link #computeSelfCost} is called. (It might be a {@link org.apache.calcite.plan.volcano.RelSubset}.)
   *
   * @see #computeLeftRequiresSubquery(DruidRel)
   */
  private final boolean leftRequiresSubquery;

  /**
   * True if {@link #right} requires a subquery.
   *
   * This is useful to store in a variable because {@link #left} is sometimes not actually a {@link DruidRel} when
   * {@link #computeSelfCost} is called. (It might be a {@link org.apache.calcite.plan.volcano.RelSubset}.)
   *
   * @see #computeLeftRequiresSubquery(DruidRel)
   */
  private final boolean rightRequiresSubquery;

  private DruidJoinQueryRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      Join joinRel,
      boolean leftRequiresSubquery,
      boolean rightRequiresSubquery,
      PartialDruidQuery partialQuery,
      QueryMaker queryMaker
  )
  {
    super(cluster, traitSet, queryMaker);
    this.joinRel = joinRel;
    this.left = joinRel.getLeft();
    this.right = joinRel.getRight();
    this.leftRequiresSubquery = leftRequiresSubquery;
    this.rightRequiresSubquery = rightRequiresSubquery;
    this.partialQuery = partialQuery;
  }

  /**
   * Create an instance from a Join that is based on two {@link DruidRel} inputs.
   */
  public static DruidJoinQueryRel create(
      final Join joinRel,
      final DruidRel<?> left,
      final DruidRel<?> right
  )
  {
    return new DruidJoinQueryRel(
        joinRel.getCluster(),
        joinRel.getTraitSet(),
        joinRel,
        computeLeftRequiresSubquery(left),
        computeRightRequiresSubquery(right),
        PartialDruidQuery.create(joinRel),
        left.getQueryMaker()
    );
  }

  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialQuery;
  }

  @Override
  public Sequence<Object[]> runQuery()
  {
    // runQuery doesn't need to finalize aggregations, because the fact that runQuery is happening suggests this
    // is the outermost query and it will actually get run as a native query. Druid's native query layer will
    // finalize aggregations for the outermost query even if we don't explicitly ask it to.

    final DruidQuery query = toDruidQuery(false);
    return getQueryMaker().runQuery(query);
  }

  @Override
  public DruidJoinQueryRel withPartialQuery(final PartialDruidQuery newQueryBuilder)
  {
    return new DruidJoinQueryRel(
        getCluster(),
        getTraitSet().plusAll(newQueryBuilder.getRelTraits()),
        joinRel,
        leftRequiresSubquery,
        rightRequiresSubquery,
        newQueryBuilder,
        getQueryMaker()
    );
  }

  @Override
  public int getQueryCount()
  {
    return ((DruidRel<?>) left).getQueryCount() + ((DruidRel<?>) right).getQueryCount();
  }

  @Override
  public DruidQuery toDruidQuery(final boolean finalizeAggregations)
  {
    final DruidRel<?> leftDruidRel = (DruidRel<?>) left;
    final DruidQuery leftQuery = Preconditions.checkNotNull((leftDruidRel).toDruidQuery(false), "leftQuery");
    final RowSignature leftSignature = leftQuery.getOutputRowSignature();
    final DataSource leftDataSource;

    final DruidRel<?> rightDruidRel = (DruidRel<?>) right;
    final DruidQuery rightQuery = Preconditions.checkNotNull(rightDruidRel.toDruidQuery(false), "rightQuery");
    final RowSignature rightSignature = rightQuery.getOutputRowSignature();
    final DataSource rightDataSource;

    if (computeLeftRequiresSubquery(leftDruidRel)) {
      assert leftRequiresSubquery;
      leftDataSource = new QueryDataSource(leftQuery.getQuery());
    } else {
      assert !leftRequiresSubquery;
      leftDataSource = leftQuery.getDataSource();
    }

    if (computeRightRequiresSubquery(rightDruidRel)) {
      assert rightRequiresSubquery;
      rightDataSource = new QueryDataSource(rightQuery.getQuery());
    } else {
      assert !rightRequiresSubquery;
      rightDataSource = rightQuery.getDataSource();
    }

    final Pair<String, RowSignature> prefixSignaturePair = computeJoinRowSignature(leftSignature, rightSignature);

    // Generate the condition for this join as a Druid expression.
    final DruidExpression condition = Expressions.toDruidExpression(
        getPlannerContext(),
        prefixSignaturePair.rhs,
        joinRel.getCondition()
    );

    // DruidJoinRule should not have created us if "condition" is null. Check defensively anyway, which also
    // quiets static code analysis.
    if (condition == null) {
      throw new CannotBuildQueryException(joinRel, joinRel.getCondition());
    }

    return partialQuery.build(
        JoinDataSource.create(
            leftDataSource,
            rightDataSource,
            prefixSignaturePair.lhs,
            condition.getExpression(),
            toDruidJoinType(joinRel.getJoinType()),
            getPlannerContext().getExprMacroTable()
        ),
        prefixSignaturePair.rhs,
        getPlannerContext(),
        getCluster().getRexBuilder(),
        finalizeAggregations
    );
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return partialQuery.build(
        DUMMY_DATA_SOURCE,
        RowSignatures.fromRelDataType(
            joinRel.getRowType().getFieldNames(),
            joinRel.getRowType()
        ),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        false
    );
  }

  @Override
  public DruidJoinQueryRel asDruidConvention()
  {
    return new DruidJoinQueryRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        joinRel.copy(
            joinRel.getTraitSet(),
            joinRel.getInputs()
                   .stream()
                   .map(input -> RelOptRule.convert(input, DruidConvention.instance()))
                   .collect(Collectors.toList())
        ),
        leftRequiresSubquery,
        rightRequiresSubquery,
        partialQuery,
        getQueryMaker()
    );
  }

  @Override
  public List<RelNode> getInputs()
  {
    return ImmutableList.of(left, right);
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p)
  {
    joinRel.replaceInput(ordinalInParent, p);

    if (ordinalInParent == 0) {
      this.left = p;
    } else if (ordinalInParent == 1) {
      this.right = p;
    } else {
      throw new IndexOutOfBoundsException(StringUtils.format("Invalid ordinalInParent[%s]", ordinalInParent));
    }
  }

  @Override
  public List<RexNode> getChildExps()
  {
    return ImmutableList.of(joinRel.getCondition());
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidJoinQueryRel(
        getCluster(),
        traitSet,
        joinRel.copy(joinRel.getTraitSet(), inputs),
        leftRequiresSubquery,
        rightRequiresSubquery,
        getPartialDruidQuery(),
        getQueryMaker()
    );
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    final Set<String> retVal = new HashSet<>();
    retVal.addAll(((DruidRel<?>) left).getDataSourceNames());
    retVal.addAll(((DruidRel<?>) right).getDataSourceNames());
    return retVal;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    final String queryString;
    final DruidQuery druidQuery = toDruidQueryForExplaining();

    try {
      queryString = getQueryMaker().getJsonMapper().writeValueAsString(druidQuery.getQuery());
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return pw.input("left", left)
             .input("right", right)
             .item("condition", joinRel.getCondition())
             .item("joinType", joinRel.getJoinType())
             .item("query", queryString)
             .item("signature", druidQuery.getOutputRowSignature());
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.getRowType();
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    return planner.getCostFactory()
                  .makeCost(partialQuery.estimateCost(), 0, 0)
                  .multiplyBy(leftRequiresSubquery ? CostEstimates.MULTIPLIER_JOIN_SUBQUERY : 1)
                  .multiplyBy(rightRequiresSubquery ? CostEstimates.MULTIPLIER_JOIN_SUBQUERY : 1);
  }

  private static JoinType toDruidJoinType(JoinRelType calciteJoinType)
  {
    switch (calciteJoinType) {
      case LEFT:
        return JoinType.LEFT;
      case RIGHT:
        return JoinType.RIGHT;
      case FULL:
        return JoinType.FULL;
      case INNER:
        return JoinType.INNER;
      default:
        throw new IAE("Cannot handle joinType[%s]", calciteJoinType);
    }
  }

  private static boolean computeLeftRequiresSubquery(final DruidRel<?> left)
  {
    // Left requires a subquery unless it's a scan or mapping on top of any table or a join.
    return !DruidRels.isScanOrMapping(left, true);
  }

  private static boolean computeRightRequiresSubquery(final DruidRel<?> right)
  {
    // Right requires a subquery unless it's a scan or mapping on top of a global datasource.
    return !(DruidRels.isScanOrMapping(right, false)
             && DruidRels.dataSourceIfLeafRel(right).filter(DataSource::isGlobal).isPresent());
  }

  /**
   * Returns a Pair of "rightPrefix" (for JoinDataSource) and the signature of rows that will result from
   * applying that prefix.
   */
  private static Pair<String, RowSignature> computeJoinRowSignature(
      final RowSignature leftSignature,
      final RowSignature rightSignature
  )
  {
    final RowSignature.Builder signatureBuilder = RowSignature.builder();

    for (final String column : leftSignature.getColumnNames()) {
      signatureBuilder.add(column, leftSignature.getColumnType(column).orElse(null));
    }

    // Need to include the "0" since findUnusedPrefixForDigits only guarantees safety for digit-initiated suffixes
    final String rightPrefix = Calcites.findUnusedPrefixForDigits("j", leftSignature.getColumnNames()) + "0.";

    for (final String column : rightSignature.getColumnNames()) {
      signatureBuilder.add(rightPrefix + column, rightSignature.getColumnType(column).orElse(null));
    }

    return Pair.of(rightPrefix, signatureBuilder.build());
  }
}

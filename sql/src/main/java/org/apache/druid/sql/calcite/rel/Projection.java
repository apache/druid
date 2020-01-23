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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.PostAggregatorVisitor;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Used to represent projections (Calcite "Project"). These are embedded in {@link Sorting} and {@link Grouping} to
 * store post-sorting and post-grouping projections, as well as directly in {@link DruidQuery} to store potential
 * post-selection projections. They may be built using either virtual columns (pre-aggregation) or post-aggregators.
 *
 * It is expected that callers will create and use Projection instances in the same context (pre- or post-aggregation).
 * If this isn't done properly (i.e. a caller creates a pre-aggregation Projection but then calls
 * {@link #getPostAggregators()} then an exception will be thrown.
 */
public class Projection
{
  @Nullable
  private final List<PostAggregator> postAggregators;

  @Nullable
  private final List<VirtualColumn> virtualColumns;

  private final RowSignature outputRowSignature;

  private Projection(
      @Nullable final List<PostAggregator> postAggregators,
      @Nullable final List<VirtualColumn> virtualColumns,
      final RowSignature outputRowSignature
  )
  {
    if (postAggregators == null && virtualColumns == null) {
      throw new IAE("postAggregators and virtualColumns cannot both be null");
    } else if (postAggregators != null && virtualColumns != null) {
      throw new IAE("postAggregators and virtualColumns cannot both be nonnull");
    }

    this.postAggregators = postAggregators;
    this.virtualColumns = virtualColumns;
    this.outputRowSignature = outputRowSignature;
  }

  private static void postAggregationHandleInputRefOrLiteral(
      final Project project,
      final PlannerContext plannerContext,
      final RowSignature inputRowSignature,
      final RexNode postAggregatorRexNode,
      final List<String> rowOrder,
      final PostAggregatorVisitor postAggregatorVisitor
  )
  {
    // Attempt to convert to PostAggregator.
    final DruidExpression postAggregatorExpression = Expressions.toDruidExpression(
        plannerContext,
        inputRowSignature,
        postAggregatorRexNode
    );

    if (postAggregatorExpression == null) {
      throw new CannotBuildQueryException(project, postAggregatorRexNode);
    }

    handlePostAggregatorExpression(
        plannerContext,
        inputRowSignature,
        postAggregatorRexNode,
        rowOrder,
        postAggregatorVisitor,
        postAggregatorExpression
    );
  }

  private static void postAggregationHandleOtherKinds(
      final Project project,
      final PlannerContext plannerContext,
      final RowSignature inputRowSignature,
      final RexNode postAggregatorRexNode,
      final List<String> rowOrder,
      final PostAggregatorVisitor postAggregatorVisitor
  )
  {
    PostAggregator pagg = OperatorConversions.toPostAggregator(
        plannerContext,
        inputRowSignature,
        postAggregatorRexNode,
        postAggregatorVisitor
    );

    if (pagg != null) {
      postAggregatorVisitor.addPostAgg(pagg);
      rowOrder.add(pagg.getName());
    } else {
      final DruidExpression postAggregatorExpression = Expressions.toDruidExpressionWithPostAggOperands(
          plannerContext,
          inputRowSignature,
          postAggregatorRexNode,
          postAggregatorVisitor
      );

      if (postAggregatorExpression == null) {
        throw new CannotBuildQueryException(project, postAggregatorRexNode);
      }

      handlePostAggregatorExpression(
          plannerContext,
          inputRowSignature,
          postAggregatorRexNode,
          rowOrder,
          postAggregatorVisitor,
          postAggregatorExpression
      );
    }
  }

  private static void handlePostAggregatorExpression(
      final PlannerContext plannerContext,
      final RowSignature inputRowSignature,
      final RexNode postAggregatorRexNode,
      final List<String> rowOrder,
      final PostAggregatorVisitor postAggregatorVisitor,
      final DruidExpression postAggregatorExpression
  )
  {
    if (postAggregatorComplexDirectColumnIsOk(inputRowSignature, postAggregatorExpression, postAggregatorRexNode)) {
      // Direct column access on a COMPLEX column, expressions cannot operate on complex columns, only postaggs
      // Wrap the column access in a field access postagg so that other postaggs can use it
      final PostAggregator postAggregator = new FieldAccessPostAggregator(
          postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
          postAggregatorExpression.getDirectColumn()
      );
      postAggregatorVisitor.addPostAgg(postAggregator);
      rowOrder.add(postAggregator.getName());
    } else if (postAggregatorDirectColumnIsOk(inputRowSignature, postAggregatorExpression, postAggregatorRexNode)) {
      // Direct column access, without any type cast as far as Druid's runtime is concerned.
      // (There might be a SQL-level type cast that we don't care about)
      rowOrder.add(postAggregatorExpression.getDirectColumn());
    } else {
      final PostAggregator postAggregator = new ExpressionPostAggregator(
          postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
          postAggregatorExpression.getExpression(),
          null,
          plannerContext.getExprMacroTable()
      );
      postAggregatorVisitor.addPostAgg(postAggregator);
      rowOrder.add(postAggregator.getName());
    }
  }

  public static Projection postAggregation(
      final Project project,
      final PlannerContext plannerContext,
      final RowSignature inputRowSignature,
      final String basePrefix
  )
  {
    final List<String> rowOrder = new ArrayList<>();
    final String outputNamePrefix = Calcites.findUnusedPrefix(
        basePrefix,
        new TreeSet<>(inputRowSignature.getRowOrder())
    );
    final PostAggregatorVisitor postAggVisitor = new PostAggregatorVisitor(outputNamePrefix);

    for (final RexNode postAggregatorRexNode : project.getChildExps()) {
      if (postAggregatorRexNode.getKind() == SqlKind.INPUT_REF || postAggregatorRexNode.getKind() == SqlKind.LITERAL) {
        postAggregationHandleInputRefOrLiteral(
            project,
            plannerContext,
            inputRowSignature,
            postAggregatorRexNode,
            rowOrder,
            postAggVisitor
        );
      } else {
        postAggregationHandleOtherKinds(
            project,
            plannerContext,
            inputRowSignature,
            postAggregatorRexNode,
            rowOrder,
            postAggVisitor
        );
      }
    }

    return new Projection(postAggVisitor.getPostAggs(), null, RowSignature.from(rowOrder, project.getRowType()));
  }

  public static Projection preAggregation(
      final Project project,
      final PlannerContext plannerContext,
      final RowSignature inputRowSignature,
      final VirtualColumnRegistry virtualColumnRegistry
  )
  {
    final List<DruidExpression> expressions = new ArrayList<>();

    for (final RexNode rexNode : project.getChildExps()) {
      final DruidExpression expression = Expressions.toDruidExpression(
          plannerContext,
          inputRowSignature,
          rexNode
      );

      if (expression == null) {
        throw new CannotBuildQueryException(project, rexNode);
      } else {
        expressions.add(expression);
      }
    }

    final Set<VirtualColumn> virtualColumns = new HashSet<>();
    final List<String> rowOrder = new ArrayList<>();

    for (int i = 0; i < expressions.size(); i++) {
      final DruidExpression expression = expressions.get(i);
      if (expression.isDirectColumnAccess()) {
        rowOrder.add(expression.getDirectColumn());
      } else {
        final VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
            plannerContext,
            expression,
            project.getChildExps().get(i).getType().getSqlTypeName()
        );
        virtualColumns.add(virtualColumn);
        rowOrder.add(virtualColumn.getOutputName());
      }
    }

    return new Projection(
        null,
        ImmutableList.copyOf(virtualColumns),
        RowSignature.from(rowOrder, project.getRowType())
    );
  }

  /**
   * Returns true if a post-aggregation "expression" can be realized as a direct field access. This is true if it's
   * a direct column access that doesn't require an implicit cast.
   *
   * @param aggregateRowSignature signature of the aggregation
   * @param expression            post-aggregation expression
   * @param rexNode               RexNode for the post-aggregation expression
   *
   * @return yes or no
   */
  private static boolean postAggregatorDirectColumnIsOk(
      final RowSignature aggregateRowSignature,
      final DruidExpression expression,
      final RexNode rexNode
  )
  {
    if (!expression.isDirectColumnAccess()) {
      return false;
    }

    // We don't really have a way to cast complex type. So might as well not do anything and return.
    final ValueType columnValueType = aggregateRowSignature.getColumnType(expression.getDirectColumn());
    if (columnValueType == ValueType.COMPLEX) {
      return true;
    }

    // Check if a cast is necessary.
    final ExprType toExprType = Expressions.exprTypeForValueType(columnValueType);
    final ExprType fromExprType = Expressions.exprTypeForValueType(
        Calcites.getValueTypeForSqlTypeName(rexNode.getType().getSqlTypeName())
    );

    return toExprType.equals(fromExprType);
  }

  /**
   * Returns true if a post-aggregation "expression" can be realized as a direct field access. This is true if it's
   * a direct column access that doesn't require an implicit cast.
   *
   * @param aggregateRowSignature signature of the aggregation
   * @param expression            post-aggregation expression
   * @param rexNode               RexNode for the post-aggregation expression
   *
   * @return yes or no
   */
  private static boolean postAggregatorComplexDirectColumnIsOk(
      final RowSignature aggregateRowSignature,
      final DruidExpression expression,
      final RexNode rexNode
  )
  {
    if (!expression.isDirectColumnAccess()) {
      return false;
    }

    // Check if a cast is necessary.
    final ValueType toValueType = aggregateRowSignature.getColumnType(expression.getDirectColumn());
    final ValueType fromValueType = Calcites.getValueTypeForSqlTypeName(rexNode.getType().getSqlTypeName());

    return toValueType == ValueType.COMPLEX && fromValueType == ValueType.COMPLEX;
  }

  public List<PostAggregator> getPostAggregators()
  {
    // If you ever see this error, it probably means a Projection was created in pre-aggregation mode, but then
    // used in a post-aggregation context. This is likely a bug somewhere in DruidQuery. See class-level Javadocs.
    return Preconditions.checkNotNull(postAggregators, "postAggregators");
  }

  public List<VirtualColumn> getVirtualColumns()
  {
    // If you ever see this error, it probably means a Projection was created in post-aggregation mode, but then
    // used in a pre-aggregation context. This is likely a bug somewhere in DruidQuery. See class-level Javadocs.
    return Preconditions.checkNotNull(virtualColumns, "virtualColumns");
  }

  public RowSignature getOutputRowSignature()
  {
    return outputRowSignature;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Projection that = (Projection) o;
    return Objects.equals(postAggregators, that.postAggregators) &&
           Objects.equals(virtualColumns, that.virtualColumns) &&
           Objects.equals(outputRowSignature, that.outputRowSignature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(postAggregators, virtualColumns, outputRowSignature);
  }

  @Override
  public String toString()
  {
    return "PostSortingExpressions{" +
           "postAggregators=" + postAggregators +
           ", virtualColumns=" + virtualColumns +
           ", outputRowSignature=" + outputRowSignature +
           '}';
  }
}

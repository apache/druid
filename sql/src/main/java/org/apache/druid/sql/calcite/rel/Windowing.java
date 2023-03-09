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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.operator.NaivePartitioningOperatorFactory;
import org.apache.druid.query.operator.NaiveSortOperatorFactory;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.window.ComposingProcessor;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.operator.window.WindowFrame;
import org.apache.druid.query.operator.window.WindowFramedAggregateProcessor;
import org.apache.druid.query.operator.window.WindowOperatorFactory;
import org.apache.druid.query.operator.window.ranking.WindowCumeDistProcessor;
import org.apache.druid.query.operator.window.ranking.WindowDenseRankProcessor;
import org.apache.druid.query.operator.window.ranking.WindowPercentileProcessor;
import org.apache.druid.query.operator.window.ranking.WindowRankProcessor;
import org.apache.druid.query.operator.window.ranking.WindowRowNumberProcessor;
import org.apache.druid.query.operator.window.value.WindowFirstProcessor;
import org.apache.druid.query.operator.window.value.WindowLastProcessor;
import org.apache.druid.query.operator.window.value.WindowOffsetProcessor;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rule.GroupByRules;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Maps onto a {@link org.apache.druid.query.operator.WindowOperatorQuery}.
 * <p>
 * Known sharp-edges/limitations:
 * 1. The support is not yet fully aware of the difference between RANGE and ROWS when evaluating peers. (Note: The
 * built-in functions are all implemented with the correctly defined semantics, so ranking functions that are
 * defined to use RANGE do the right thing)
 * 2. No support for framing last/first functions
 * 3. No nth function
 * 4. No finalization, meaning that aggregators like sketches that rely on finalization might return surprising results
 * 5. No big big test suite of loveliness
 */
public class Windowing
{
  private static final ImmutableMap<String, ProcessorMaker> KNOWN_WINDOW_FNS = ImmutableMap
      .<String, ProcessorMaker>builder()
      .put("LAG", (agg, typeFactory) ->
          new WindowOffsetProcessor(agg.getColumn(typeFactory, 0), agg.getOutputName(), -agg.getConstantInt(1)))
      .put("LEAD", (agg, typeFactory) ->
          new WindowOffsetProcessor(agg.getColumn(typeFactory, 0), agg.getOutputName(), agg.getConstantInt(1)))
      .put("FIRST_VALUE", (agg, typeFactory) ->
          new WindowFirstProcessor(agg.getColumn(typeFactory, 0), agg.getOutputName()))
      .put("LAST_VALUE", (agg, typeFactory) ->
          new WindowLastProcessor(agg.getColumn(typeFactory, 0), agg.getOutputName()))
      .put("CUME_DIST", (agg, typeFactory) ->
          new WindowCumeDistProcessor(agg.getGroup().getOrderingColumNames(), agg.getOutputName()))
      .put("DENSE_RANK", (agg, typeFactory) ->
          new WindowDenseRankProcessor(agg.getGroup().getOrderingColumNames(), agg.getOutputName()))
      .put("NTILE", (agg, typeFactory) ->
          new WindowPercentileProcessor(agg.getOutputName(), agg.getConstantInt(0)))
      .put("PERCENT_RANK", (agg, typeFactory) ->
          new WindowRankProcessor(agg.getGroup().getOrderingColumNames(), agg.getOutputName(), true))
      .put("RANK", (agg, typeFactory) ->
          new WindowRankProcessor(agg.getGroup().getOrderingColumNames(), agg.getOutputName(), false))
      .put("ROW_NUMBER", (agg, typeFactory) ->
          new WindowRowNumberProcessor(agg.getOutputName()))
      .build();
  private final List<OperatorFactory> ops;

  @Nonnull
  public static Windowing fromCalciteStuff(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexBuilder rexBuilder
  )
  {
    final Window window = Preconditions.checkNotNull(partialQuery.getWindow(), "window");

    ArrayList<OperatorFactory> ops = new ArrayList<>();

    final List<String> expectedOutputColumns = new ArrayList<>(rowSignature.getColumnNames());
    final String outputNamePrefix = Calcites.findUnusedPrefixForDigits("w", rowSignature.getColumnNames());
    int outputNameCounter = 0;
    for (int i = 0; i < window.groups.size(); ++i) {
      final WindowGroup group = new WindowGroup(window, window.groups.get(i), rowSignature);

      if (i > 0) {
        LinkedHashSet<ColumnWithDirection> sortColumns = new LinkedHashSet<>();
        for (String partitionColumn : group.getPartitionColumns()) {
          sortColumns.add(ColumnWithDirection.ascending(partitionColumn));
        }
        sortColumns.addAll(group.getOrdering());

        ops.add(new NaiveSortOperatorFactory(new ArrayList<>(sortColumns)));
      }

      // Presently, the order by keys are not validated to ensure that the incoming query has pre-sorted the data
      // as required by the window query.  This should be done.  In order to do it, we will need to know what the
      // sub-query that we are running against actually looks like in order to then validate that the data will
      // come back in the order expected.  Unfortunately, the way that the queries are re-written to DruidRels
      // loses all the context of sub-queries, making it not possible to validate this without changing how the
      // various Druid rules work (i.e. a very large blast radius change).  For now, it is easy enough to validate
      // this when we build the native query, so we validate it there.

      // Aggregations.
      final List<AggregateCall> aggregateCalls = group.getAggregateCalls();

      final List<Processor> processors = new ArrayList<>();
      final List<AggregatorFactory> aggregations = new ArrayList<>();

      for (AggregateCall aggregateCall : aggregateCalls) {
        final String aggName = outputNamePrefix + outputNameCounter++;
        expectedOutputColumns.add(aggName);

        ProcessorMaker maker = KNOWN_WINDOW_FNS.get(aggregateCall.getAggregation().getName());
        if (maker == null) {
          final Aggregation aggregation = GroupByRules.translateAggregateCall(
              plannerContext,
              rowSignature,
              null,
              rexBuilder,
              partialQuery.getSelectProject(),
              Collections.emptyList(),
              aggName,
              aggregateCall,
              false // Windowed aggregations don't currently finalize.  This means that sketches won't work as expected.
          );

          if (aggregation == null
              || aggregation.getPostAggregator() != null
              || aggregation.getAggregatorFactories().size() != 1) {
            if (null == plannerContext.getPlanningError()) {
              plannerContext.setPlanningError("Aggregation [%s] is not supported", aggregateCall);
            }
            throw new CannotBuildQueryException(window, aggregateCall);
          }

          aggregations.add(Iterables.getOnlyElement(aggregation.getAggregatorFactories()));
        } else {
          processors.add(maker.make(
              new WindowAggregate(
                  aggName,
                  aggregateCall,
                  rowSignature,
                  plannerContext,
                  partialQuery.getSelectProject(),
                  window.constants,
                  group
              ),
              rexBuilder.getTypeFactory()
          ));
        }
      }

      if (!aggregations.isEmpty()) {
        processors.add(
            new WindowFramedAggregateProcessor(
                group.getWindowFrame(),
                aggregations.toArray(new AggregatorFactory[0])
            )
        );
      }

      if (processors.isEmpty()) {
        throw new ISE("No processors from Window[%s], why was this code called?", window);
      }

      // The ordering required for partitioning is actually not important for the semantics.  However, it *is*
      // important that it be consistent across the query.  Because if the incoming data is sorted descending
      // and we try to partition on an ascending sort, we will think the data is not sorted correctly
      ops.add(new NaivePartitioningOperatorFactory(group.getPartitionColumns()));
      ops.add(new WindowOperatorFactory(
          processors.size() == 1 ?
          processors.get(0) : new ComposingProcessor(processors.toArray(new Processor[0]))
      ));
    }

    return new Windowing(
        RowSignatures.fromRelDataType(expectedOutputColumns, window.getRowType()),
        ops
    );
  }

  private final RowSignature signature;

  public Windowing(
      final RowSignature signature,
      List<OperatorFactory> ops
  )
  {
    this.signature = signature;
    this.ops = ops;
  }

  public RowSignature getSignature()
  {
    return signature;
  }

  public List<OperatorFactory> getOperators()
  {
    return ops;
  }

  private interface ProcessorMaker
  {
    Processor make(WindowAggregate agg, RelDataTypeFactory typeFactory);
  }

  private static class WindowGroup
  {
    private final Window window;
    private final RowSignature sig;
    private final Window.Group group;

    public WindowGroup(Window window, Window.Group group, RowSignature sig)
    {
      this.window = window;
      this.sig = sig;
      this.group = group;
    }

    public ArrayList<String> getPartitionColumns()
    {
      final ArrayList<String> retVal = new ArrayList<>();
      for (int groupKey : group.keys) {
        retVal.add(sig.getColumnName(groupKey));
      }
      return retVal;
    }

    public ArrayList<ColumnWithDirection> getOrdering()
    {
      final List<RelFieldCollation> fields = group.orderKeys.getFieldCollations();
      ArrayList<ColumnWithDirection> retVal = new ArrayList<>(fields.size());
      for (RelFieldCollation field : fields) {
        final ColumnWithDirection.Direction direction;
        switch (field.direction) {
          case ASCENDING:
            direction = ColumnWithDirection.Direction.ASC;
            break;
          case DESCENDING:
            direction = ColumnWithDirection.Direction.DESC;
            break;
          default:
            throw new QueryException(
                QueryException.SQL_QUERY_UNSUPPORTED_ERROR_CODE,
                StringUtils.format("Cannot handle ordering with direction[%s]", field.direction),
                null,
                null
            );
        }
        retVal.add(new ColumnWithDirection(sig.getColumnName(field.getFieldIndex()), direction));
      }
      return retVal;
    }

    public ArrayList<String> getOrderingColumNames()
    {
      final ArrayList<ColumnWithDirection> ordering = getOrdering();
      final ArrayList<String> retVal = new ArrayList<>(ordering.size());
      for (ColumnWithDirection column : ordering) {
        retVal.add(column.getColumn());
      }
      return retVal;
    }

    public List<AggregateCall> getAggregateCalls()
    {
      return group.getAggregateCalls(window);
    }

    public WindowFrame getWindowFrame()
    {
      return new WindowFrame(
          WindowFrame.PeerType.ROWS,
          group.lowerBound.isUnbounded(),
          figureOutOffset(group.lowerBound),
          group.upperBound.isUnbounded(),
          figureOutOffset(group.upperBound)
      );
    }

    private int figureOutOffset(RexWindowBound bound)
    {
      if (bound.isUnbounded() || bound.isCurrentRow()) {
        return 0;
      }
      return getConstant(((RexInputRef) bound.getOffset()).getIndex());
    }

    private int getConstant(int refIndex)
    {
      return ((Number) window.constants.get(refIndex - sig.size()).getValue()).intValue();
    }
  }

  private static class WindowAggregate
  {
    private final String outputName;
    private final AggregateCall call;
    private final RowSignature sig;
    private final PlannerContext context;
    private final Project project;
    private final List<RexLiteral> constants;
    private final WindowGroup group;

    private WindowAggregate(
        String outputName,
        AggregateCall call,
        RowSignature sig,
        PlannerContext context,
        Project project,
        List<RexLiteral> constants,
        WindowGroup group
    )
    {
      this.outputName = outputName;
      this.call = call;
      this.sig = sig;
      this.context = context;
      this.project = project;
      this.constants = constants;
      this.group = group;

      if (project != null) {
        throw new ISE("Suddenly, the project[%s] is no longer null, the code might need to change.", project);
      }
    }

    public String getOutputName()
    {
      return outputName;
    }

    public WindowGroup getGroup()
    {
      return group;
    }

    public String getColumn(final RelDataTypeFactory typeFactory, int argPosition)
    {
      final RexNode columnArgument =
          Expressions.fromFieldAccess(typeFactory, sig, project, call.getArgList().get(argPosition));
      final DruidExpression expression = Expressions.toDruidExpression(context, sig, columnArgument);
      if (expression == null) {
        throw new ISE("Couldn't get an expression from columnArgument[%s]", columnArgument);
      }
      return expression.getDirectColumn();
    }

    public RexLiteral getConstantArgument(int argPosition)
    {
      return constants.get(call.getArgList().get(argPosition) - sig.size());
    }

    public int getConstantInt(int argPosition)
    {
      return ((Number) getConstantArgument(argPosition).getValue()).intValue();
    }
  }
}

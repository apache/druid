package org.apache.druid.sql.calcite.rule;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidCorrelateUnnestRel;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidUnnestDatasourceRel;
import org.apache.druid.sql.calcite.table.RowSignatures;

public class DruidCorrelateUnnestRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public DruidCorrelateUnnestRule(final PlannerContext plannerContext)
  {
    super(
        operand(
            LogicalCorrelate.class,
            operand(DruidRel.class, any()),
            operand(DruidUnnestDatasourceRel.class, any())
        )
    );

    this.plannerContext = plannerContext;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    LogicalCorrelate logicalCorrelate = call.rel(0);
    DruidQueryRel druidQueryRel = call.rel(1);
    DruidUnnestDatasourceRel unnestDatasourceRel = call.rel(2);

    final RexBuilder rexBuilder = logicalCorrelate.getCluster().getRexBuilder();

    final LogicalProject queryProject = LogicalProject.create(
        unnestDatasourceRel,
        ImmutableList.of(rexBuilder.makeInputRef(unnestDatasourceRel.getRowType().getFieldList().get(0).getType(), 0)),
        unnestDatasourceRel.getRowType()
    );

    RowSignature rowSignature = RowSignatures.fromRelDataType(
        logicalCorrelate.getRowType().getFieldNames(),
        logicalCorrelate.getRowType()
    );

    RowSignature rowSignature1 = RowSignatures.fromRelDataType(
        unnestDatasourceRel.getUnnestProject().getRowType().getFieldNames(),
        unnestDatasourceRel.getUnnestProject().getRowType()
    );

    RowSignature rowSignature2 = RowSignatures.fromRelDataType(
        druidQueryRel.getRowType().getFieldNames(),
        druidQueryRel.getRowType()
    );

    final DruidExpression expression = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        unnestDatasourceRel.getUnnestProject().getProjects().get(0)
    );


    DruidCorrelateUnnestRel druidCorrelateUnnestRel = new DruidCorrelateUnnestRel(
        logicalCorrelate,
        druidQueryRel,
        unnestDatasourceRel,
        plannerContext
    );


    // todo: make new projects with druidQueryRel projects + unnestRel projects shifted

    DruidCorrelateUnnestRel newRel = new DruidCorrelateUnnestRel(
        logicalCorrelate,
        druidQueryRel,
        unnestDatasourceRel,
        plannerContext
    );

    // build this based on the druidUnnestRel
    final RelBuilder relBuilder =
        call.builder()
            .push(newRel);

    call.transformTo(relBuilder.build());
  }
}

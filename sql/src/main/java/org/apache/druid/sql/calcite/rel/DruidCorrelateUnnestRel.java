package org.apache.druid.sql.calcite.rel;

import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.Set;

public class DruidCorrelateUnnestRel extends DruidRel<DruidCorrelateUnnestRel>
{
  private final DruidQueryRel baseQueryRel;
  private final LogicalCorrelate logicalCorrelate;
  private final DruidUnnestDatasourceRel unnestDatasourceRel;
  private final PartialDruidQuery partialQuery;
  private static final TableDataSource DUMMY_DATA_SOURCE = new TableDataSource("__unnest__");

  private DruidCorrelateUnnestRel(
      LogicalCorrelate logicalCorrelate,
      DruidQueryRel druidQueryRel,
      DruidUnnestDatasourceRel unnestDatasourceRel,
      PartialDruidQuery partialDruidQuery,
      PlannerContext plannerContext
  )
  {
    super(logicalCorrelate.getCluster(), logicalCorrelate.getTraitSet(), plannerContext);
    this.baseQueryRel = druidQueryRel;
    this.logicalCorrelate = logicalCorrelate;
    this.unnestDatasourceRel = unnestDatasourceRel;
    this.partialQuery = partialDruidQuery;
  }

  public static DruidCorrelateUnnestRel create(
      LogicalCorrelate logicalCorrelate,
      DruidQueryRel druidQueryRel,
      DruidUnnestDatasourceRel unnestDatasourceRel,
      PlannerContext plannerContext
  ){
    return new DruidCorrelateUnnestRel(
        logicalCorrelate,
        druidQueryRel,
        unnestDatasourceRel,
        PartialDruidQuery.create(logicalCorrelate),
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
        logicalCorrelate,
        baseQueryRel,
        unnestDatasourceRel,
        newQueryBuilder,
        getPlannerContext()
    );
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
        baseQueryRel.getDruidTable().getRowSignature(),
        getPlannerContext().getExprMacroTable(),
        getPlannerContext().getPlannerConfig().isForceExpressionVirtualColumns()
    );
    getPlannerContext().setJoinExpressionVirtualColumnRegistry(virtualColumnRegistry);

    final RowSignature rowSignature = RowSignatures.fromRelDataType(
        logicalCorrelate.getRowType().getFieldNames(),
        logicalCorrelate.getRowType()
    );
    final DruidExpression expression = Expressions.toDruidExpression(
        getPlannerContext(),
        rowSignature,
        unnestDatasourceRel.getUnnestProject().getProjects().get(0)
    );

    String dimension = "";
    if (expression.getArguments().get(0).isDirectColumnAccess()) {
      dimension = expression.getArguments().get(0).getDirectColumn();
    }

    UnnestDataSource dataSource = UnnestDataSource.create(
        baseQueryRel.getDruidTable().getDataSource(),
        dimension,
        unnestDatasourceRel.getUnnestProject().getRowType().getFieldNames().get(0),
        null
    );

    DruidQuery query = baseQueryRel.getPartialDruidQuery().build(
        dataSource,
        rowSignature,
        getPlannerContext(),
        getCluster().getRexBuilder(),
        finalizeAggregations,
        virtualColumnRegistry
    );
    getPlannerContext().setJoinExpressionVirtualColumnRegistry(null);
    return query;
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    /*return partialQuery.build(
        DUMMY_DATA_SOURCE,
        RowSignatures.fromRelDataType(
            logicalCorrelate.getRowType().getFieldNames(),
            logicalCorrelate.getRowType()
        ),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        false
    );*/
    return toDruidQuery(false);
  }

  @Override
  public DruidCorrelateUnnestRel asDruidConvention()
  {
    return new DruidCorrelateUnnestRel(logicalCorrelate, baseQueryRel.asDruidConvention(), unnestDatasourceRel, partialQuery, getPlannerContext());
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    return baseQueryRel.getDataSourceNames();
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.getRowType();
  }
}

package org.apache.druid.sql.calcite.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;


public class DruidJoinUnnestRel extends DruidRel<DruidJoinUnnestRel>
{
  private static final TableDataSource DUMMY_DATA_SOURCE = new TableDataSource("__correlate_unnest__");

  public DruidUnnestRel getUnnestRel()
  {
    return unnestRel;
  }

  public DruidRel<?> getRightRel()
  {
    return rightRel;
  }

  public Join getJoin()
  {
    return join;
  }

  private final DruidUnnestRel unnestRel;
  private final DruidRel<?> rightRel;

  private PartialDruidQuery partialDruidQuery;

  private final Join join;

  protected DruidJoinUnnestRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      PlannerContext plannerContext,
      Join joinRel,
      DruidRel<?> left,
      DruidRel<?> right,
      PartialDruidQuery pq
  )
  {
    super(cluster, traitSet, plannerContext);
    this.unnestRel = (DruidUnnestRel) left;
    this.rightRel = right;
    this.join = joinRel;
    this.partialDruidQuery = pq;
  }

  public static DruidJoinUnnestRel create(Join join, DruidRel<?> left, DruidRel<?> right, PlannerContext context){
    return new DruidJoinUnnestRel(join.getCluster(), join.getTraitSet(), context, join, left, right, PartialDruidQuery.create(join));
  }

  @Nullable
  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialDruidQuery;
  }

  @Override
  public DruidJoinUnnestRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    return new DruidJoinUnnestRel(
      getCluster(),
      newQueryBuilder.getTraitSet(getConvention()),
      getPlannerContext(),
      join,
      unnestRel,
      rightRel,
      newQueryBuilder
    );
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    throw new CannotBuildQueryException("Cannot execute UNNEST directly");
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return partialDruidQuery.build(
        DUMMY_DATA_SOURCE,
        RowSignatures.fromRelDataType(
            join.getRowType().getFieldNames(),
            join.getRowType()
        ),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        false
    );
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialDruidQuery.getRowType();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return pw.item("join", join).item("left", unnestRel).item("right", rightRel);
  }

  @Override
  public DruidJoinUnnestRel asDruidConvention()
  {
    return new DruidJoinUnnestRel(getCluster(), getTraitSet(), getPlannerContext(), join, unnestRel, rightRel, partialDruidQuery);
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    return Collections.emptySet();
  }
}

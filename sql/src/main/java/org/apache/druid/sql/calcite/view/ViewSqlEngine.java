package org.apache.druid.sql.calcite.view;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;

/**
 * Engine used for getting the row type of views. Does not do any actual execution.
 */
public class ViewSqlEngine implements SqlEngine
{
  public static final ViewSqlEngine INSTANCE = new ViewSqlEngine();

  private ViewSqlEngine()
  {
    // Singleton.
  }

  @Override
  public boolean feature(EngineFeature feature, PlannerContext plannerContext)
  {
    switch (feature) {
      // Use most permissive set of features, since our goal is to get the row type of the view.
      // Later on, the query involving the view will be executed with an actual engine with a different set of
      // features, and planning may fail then. But we don't want it to fail now.
      case ALLOW_BINDABLE_PLAN:
      case CAN_READ_EXTERNAL_DATA:
      case SCAN_CAN_ORDER_BY_NON_TIME:
        return true;

      // Views can't sit on top of INSERTs.
      case CAN_INSERT:
        return false;

      // Simplify planning by sticking to basic query types.
      case CAN_RUN_TOPN:
      case CAN_RUN_TIMESERIES:
      case CAN_RUN_TIME_BOUNDARY:
      case SCAN_NEEDS_SIGNATURE:
        return false;

      default:
        throw new IAE("Unrecognized feature: %s", feature);
    }
  }

  @Override
  public boolean isSystemContextParameter(String contextParameterName)
  {
    return false;
  }

  @Override
  public RelDataType resultTypeForSelect(RelDataTypeFactory typeFactory, RelDataType validatedRowType)
  {
    return validatedRowType;
  }

  @Override
  public RelDataType resultTypeForInsert(RelDataTypeFactory typeFactory, RelDataType validatedRowType)
  {
    // Can't have views of INSERT or REPLACE statements.
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryMaker buildQueryMakerForSelect(RelRoot relRoot, PlannerContext plannerContext)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryMaker buildQueryMakerForInsert(String targetDataSource, RelRoot relRoot, PlannerContext plannerContext)
  {
    // Can't have views of INSERT or REPLACE statements.
    throw new UnsupportedOperationException();
  }
}

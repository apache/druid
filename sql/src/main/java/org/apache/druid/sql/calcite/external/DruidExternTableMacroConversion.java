package org.apache.druid.sql.calcite.external;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.druid.catalog.model.table.TableFunction;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;

public class DruidExternTableMacroConversion implements SqlOperatorConversion
{
  private final SqlUserDefinedTableMacro operator;

  public DruidExternTableMacroConversion(
      final String name,
      final TableFunction fn,
      final ObjectMapper jsonMapper
  )
  {
    this.operator = new DruidExternTableMacro(
        new DruidTableMacro(name, fn, jsonMapper)
    );
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return operator;
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    return null;
  }
}

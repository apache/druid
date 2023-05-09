package org.apache.druid.sql.calcite.expression.builtin;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.OperatorConversions;

public class ArrayToMultiValueStringOperatorConversion extends DirectOperatorConversion
{
  public static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("ARRAY_TO_MV")
      .operandTypeChecker(
          OperandTypes.or(
              OperandTypes.family(SqlTypeFamily.STRING),
              OperandTypes.family(SqlTypeFamily.ARRAY)
          )
      )
      .functionCategory(SqlFunctionCategory.STRING)
      .returnTypeNullable(SqlTypeName.VARCHAR)
      .build();

  public ArrayToMultiValueStringOperatorConversion()
  {
    super(SQL_FUNCTION, "array_to_mv");
  }


}

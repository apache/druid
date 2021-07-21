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

package org.apache.druid.sql.calcite.expression.builtin;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.sql.calcite.expression.AliasedOperatorConversion;
import org.apache.druid.sql.calcite.expression.OperatorConversions;

/**
 * Array functions which return an array, but are used in a multi-valued string dimension context instead will output
 * {@link SqlTypeName#VARCHAR} instead of {@link SqlTypeName#ARRAY}. On the backend, these functions are identical,
 * so these classes only override the signature information.
 */
public class MultiValueStringOperatorConversions
{
  public static class Append extends ArrayAppendOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_APPEND")
        .operandTypeChecker(
            OperandTypes.sequence(
                "(array,expr)",
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                ),
                OperandTypes.family(SqlTypeFamily.STRING)
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeNullable(SqlTypeName.VARCHAR)
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }
  }

  public static class Prepend extends ArrayPrependOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_PREPEND")
        .operandTypeChecker(
            OperandTypes.sequence(
                "(expr,array)",
                OperandTypes.family(SqlTypeFamily.STRING),
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                )
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeNullable(SqlTypeName.VARCHAR)
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }
  }

  public static class Concat extends ArrayConcatOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_CONCAT")
        .operandTypeChecker(
            OperandTypes.sequence(
                "(array,array)",
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                ),
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                )
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeNullable(SqlTypeName.VARCHAR)
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }
  }

  public static class Contains extends ArrayContainsOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_CONTAINS")
        .operandTypeChecker(
            OperandTypes.sequence(
                "(array,array)",
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                ),
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING),
                    OperandTypes.family(SqlTypeFamily.NUMERIC)
                )
            )
        )
        .returnTypeInference(ReturnTypes.BOOLEAN)
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }
  }

  public static class Offset extends ArrayOffsetOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_OFFSET")
        .operandTypeChecker(
            OperandTypes.sequence(
                "(array,expr)",
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                ),
                OperandTypes.family(SqlTypeFamily.NUMERIC)
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeNullable(SqlTypeName.VARCHAR)
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }
  }

  public static class Ordinal extends ArrayOrdinalOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_ORDINAL")
        .operandTypeChecker(
            OperandTypes.sequence(
                "(array,expr)",
                OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.ARRAY),
                    OperandTypes.family(SqlTypeFamily.STRING)
                ),
                OperandTypes.family(SqlTypeFamily.NUMERIC)
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeNullable(SqlTypeName.VARCHAR)
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }
  }

  public static class Slice extends ArraySliceOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("MV_SLICE")
        .operandTypeChecker(
            OperandTypes.or(
                OperandTypes.sequence(
                    "(expr,start)",
                    OperandTypes.or(
                        OperandTypes.family(SqlTypeFamily.ARRAY),
                        OperandTypes.family(SqlTypeFamily.STRING)
                    ),
                    OperandTypes.family(SqlTypeFamily.NUMERIC)
                ),
                OperandTypes.sequence(
                    "(expr,start,end)",
                    OperandTypes.or(
                        OperandTypes.family(SqlTypeFamily.ARRAY),
                        OperandTypes.family(SqlTypeFamily.STRING)
                    ),
                    OperandTypes.family(SqlTypeFamily.NUMERIC),
                    OperandTypes.family(SqlTypeFamily.NUMERIC)
                )
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeNullable(SqlTypeName.VARCHAR)
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }
  }

  public static class StringToMultiString extends StringToArrayOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder("STRING_TO_MV")
        .operandTypeChecker(
            OperandTypes.sequence(
                "(string,expr)",
                OperandTypes.family(SqlTypeFamily.STRING),
                OperandTypes.family(SqlTypeFamily.STRING)
            )
        )
        .functionCategory(SqlFunctionCategory.STRING)
        .returnTypeNullable(SqlTypeName.VARCHAR)
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }
  }

  public static class MultiStringToString extends AliasedOperatorConversion
  {
    public MultiStringToString()
    {
      super(new ArrayToStringOperatorConversion(), "MV_TO_STRING");
    }
  }

  public static class Length extends AliasedOperatorConversion
  {
    public Length()
    {
      super(new ArrayLengthOperatorConversion(), "MV_LENGTH");
    }
  }

  public static class OffsetOf extends AliasedOperatorConversion
  {
    public OffsetOf()
    {
      super(new ArrayOffsetOfOperatorConversion(), "MV_OFFSET_OF");
    }
  }

  public static class OrdinalOf extends AliasedOperatorConversion
  {
    public OrdinalOf()
    {
      super(new ArrayOrdinalOfOperatorConversion(), "MV_ORDINAL_OF");
    }
  }

  public static class Overlap extends AliasedOperatorConversion
  {
    public Overlap()
    {
      super(new ArrayOverlapOperatorConversion(), "MV_OVERLAP");
    }
  }

  private MultiValueStringOperatorConversions()
  {
    // no instantiation
  }
}

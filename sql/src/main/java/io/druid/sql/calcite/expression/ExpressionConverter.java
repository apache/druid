/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.expression;

import com.google.common.collect.Maps;
import io.druid.java.util.common.ISE;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Map;

public class ExpressionConverter
{
  private final Map<SqlKind, ExpressionConversion> kindMap;
  private final Map<String, ExpressionConversion> otherFunctionMap;

  private ExpressionConverter(
      Map<SqlKind, ExpressionConversion> kindMap,
      Map<String, ExpressionConversion> otherFunctionMap
  )
  {
    this.kindMap = kindMap;
    this.otherFunctionMap = otherFunctionMap;
  }

  public static ExpressionConverter create(final List<ExpressionConversion> conversions)
  {
    final Map<SqlKind, ExpressionConversion> kindMap = Maps.newHashMap();
    final Map<String, ExpressionConversion> otherFunctionMap = Maps.newHashMap();

    for (final ExpressionConversion conversion : conversions) {
      if (conversion.sqlKind() != SqlKind.OTHER_FUNCTION) {
        if (kindMap.put(conversion.sqlKind(), conversion) != null) {
          throw new ISE("Oops, can't have two conversions for sqlKind[%s]", conversion.sqlKind());
        }
      } else {
        // kind is OTHER_FUNCTION
        if (otherFunctionMap.put(conversion.operatorName(), conversion) != null) {
          throw new ISE(
              "Oops, can't have two conversions for sqlKind[%s], operatorName[%s]",
              conversion.sqlKind(),
              conversion.operatorName()
          );
        }
      }
    }

    return new ExpressionConverter(kindMap, otherFunctionMap);
  }

  /**
   * Translate a row-expression to a Druid row extraction. Note that this signature will probably need to change
   * once we support extractions from multiple columns.
   *
   * @param rowOrder   order of fields in the Druid rows to be extracted from
   * @param expression expression meant to be applied on top of the table
   *
   * @return (columnName, extractionFn) or null
   */
  public RowExtraction convert(List<String> rowOrder, RexNode expression)
  {
    if (expression.getKind() == SqlKind.INPUT_REF) {
      final RexInputRef ref = (RexInputRef) expression;
      final String columnName = rowOrder.get(ref.getIndex());
      if (columnName == null) {
        throw new ISE("WTF?! Expression referred to nonexistent index[%d]", ref.getIndex());
      }

      return RowExtraction.of(columnName, null);
    } else if (expression.getKind() == SqlKind.CAST) {
      // TODO(gianm): Probably not a good idea to ignore CAST like this.
      return convert(rowOrder, ((RexCall) expression).getOperands().get(0));
    } else {
      // Try conversion using an ExpressionConversion specific to this operator.
      final RowExtraction retVal;

      if (expression.getKind() == SqlKind.OTHER_FUNCTION) {
        final ExpressionConversion conversion = otherFunctionMap.get(((RexCall) expression).getOperator().getName());
        retVal = conversion != null ? conversion.convert(this, rowOrder, expression) : null;
      } else {
        final ExpressionConversion conversion = kindMap.get(expression.getKind());
        retVal = conversion != null ? conversion.convert(this, rowOrder, expression) : null;
      }

      return retVal;
    }
  }
}

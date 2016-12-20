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

import io.druid.query.extraction.StrlenExtractionFn;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

public class CharLengthExpressionConversion extends AbstractExpressionConversion
{
  private static final CharLengthExpressionConversion INSTANCE = new CharLengthExpressionConversion();

  private CharLengthExpressionConversion()
  {
    super(SqlKind.OTHER_FUNCTION, "CHAR_LENGTH");
  }

  public static CharLengthExpressionConversion instance()
  {
    return INSTANCE;
  }

  @Override
  public RowExtraction convert(
      final ExpressionConverter converter,
      final List<String> rowOrder,
      final RexNode expression
  )
  {
    final RexCall call = (RexCall) expression;
    final RowExtraction arg = converter.convert(rowOrder, call.getOperands().get(0));
    if (arg == null) {
      return null;
    }

    return RowExtraction.of(
        arg.getColumn(),
        ExtractionFns.compose(StrlenExtractionFn.instance(), arg.getExtractionFn())
    );
  }
}

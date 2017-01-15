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

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

public interface ExpressionConversion
{
  /**
   * SQL kind that this converter knows how to convert.
   *
   * @return sql kind
   */
  SqlKind sqlKind();

  /**
   * Operator name, if {@link #sqlKind()} is {@code OTHER_FUNCTION}.
   *
   * @return operator name, or null
   */
  String operatorName();

  /**
   * Translate a row-expression to a Druid column reference. Note that this signature will probably need to change
   * once we support extractions from multiple columns.
   *
   * @param converter  converter that can be used to convert sub-expressions
   * @param rowOrder   order of fields in the Druid rows to be extracted from
   * @param expression expression meant to be applied on top of the table
   *
   * @return (columnName, extractionFn) or null
   */
  RowExtraction convert(ExpressionConverter converter, List<String> rowOrder, RexNode expression);
}

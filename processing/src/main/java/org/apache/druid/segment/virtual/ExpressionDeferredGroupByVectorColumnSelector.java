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

package org.apache.druid.segment.virtual;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryPointer;
import org.apache.druid.query.groupby.epinephelinae.vector.GroupByVectorColumnSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.column.RowSignature;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link GroupByVectorColumnSelector} that uses a wide key representing all expression inputs
 * to enable deferring expression evaluation to {@link #writeKeyToResultRow(MemoryPointer, int, ResultRow, int)}.
 *
 * For example, the expression "coalesce(x, y)" would write a key composed of (x, y) in {@link #writeKeys}, then
 * compute "coalesce(x, y)" in {@link #writeKeyToResultRow}.
 */
public class ExpressionDeferredGroupByVectorColumnSelector implements GroupByVectorColumnSelector
{
  private final Expr expr;
  private final List<GroupByVectorColumnSelector> subSelectors;
  private final int exprKeyBytes;

  /**
   * Used internally by {@link #writeKeyToResultRow(MemoryPointer, int, ResultRow, int)} to populate inputs
   * for the expression.
   */
  private final ResultRow tmpResultRow;

  /**
   * Used internally by {@link #writeKeyToResultRow(MemoryPointer, int, ResultRow, int)} to evaluate the expression
   * on {@link #tmpResultRow}.
   */
  private final Expr.ObjectBinding tmpResultRowBindings;

  ExpressionDeferredGroupByVectorColumnSelector(
      final Expr expr,
      final RowSignature exprInputSignature,
      final List<GroupByVectorColumnSelector> subSelectors
  )
  {
    this.expr = expr;
    this.subSelectors = subSelectors;
    this.tmpResultRow = ResultRow.create(subSelectors.size());

    int exprKeyBytesTmp = 0;
    final Map<String, InputBindings.InputSupplier<?>> tmpResultRowSuppliers = new HashMap<>();
    for (int i = 0; i < exprInputSignature.size(); i++) {
      final int columnPosition = i;
      exprKeyBytesTmp += subSelectors.get(i).getGroupingKeySize();
      tmpResultRowSuppliers.put(
          exprInputSignature.getColumnName(i),
          InputBindings.inputSupplier(
              ExpressionType.fromColumnType(exprInputSignature.getColumnType(columnPosition).orElse(null)),
              () -> tmpResultRow.getArray()[columnPosition]
          )
      );
    }
    this.exprKeyBytes = exprKeyBytesTmp;
    this.tmpResultRowBindings = InputBindings.forInputSuppliers(tmpResultRowSuppliers);
  }

  @Override
  public int getGroupingKeySize()
  {
    return exprKeyBytes;
  }

  @Override
  public int getValueCardinality()
  {
    if (subSelectors.size() == 1) {
      return subSelectors.get(0).getValueCardinality();
    }
    return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
  }

  @Override
  public int writeKeys(WritableMemory keySpace, int keySize, int keyOffset, int startRow, int endRow)
  {
    int retVal = 0;
    for (final GroupByVectorColumnSelector subSelector : subSelectors) {
      retVal += subSelector.writeKeys(keySpace, keySize, keyOffset, startRow, endRow);
      keyOffset += subSelector.getGroupingKeySize();
    }
    return retVal;
  }

  @Override
  public void writeKeyToResultRow(MemoryPointer keyMemory, int keyOffset, ResultRow resultRow, int resultRowPosition)
  {
    for (int i = 0; i < subSelectors.size(); i++) {
      final GroupByVectorColumnSelector subSelector = subSelectors.get(i);
      subSelector.writeKeyToResultRow(keyMemory, keyOffset, tmpResultRow, i);
      keyOffset += subSelector.getGroupingKeySize();
    }

    resultRow.getArray()[resultRowPosition] = expr.eval(tmpResultRowBindings).valueOrDefault();
  }

  @Override
  public void reset()
  {
    for (final GroupByVectorColumnSelector subSelector : subSelectors) {
      subSelector.reset();
    }
  }
}

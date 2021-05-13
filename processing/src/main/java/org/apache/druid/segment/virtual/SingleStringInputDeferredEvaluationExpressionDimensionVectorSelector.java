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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;

/**
 * A {@link SingleValueDimensionVectorSelector} decorator that directly exposes the underlying dictionary ids in
 * {@link #getRowVector}, saving expression computation until {@link #lookupName} is called. This allows for
 * performing operations like grouping on the native dictionary ids, and deferring expression evaluation until
 * after, which can dramatically reduce the total number of evaluations.
 *
 * @see ExpressionVectorSelectors for details on how expression vector selectors are constructed.
 *
 * @see SingleStringInputDeferredEvaluationExpressionDimensionSelector for the non-vectorized version of this selector.
 */
public class SingleStringInputDeferredEvaluationExpressionDimensionVectorSelector
    implements SingleValueDimensionVectorSelector
{
  private final SingleValueDimensionVectorSelector selector;
  private final Expr expression;
  private final SingleInputBindings bindings = new SingleInputBindings();

  public SingleStringInputDeferredEvaluationExpressionDimensionVectorSelector(
      SingleValueDimensionVectorSelector selector,
      Expr expression
  )
  {
    // Verify selector has a working dictionary.
    if (selector.getValueCardinality() == DimensionDictionarySelector.CARDINALITY_UNKNOWN
        || !selector.nameLookupPossibleInAdvance()) {
      throw new ISE("Selector of class[%s] does not have a dictionary, cannot use it.", selector.getClass().getName());
    }
    this.selector = selector;
    this.expression = expression;
  }

  @Override
  public int getValueCardinality()
  {
    return CARDINALITY_UNKNOWN;
  }

  @Nullable
  @Override
  public String lookupName(int id)
  {
    final String value;

    value = selector.lookupName(id);

    bindings.set(value);
    return expression.eval(bindings).asString();
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return selector.nameLookupPossibleInAdvance();
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return null;
  }

  @Override
  public int[] getRowVector()
  {
    return selector.getRowVector();
  }

  @Override
  public int getMaxVectorSize()
  {
    return selector.getMaxVectorSize();
  }

  @Override
  public int getCurrentVectorSize()
  {
    return selector.getCurrentVectorSize();
  }
}

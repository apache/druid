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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.SingleIndexedInt;
import org.apache.druid.segment.data.ZeroIndexedInts;

import javax.annotation.Nullable;

/**
 * A DimensionSelector decorator that computes an expression on top of it.
 */
public class SingleStringInputDimensionSelector implements DimensionSelector
{
  private final DimensionSelector selector;
  private final Expr expression;
  private final SingleInputBindings bindings = new SingleInputBindings();
  private final SingleIndexedInt nullAdjustedRow = new SingleIndexedInt();

  /**
   * 0 if selector has null as a value; 1 if it doesn't.
   */
  private final int nullAdjustment;

  public SingleStringInputDimensionSelector(
      final DimensionSelector selector,
      final Expr expression
  )
  {
    // Verify expression has just one binding.
    if (Parser.findRequiredBindings(expression).size() != 1) {
      throw new ISE("WTF?! Expected expression with just one binding");
    }

    // Verify selector has a working dictionary.
    if (selector.getValueCardinality() == DimensionSelector.CARDINALITY_UNKNOWN
        || !selector.nameLookupPossibleInAdvance()) {
      throw new ISE("Selector of class[%s] does not have a dictionary, cannot use it.", selector.getClass().getName());
    }

    this.selector = Preconditions.checkNotNull(selector, "selector");
    this.expression = Preconditions.checkNotNull(expression, "expression");
    this.nullAdjustment = selector.getValueCardinality() == 0 || selector.lookupName(0) != null ? 1 : 0;
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
    inspector.visit("expression", expression);
  }

  /**
   * Treats any non-single-valued row as a row containing a single null value, to ensure consistency with
   * other expression selectors. See also {@link ExpressionSelectors#supplierFromDimensionSelector} for similar
   * behavior.
   */
  @Override
  public IndexedInts getRow()
  {
    final IndexedInts row = selector.getRow();

    if (row.size() == 1) {
      if (nullAdjustment == 0) {
        return row;
      } else {
        nullAdjustedRow.setValue(row.get(0) + nullAdjustment);
        return nullAdjustedRow;
      }
    } else {
      // Can't handle non-singly-valued rows in expressions.
      // Treat them as nulls until we think of something better to do.
      return ZeroIndexedInts.instance();
    }
  }

  @Override
  public ValueMatcher makeValueMatcher(@Nullable final String value)
  {
    return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
  }

  @Override
  public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
  {
    return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
  }

  @Override
  public int getValueCardinality()
  {
    return selector.getValueCardinality() + nullAdjustment;
  }

  @Override
  public String lookupName(final int id)
  {
    final String value;

    if (id == 0) {
      // id 0 is always null for this selector impl.
      value = null;
    } else {
      value = selector.lookupName(id - nullAdjustment);
    }

    bindings.set(value);
    return expression.eval(bindings).asString();
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return true;
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return null;
  }

  @Nullable
  @Override
  public Object getObject()
  {
    return defaultGetObject();
  }

  @Override
  public Class classOfObject()
  {
    return Object.class;
  }
}

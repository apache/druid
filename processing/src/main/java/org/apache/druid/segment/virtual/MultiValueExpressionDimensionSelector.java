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

import com.google.common.base.Predicate;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.RangeIndexedInts;
import org.apache.druid.segment.data.ZeroIndexedInts;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Basic multi-value dimension selector for an {@link org.apache.druid.math.expr.Expr} evaluating
 * {@link ColumnValueSelector}.
 */
public class MultiValueExpressionDimensionSelector implements DimensionSelector
{
  private final ColumnValueSelector<ExprEval> baseSelector;

  public MultiValueExpressionDimensionSelector(ColumnValueSelector<ExprEval> baseSelector)
  {
    this.baseSelector = baseSelector;
  }

  ExprEval getEvaluated()
  {
    return baseSelector.getObject();
  }

  @Nullable
  String getValue(ExprEval evaluated)
  {
    assert !evaluated.isArray();
    return NullHandling.emptyToNullIfNeeded(evaluated.asString());
  }

  List<String> getArray(ExprEval evaluated)
  {
    assert evaluated.isArray();
    //noinspection ConstantConditions
    return Arrays.stream(evaluated.asStringArray())
                 .map(NullHandling::emptyToNullIfNeeded)
                 .collect(Collectors.toList());
  }

  @Nullable
  String getArrayValue(ExprEval evaluated, int i)
  {
    assert evaluated.isArray();
    String[] stringArray = evaluated.asStringArray();
    //noinspection ConstantConditions because of assert statement above
    assert i < stringArray.length;
    return NullHandling.emptyToNullIfNeeded(stringArray[i]);
  }

  @Override
  public IndexedInts getRow()
  {
    ExprEval evaluated = getEvaluated();
    if (evaluated.isArray()) {
      RangeIndexedInts ints = new RangeIndexedInts();
      Object[] evaluatedArray = evaluated.asArray();
      ints.setSize(evaluatedArray != null ? evaluatedArray.length : 0);
      return ints;
    }
    return ZeroIndexedInts.instance();
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
    ExprEval evaluated = getEvaluated();
    if (evaluated.isArray()) {
      return getArrayValue(evaluated, id);
    }
    assert id == 0;
    return NullHandling.emptyToNullIfNeeded(evaluated.asString());
  }

  @Override
  public ValueMatcher makeValueMatcher(@Nullable String value)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        ExprEval evaluated = getEvaluated();
        if (evaluated.isArray()) {
          List<String> array = getArray(evaluated);
          return array.stream().anyMatch(x -> Objects.equals(x, value));
        }
        return Objects.equals(getValue(evaluated), value);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", baseSelector);
      }
    };
  }

  @Override
  public ValueMatcher makeValueMatcher(Predicate<String> predicate)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        ExprEval evaluated = getEvaluated();
        if (evaluated.isArray()) {
          List<String> array = getArray(evaluated);
          return array.stream().anyMatch(x -> predicate.apply(x));
        }
        return predicate.apply(getValue(evaluated));
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", baseSelector);
        inspector.visit("predicate", predicate);
      }
    };
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("baseSelector", baseSelector);
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return false;
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
    ExprEval evaluated = getEvaluated();
    if (evaluated.isArray()) {
      return getArray(evaluated);
    }
    return getValue(evaluated);
  }

  @Override
  public Class<?> classOfObject()
  {
    return Object.class;
  }
}

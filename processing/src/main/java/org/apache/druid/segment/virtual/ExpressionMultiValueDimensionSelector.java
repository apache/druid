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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
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
public class ExpressionMultiValueDimensionSelector implements DimensionSelector
{
  public static ExpressionMultiValueDimensionSelector fromValueSelector(
      ColumnValueSelector<ExprEval> baseSelector,
      @Nullable ExtractionFn extractionFn
  )
  {
    if (extractionFn != null) {
      return new ExtractionMultiValueDimensionSelector(baseSelector, extractionFn);
    }
    return new ExpressionMultiValueDimensionSelector(baseSelector);
  }

  protected final ColumnValueSelector<ExprEval> baseSelector;

  public ExpressionMultiValueDimensionSelector(ColumnValueSelector<ExprEval> baseSelector)
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
    return evaluated.asString();
  }

  List<String> getArrayAsList(ExprEval evaluated)
  {
    assert evaluated.isArray();
    //noinspection ConstantConditions
    return Arrays.stream(evaluated.asArray())
                 .map(Evals::asString)
                 .collect(Collectors.toList());
  }

  @Nullable
  String getArrayValue(ExprEval evaluated, int i)
  {
    return getArrayElement(evaluated, i);
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
    return evaluated.asString();
  }

  @Override
  public ValueMatcher makeValueMatcher(@Nullable String value)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        ExprEval evaluated = getEvaluated();
        if (evaluated.isArray()) {
          List<String> array = getArrayAsList(evaluated);
          return array.stream().anyMatch(x -> (includeUnknown && x == null) || Objects.equals(x, value));
        }
        final String rowValue = getValue(evaluated);
        return (includeUnknown && rowValue == null) || Objects.equals(rowValue, value);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", baseSelector);
      }
    };
  }

  @Override
  public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        ExprEval evaluated = getEvaluated();
        final DruidObjectPredicate<String> predicate = predicateFactory.makeStringPredicate();
        if (evaluated.isArray()) {
          List<String> array = getArrayAsList(evaluated);
          return array.stream().anyMatch(x -> predicate.apply(x).matches(includeUnknown));
        }
        final String rowValue = getValue(evaluated);
        return predicate.apply(rowValue).matches(includeUnknown);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", baseSelector);
        inspector.visit("predicate", predicateFactory);
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
      return getArrayAsList(evaluated);
    }
    return getValue(evaluated);
  }

  @Override
  public Class<?> classOfObject()
  {
    return Object.class;
  }

  /**
   * expressions + extractions
   */
  static class ExtractionMultiValueDimensionSelector extends ExpressionMultiValueDimensionSelector
  {
    private final ExtractionFn extractionFn;

    private ExtractionMultiValueDimensionSelector(ColumnValueSelector<ExprEval> baseSelector, ExtractionFn extractionFn)
    {
      super(baseSelector);
      this.extractionFn = extractionFn;
    }

    @Override
    String getValue(ExprEval evaluated)
    {
      assert !evaluated.isArray();
      return extractionFn.apply(NullHandling.emptyToNullIfNeeded(evaluated.asString()));
    }

    @Override
    List<String> getArrayAsList(ExprEval evaluated)
    {
      assert evaluated.isArray();
      return Arrays.stream(evaluated.asArray())
                   .map(x -> extractionFn.apply(Evals.asString(x)))
                   .collect(Collectors.toList());
    }

    @Override
    String getArrayValue(ExprEval evaluated, int i)
    {
      return extractionFn.apply(ExpressionMultiValueDimensionSelector.getArrayElement(evaluated, i));
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("baseSelector", baseSelector);
      inspector.visit("extractionFn", extractionFn);
    }
  }

  @Nullable
  private static String getArrayElement(ExprEval eval, int i)
  {
    final Object[] stringArray = eval.asArray();
    if (stringArray == null) {
      return null;
    }
    return Evals.asString(stringArray[i]);
  }
}

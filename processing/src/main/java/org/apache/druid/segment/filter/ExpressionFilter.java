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

package org.apache.druid.segment.filter;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.expression.ExprUtils;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.FalseVectorMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.virtual.ExpressionSelectors;
import org.apache.druid.segment.virtual.ExpressionVectorSelectors;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

public class ExpressionFilter implements Filter
{
  private final Supplier<Expr> expr;
  private final Supplier<Expr.BindingAnalysis> bindingDetails;
  private final FilterTuning filterTuning;

  public ExpressionFilter(final Supplier<Expr> expr, final FilterTuning filterTuning)
  {
    this.expr = expr;
    this.bindingDetails = Suppliers.memoize(() -> expr.get().analyzeInputs());
    this.filterTuning = filterTuning;
  }

  @Override
  public boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return expr.get().canVectorize(inspector);
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(VectorColumnSelectorFactory factory)
  {
    final Expr theExpr = expr.get();

    DruidPredicateFactory predicateFactory = new DruidPredicateFactory()
    {
      @Override
      public Predicate<String> makeStringPredicate()
      {
        return Evals::asBoolean;
      }

      @Override
      public DruidLongPredicate makeLongPredicate()
      {
        return Evals::asBoolean;
      }

      @Override
      public DruidFloatPredicate makeFloatPredicate()
      {
        return Evals::asBoolean;
      }

      @Override
      public DruidDoublePredicate makeDoublePredicate()
      {
        return Evals::asBoolean;
      }

      // The hashcode and equals are to make SubclassesMustOverrideEqualsAndHashCodeTest stop complaining..
      // DruidPredicateFactory doesn't really need equals or hashcode, in fact only the 'toString' method is called
      // when testing equality of DimensionPredicateFilter, so it's the truly required method, but even that seems
      // strange at best.
      // Rather than tackle removing the annotation and reworking equality tests for now, will leave this to refactor
      // as part of https://github.com/apache/druid/issues/8256 which suggests combining Filter and DimFilter
      // implementations, which should clean up some of this mess.
      @Override
      public int hashCode()
      {
        return super.hashCode();
      }

      @Override
      public boolean equals(Object obj)
      {
        return super.equals(obj);
      }
    };


    final ExprType outputType = theExpr.getOutputType(factory);

    if (outputType == null) {
      // if an expression is vectorizable, but the output type is null, the result will be null (or the default
      // value in default mode) because expression is either all null constants or missing columns

      // in sql compatible mode, this means no matches ever, so just use the false matcher:
      if (NullHandling.sqlCompatible()) {
        return new FalseVectorMatcher(factory.getVectorSizeInspector());
      }
      // in default mode, just fallback to using a long matcher since nearly all boolean-ish expressions
      // output a long value so it is probably a safe bet? idk, ending up here by using all null-ish things
      // in default mode is dancing on the edge of insanity anyway...
      return VectorValueMatcherColumnProcessorFactory.instance().makeLongProcessor(
          ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.LONG),
          ExpressionVectorSelectors.makeVectorValueSelector(factory, theExpr)
      ).makeMatcher(predicateFactory);
    }

    switch (outputType) {
      case LONG:
        return VectorValueMatcherColumnProcessorFactory.instance().makeLongProcessor(
            ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.LONG),
            ExpressionVectorSelectors.makeVectorValueSelector(factory, theExpr)
        ).makeMatcher(predicateFactory);
      case DOUBLE:
        return VectorValueMatcherColumnProcessorFactory.instance().makeDoubleProcessor(
            ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.DOUBLE),
            ExpressionVectorSelectors.makeVectorValueSelector(factory, theExpr)
        ).makeMatcher(predicateFactory);
      case STRING:
        return VectorValueMatcherColumnProcessorFactory.instance().makeObjectProcessor(
            // using 'numeric' capabilities creator so we are configured to NOT be dictionary encoded, etc
            ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.STRING),
            ExpressionVectorSelectors.makeVectorObjectSelector(factory, theExpr)
        ).makeMatcher(predicateFactory);
      default:
        throw new UnsupportedOperationException("not implemented");
    }
  }

  @Override
  public ValueMatcher makeMatcher(final ColumnSelectorFactory factory)
  {
    final ColumnValueSelector<ExprEval> selector = ExpressionSelectors.makeExprEvalSelector(factory, expr.get());
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        final ExprEval eval = selector.getObject();

        switch (eval.type()) {
          case LONG_ARRAY:
            final Long[] lResult = eval.asLongArray();
            if (lResult == null) {
              return false;
            }

            return Arrays.stream(lResult).anyMatch(Evals::asBoolean);

          case STRING_ARRAY:
            final String[] sResult = eval.asStringArray();
            if (sResult == null) {
              return false;
            }

            return Arrays.stream(sResult).anyMatch(Evals::asBoolean);

          case DOUBLE_ARRAY:
            final Double[] dResult = eval.asDoubleArray();
            if (dResult == null) {
              return false;
            }

            return Arrays.stream(dResult).anyMatch(Evals::asBoolean);

          default:
            return eval.asBoolean();
        }
      }

      @Override
      public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }

  @Override
  public boolean supportsBitmapIndex(final BitmapIndexSelector selector)
  {
    final Expr.BindingAnalysis details = this.bindingDetails.get();


    if (details.getRequiredBindings().isEmpty()) {
      // Constant expression.
      return true;
    } else if (details.getRequiredBindings().size() == 1) {
      // Single-column expression. We can use bitmap indexes if this column has an index and the expression can
      // map over the values of the index.
      final String column = Iterables.getOnlyElement(details.getRequiredBindings());
      return selector.getBitmapIndex(column) != null
             && ExpressionSelectors.canMapOverDictionary(details, selector.hasMultipleValues(column));
    } else {
      // Multi-column expression.
      return false;
    }
  }

  @Override
  public boolean shouldUseBitmapIndex(BitmapIndexSelector selector)
  {
    return Filters.shouldUseBitmapIndex(this, selector, filterTuning);
  }

  @Override
  public <T> T getBitmapResult(final BitmapIndexSelector selector, final BitmapResultFactory<T> bitmapResultFactory)
  {
    if (bindingDetails.get().getRequiredBindings().isEmpty()) {
      // Constant expression.
      if (expr.get().eval(ExprUtils.nilBindings()).asBoolean()) {
        return bitmapResultFactory.wrapAllTrue(Filters.allTrue(selector));
      } else {
        return bitmapResultFactory.wrapAllFalse(Filters.allFalse(selector));
      }
    } else {
      // Can assume there's only one binding, it has a bitmap index, and it's a single input mapping.
      // Otherwise, supportsBitmapIndex would have returned false and the caller should not have called us.
      assert supportsBitmapIndex(selector);

      final String column = Iterables.getOnlyElement(bindingDetails.get().getRequiredBindings());
      return Filters.matchPredicate(
          column,
          selector,
          bitmapResultFactory,
          value -> expr.get().eval(identifierName -> {
            // There's only one binding, and it must be the single column, so it can safely be ignored in production.
            assert column.equals(identifierName);
            // convert null to Empty before passing to expressions if needed.
            return NullHandling.nullToEmptyIfNeeded(value);
          }).asBoolean()
      );
    }
  }

  @Override
  public boolean supportsSelectivityEstimation(
      final ColumnSelector columnSelector,
      final BitmapIndexSelector indexSelector
  )
  {
    return false;
  }

  @Override
  public double estimateSelectivity(final BitmapIndexSelector indexSelector)
  {
    // Selectivity estimation not supported.
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return bindingDetails.get().getRequiredBindings();
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    // We could support this, but need a good approach to rewriting the identifiers within an expression.
    return false;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExpressionFilter that = (ExpressionFilter) o;
    return Objects.equals(expr, that.expr) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(expr, filterTuning);
  }

  @Override
  public String toString()
  {
    return "ExpressionFilter{" +
           "expr=" + expr +
           ", filterTuning=" + filterTuning +
           '}';
  }
}

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
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.BooleanVectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.BitmapColumnIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.virtual.ExpressionSelectors;
import org.apache.druid.segment.virtual.ExpressionVectorSelectors;

import javax.annotation.Nullable;
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

    DruidPredicateFactory predicateFactory = getPredicateFactory();
    final ExpressionType outputType = theExpr.getOutputType(factory);

    // for all vectorizable expressions, outputType will only ever be null in cases where there is absolutely no
    // input type information, so composed entirely of null constants or missing columns. the expression is
    // effectively constant
    if (outputType == null) {

      // in sql compatible mode, this means no matches ever because null doesn't equal anything so just use the
      // false matcher
      if (NullHandling.sqlCompatible()) {
        return BooleanVectorValueMatcher.of(factory.getReadableVectorInspector(), false);
      }
      // however in default mode, we still need to evaluate the expression since it might end up... strange, from
      // default values. Since it is effectively constant though, we can just do that up front and decide if it matches
      // or not.
      return BooleanVectorValueMatcher.of(
          factory.getReadableVectorInspector(),
          theExpr.eval(InputBindings.nilBindings()).asBoolean()
      );
    }

    // if we got here, we really have to evaluate the expressions to match
    switch (outputType.getType()) {
      case LONG:
        return VectorValueMatcherColumnProcessorFactory.instance().makeLongProcessor(
            ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG),
            ExpressionVectorSelectors.makeVectorValueSelector(factory, theExpr)
        ).makeMatcher(predicateFactory);
      case DOUBLE:
        return VectorValueMatcherColumnProcessorFactory.instance().makeDoubleProcessor(
            ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE),
            ExpressionVectorSelectors.makeVectorValueSelector(factory, theExpr)
        ).makeMatcher(predicateFactory);
      case STRING:
        return VectorValueMatcherColumnProcessorFactory.instance().makeObjectProcessor(
            ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities(),
            ExpressionVectorSelectors.makeVectorObjectSelector(factory, theExpr)
        ).makeMatcher(predicateFactory);
      default:
        throw new UOE("Vectorized expression matchers not implemented for type: [%s]", outputType);
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

        if (eval.type().isArray()) {
          switch (eval.elementType().getType()) {
            case LONG:
              final Object[] lResult = eval.asArray();
              if (lResult == null) {
                return false;
              }

              return Arrays.stream(lResult).filter(Objects::nonNull).anyMatch(o -> Evals.asBoolean((long) o));
            case STRING:
              final Object[] sResult = eval.asArray();
              if (sResult == null) {
                return false;
              }

              return Arrays.stream(sResult).anyMatch(o -> Evals.asBoolean((String) o));
            case DOUBLE:
              final Object[] dResult = eval.asArray();
              if (dResult == null) {
                return false;
              }

              return Arrays.stream(dResult).filter(Objects::nonNull).anyMatch(o -> Evals.asBoolean((double) o));
          }
        }
        return eval.asBoolean();
      }

      @Override
      public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }

  @Nullable
  @Override
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    final Expr.BindingAnalysis details = bindingDetails.get();
    if (details.getRequiredBindings().isEmpty()) {
      // Constant expression.
      return Filters.makeNullIndex(
          expr.get().eval(InputBindings.nilBindings()).asBoolean(),
          selector
      );
    } else if (details.getRequiredBindings().size() == 1) {
      // Single-column expression. We can use bitmap indexes if this column has an index and the expression can
      // map over the values of the index.
      final String column = Iterables.getOnlyElement(details.getRequiredBindings());

      // we use a default 'all false' capabilities here because if the column has a bitmap index, but the capabilities
      // are null, it means that the column is missing and should take the single valued path, while truly unknown
      // things will not have a bitmap index available
      final ColumnCapabilities capabilities = selector.getColumnCapabilitiesWithDefault(
          column,
          ColumnCapabilitiesImpl.createDefault()
      );
      if (ExpressionSelectors.canMapOverDictionary(details, capabilities)) {
        if (!Filters.checkFilterTuningUseIndex(column, selector, filterTuning)) {
          return null;
        }
        return Filters.makePredicateIndex(
            column,
            selector,
            getBitmapPredicateFactory()
        );
      }
    }
    return null;
  }

  @Override
  public boolean supportsSelectivityEstimation(
      final ColumnSelector columnSelector,
      final ColumnIndexSelector indexSelector
  )
  {
    return false;
  }

  @Override
  public double estimateSelectivity(final ColumnIndexSelector indexSelector)
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



  /**
   * {@link DruidPredicateFactory} which pipes already evaluated expression values through to {@link Evals#asBoolean},
   * used for a value matcher on top of expression selectors
   */
  private DruidPredicateFactory getPredicateFactory()
  {
    return new DruidPredicateFactory()
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
      // DruidPredicateFactory currently doesn't really need equals or hashcode since 'toString' method that is actually
      // called when testing equality of DimensionPredicateFilter, so it's the truly required method, but that seems
      // a bit strange. DimensionPredicateFilter should probably be reworked to use equals from DruidPredicateFactory
      // instead of using toString.
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
  }

  /**
   * {@link DruidPredicateFactory} which evaluates the expression using the value as input, used for building predicate
   * indexes where the raw column values will be checked against this predicate
   */
  private DruidPredicateFactory getBitmapPredicateFactory()
  {
    return new DruidPredicateFactory()
    {
      @Override
      public Predicate<String> makeStringPredicate()
      {
        return value -> expr.get().eval(
            InputBindings.forFunction(identifierName -> NullHandling.nullToEmptyIfNeeded(value))
        ).asBoolean();
      }

      @Override
      public DruidLongPredicate makeLongPredicate()
      {
        return new DruidLongPredicate()
        {
          @Override
          public boolean applyLong(long input)
          {
            return expr.get().eval(InputBindings.forFunction(identifierName -> input)).asBoolean();
          }

          @Override
          public boolean applyNull()
          {
            return expr.get().eval(InputBindings.nilBindings()).asBoolean();
          }
        };
      }

      @Override
      public DruidFloatPredicate makeFloatPredicate()
      {
        return new DruidFloatPredicate()
        {
          @Override
          public boolean applyFloat(float input)
          {
            return expr.get().eval(InputBindings.forFunction(identifierName -> input)).asBoolean();
          }

          @Override
          public boolean applyNull()
          {
            return expr.get().eval(InputBindings.nilBindings()).asBoolean();
          }
        };
      }

      @Override
      public DruidDoublePredicate makeDoublePredicate()
      {
        return new DruidDoublePredicate()
        {
          @Override
          public boolean applyDouble(double input)
          {
            return expr.get().eval(InputBindings.forFunction(identifierName -> input)).asBoolean();
          }

          @Override
          public boolean applyNull()
          {
            return expr.get().eval(InputBindings.nilBindings()).asBoolean();
          }
        };
      }

      // The hashcode and equals are to make SubclassesMustOverrideEqualsAndHashCodeTest stop complaining..
      // DruidPredicateFactory currently doesn't really need equals or hashcode since 'toString' method that is actually
      // called when testing equality of DimensionPredicateFilter, so it's the truly required method, but that seems
      // a bit strange. DimensionPredicateFilter should probably be reworked to use equals from DruidPredicateFactory
      // instead of using toString.
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
  }
}

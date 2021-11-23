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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.math.expr.SettableObjectBinding;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionPlan;
import org.apache.druid.segment.virtual.ExpressionPlanner;
import org.apache.druid.segment.virtual.ExpressionSelectors;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class ExpressionLambdaAggregatorFactory extends AggregatorFactory
{
  private static final String FINALIZE_IDENTIFIER = "o";
  private static final String COMPARE_O1 = "o1";
  private static final String COMPARE_O2 = "o2";
  private static final String DEFAULT_ACCUMULATOR_ID = "__acc";

  // minimum permitted agg size is 10 bytes so it is at least large enough to hold primitive numerics (long, double)
  // | expression type byte | is_null byte | primitive value (8 bytes) |
  private static final int MIN_SIZE_BYTES = 10;
  public static final HumanReadableBytes DEFAULT_MAX_SIZE_BYTES = new HumanReadableBytes(1L << 10);

  private final String name;
  @Nullable
  private final Set<String> fields;
  private final String accumulatorId;
  private final String foldExpressionString;
  private final String initialValueExpressionString;
  private final String initialCombineValueExpressionString;
  private final boolean isNullUnlessAggregated;

  private final String combineExpressionString;
  @Nullable
  private final String compareExpressionString;
  @Nullable
  private final String finalizeExpressionString;

  private final ExprMacroTable macroTable;
  private final Supplier<ExprEval<?>> initialValue;
  private final Supplier<ExprEval<?>> initialCombineValue;
  private final Supplier<Expr> foldExpression;
  private final Supplier<Expr> combineExpression;
  private final Supplier<Expr> compareExpression;
  private final Supplier<Expr> finalizeExpression;
  private final HumanReadableBytes maxSizeBytes;

  private final Supplier<SettableObjectBinding> compareBindings;
  private final Supplier<SettableObjectBinding> combineBindings;
  private final Supplier<SettableObjectBinding> finalizeBindings;
  private final Supplier<Expr.InputBindingInspector> finalizeInspector;

  @JsonCreator
  public ExpressionLambdaAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fields") @Nullable final Set<String> fields,
      @JsonProperty("accumulatorIdentifier") @Nullable final String accumulatorIdentifier,
      @JsonProperty("initialValue") final String initialValue,
      @JsonProperty("initialCombineValue") @Nullable final String initialCombineValue,
      @JsonProperty("isNullUnlessAggregated") @Nullable final Boolean isNullUnlessAggregated,
      @JsonProperty("fold") final String foldExpression,
      @JsonProperty("combine") @Nullable final String combineExpression,
      @JsonProperty("compare") @Nullable final String compareExpression,
      @JsonProperty("finalize") @Nullable final String finalizeExpression,
      @JsonProperty("maxSizeBytes") @Nullable final HumanReadableBytes maxSizeBytes,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");

    this.name = name;
    this.fields = fields;
    this.accumulatorId = accumulatorIdentifier != null ? accumulatorIdentifier : DEFAULT_ACCUMULATOR_ID;

    this.initialValueExpressionString = initialValue;
    this.initialCombineValueExpressionString = initialCombineValue == null ? initialValue : initialCombineValue;
    this.isNullUnlessAggregated = isNullUnlessAggregated == null ? NullHandling.sqlCompatible() : isNullUnlessAggregated;
    this.foldExpressionString = foldExpression;
    if (combineExpression != null) {
      this.combineExpressionString = combineExpression;
    } else {
      // if the combine expression is null, allow single input aggregator expressions to be rewritten to replace the
      // field with the aggregator name. Fields is null for the combining/merging aggregator, but the expression should
      // already be set with the rewritten value at that point
      Preconditions.checkArgument(
          fields != null && fields.size() == 1,
          "Must have a single input field if no combine expression is supplied"
      );
      this.combineExpressionString = StringUtils.replace(foldExpression, Iterables.getOnlyElement(fields), name);
    }
    this.compareExpressionString = compareExpression;
    this.finalizeExpressionString = finalizeExpression;
    this.macroTable = macroTable;

    this.initialValue = Suppliers.memoize(() -> {
      Expr parsed = Parser.parse(initialValue, macroTable);
      Preconditions.checkArgument(parsed.isLiteral(), "initial value must be constant");
      return parsed.eval(InputBindings.nilBindings());
    });
    this.initialCombineValue = Suppliers.memoize(() -> {
      Expr parsed = Parser.parse(this.initialCombineValueExpressionString, macroTable);
      Preconditions.checkArgument(parsed.isLiteral(), "initial combining value must be constant");
      return parsed.eval(InputBindings.nilBindings());
    });
    this.foldExpression = Parser.lazyParse(foldExpressionString, macroTable);
    this.combineExpression = Parser.lazyParse(combineExpressionString, macroTable);
    this.compareExpression = Parser.lazyParse(compareExpressionString, macroTable);
    this.finalizeInspector = Suppliers.memoize(
        () -> InputBindings.inspectorFromTypeMap(
            ImmutableMap.of(FINALIZE_IDENTIFIER, this.initialCombineValue.get().type())
        )
    );
    this.compareBindings = Suppliers.memoize(
        () -> new SettableObjectBinding(2).withInspector(
            InputBindings.inspectorFromTypeMap(
                ImmutableMap.of(
                    COMPARE_O1, this.initialCombineValue.get().type(),
                    COMPARE_O2, this.initialCombineValue.get().type()
                )
            )
        )
    );
    this.combineBindings = Suppliers.memoize(
        () -> new SettableObjectBinding(2).withInspector(
            InputBindings.inspectorFromTypeMap(
                ImmutableMap.of(
                    accumulatorId, this.initialCombineValue.get().type(),
                    name, this.initialCombineValue.get().type()
                )
            )
        )
    );
    this.finalizeBindings = Suppliers.memoize(
        () -> new SettableObjectBinding(1).withInspector(finalizeInspector.get())
    );
    this.finalizeExpression = Parser.lazyParse(finalizeExpressionString, macroTable);
    this.maxSizeBytes = maxSizeBytes != null ? maxSizeBytes : DEFAULT_MAX_SIZE_BYTES;
    Preconditions.checkArgument(this.maxSizeBytes.getBytesInInt() >= MIN_SIZE_BYTES);

  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @JsonProperty
  @Nullable
  public Set<String> getFields()
  {
    return fields;
  }

  @JsonProperty
  @Nullable
  public String getAccumulatorIdentifier()
  {
    return accumulatorId;
  }

  @JsonProperty("initialValue")
  public String getInitialValueExpressionString()
  {
    return initialValueExpressionString;
  }

  @JsonProperty("initialCombineValue")
  public String getInitialCombineValueExpressionString()
  {
    return initialCombineValueExpressionString;
  }

  @JsonProperty("isNullUnlessAggregated")
  public boolean getIsNullUnlessAggregated()
  {
    return isNullUnlessAggregated;
  }

  @JsonProperty("fold")
  public String getFoldExpressionString()
  {
    return foldExpressionString;
  }

  @JsonProperty("combine")
  public String getCombineExpressionString()
  {
    return combineExpressionString;
  }

  @JsonProperty("compare")
  @Nullable
  public String getCompareExpressionString()
  {
    return compareExpressionString;
  }

  @JsonProperty("finalize")
  @Nullable
  public String getFinalizeExpressionString()
  {
    return finalizeExpressionString;
  }

  @JsonProperty("maxSizeBytes")
  public HumanReadableBytes getMaxSizeBytes()
  {
    return maxSizeBytes;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.EXPRESSION_LAMBDA_CACHE_TYPE_ID)
        .appendStrings(fields)
        .appendString(initialValueExpressionString)
        .appendString(initialCombineValueExpressionString)
        .appendCacheable(foldExpression.get())
        .appendCacheable(combineExpression.get())
        .appendCacheable(combineExpression.get())
        .appendCacheable(finalizeExpression.get())
        .appendInt(maxSizeBytes.getBytesInInt())
        .build();
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    FactorizePlan thePlan = new FactorizePlan(metricFactory);
    return new ExpressionLambdaAggregator(
        thePlan.getExpression(),
        thePlan.getBindings(),
        isNullUnlessAggregated,
        maxSizeBytes.getBytesInInt()
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    FactorizePlan thePlan = new FactorizePlan(metricFactory);
    return new ExpressionLambdaBufferAggregator(
        thePlan.getExpression(),
        thePlan.getInitialValue(),
        thePlan.getBindings(),
        isNullUnlessAggregated,
        maxSizeBytes.getBytesInInt()
    );
  }

  @Override
  public Comparator getComparator()
  {
    Expr compareExpr = compareExpression.get();
    if (compareExpr != null) {
      return (o1, o2) ->
          compareExpr.eval(compareBindings.get().withBinding(COMPARE_O1, o1).withBinding(COMPARE_O2, o2)).asInt();
    }
    return initialCombineValue.get().type().getStrategy();
  }

  @Nullable
  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    // arbitrarily assign lhs and rhs to accumulator and aggregator name inputs to re-use combine function
    return combineExpression.get().eval(
        combineBindings.get().withBinding(accumulatorId, lhs).withBinding(name, rhs)
    ).value();
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    Expr finalizeExpr;
    finalizeExpr = finalizeExpression.get();
    if (finalizeExpr != null) {
      return finalizeExpr.eval(finalizeBindings.get().withBinding(FINALIZE_IDENTIFIER, object)).value();
    }
    return object;
  }

  @Override
  public List<String> requiredFields()
  {
    if (fields == null) {
      return combineExpression.get().analyzeInputs().getRequiredBindingsList();
    }
    return foldExpression.get().analyzeInputs().getRequiredBindingsList();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new ExpressionLambdaAggregatorFactory(
        name,
        null,
        accumulatorId,
        initialValueExpressionString,
        initialCombineValueExpressionString,
        isNullUnlessAggregated,
        foldExpressionString,
        combineExpressionString,
        compareExpressionString,
        finalizeExpressionString,
        maxSizeBytes,
        macroTable
    );
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(
        new ExpressionLambdaAggregatorFactory(
            name,
            fields,
            accumulatorId,
            initialValueExpressionString,
            initialCombineValueExpressionString,
            isNullUnlessAggregated,
            foldExpressionString,
            combineExpressionString,
            compareExpressionString,
            finalizeExpressionString,
            maxSizeBytes,
            macroTable
        )
    );
  }

  @Override
  public ColumnType getIntermediateType()
  {
    if (fields == null) {
      return ExpressionType.toColumnType(initialCombineValue.get().type());
    }
    return ExpressionType.toColumnType(initialValue.get().type());
  }

  @Override
  public ColumnType getResultType()
  {
    Expr finalizeExpr = finalizeExpression.get();
    ExprEval<?> initialVal = initialCombineValue.get();
    if (finalizeExpr != null) {
      ExpressionType type = finalizeExpr.getOutputType(finalizeInspector.get());
      if (type == null) {
        type = initialVal.type();
      }
      return ExpressionType.toColumnType(type);
    }
    return ExpressionType.toColumnType(initialVal.type());
  }

  @Override
  public int getMaxIntermediateSize()
  {
    // numeric expressions are either longs or doubles, with strings or arrays max size is unknown
    // for numeric arguments, the first 2 bytes are used for expression type byte and is_null byte
    return getIntermediateType().isNumeric() ? 2 + Long.BYTES : maxSizeBytes.getBytesInInt();
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
    ExpressionLambdaAggregatorFactory that = (ExpressionLambdaAggregatorFactory) o;
    return maxSizeBytes.equals(that.maxSizeBytes)
           && name.equals(that.name)
           && Objects.equals(fields, that.fields)
           && accumulatorId.equals(that.accumulatorId)
           && foldExpressionString.equals(that.foldExpressionString)
           && initialValueExpressionString.equals(that.initialValueExpressionString)
           && initialCombineValueExpressionString.equals(that.initialCombineValueExpressionString)
           && isNullUnlessAggregated == that.isNullUnlessAggregated
           && combineExpressionString.equals(that.combineExpressionString)
           && Objects.equals(compareExpressionString, that.compareExpressionString)
           && Objects.equals(finalizeExpressionString, that.finalizeExpressionString);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        name,
        fields,
        accumulatorId,
        foldExpressionString,
        initialValueExpressionString,
        initialCombineValueExpressionString,
        isNullUnlessAggregated,
        combineExpressionString,
        compareExpressionString,
        finalizeExpressionString,
        maxSizeBytes
    );
  }

  @Override
  public String toString()
  {
    return "ExpressionLambdaAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fields=" + fields +
           ", accumulatorId='" + accumulatorId + '\'' +
           ", foldExpressionString='" + foldExpressionString + '\'' +
           ", initialValueExpressionString='" + initialValueExpressionString + '\'' +
           ", initialCombineValueExpressionString='" + initialCombineValueExpressionString + '\'' +
           ", nullUnlessAggregated='" + isNullUnlessAggregated + '\'' +
           ", combineExpressionString='" + combineExpressionString + '\'' +
           ", compareExpressionString='" + compareExpressionString + '\'' +
           ", finalizeExpressionString='" + finalizeExpressionString + '\'' +
           ", maxSizeBytes=" + maxSizeBytes +
           '}';
  }

  /**
   * Determine how to factorize the aggregator
   */
  private class FactorizePlan
  {
    private final ExpressionPlan plan;

    private final ExprEval<?> seed;
    private final ExpressionLambdaAggregatorInputBindings bindings;

    FactorizePlan(ColumnSelectorFactory metricFactory)
    {
      final List<String> columns;

      if (fields != null) {
        // if fields are set, we are accumulating from raw inputs, use fold expression
        plan = ExpressionPlanner.plan(inspectorWithAccumulator(metricFactory), foldExpression.get());
        seed = initialValue.get();
        columns = plan.getAnalysis().getRequiredBindingsList();
      } else {
        // else we are merging intermediary results, use combine expression
        plan = ExpressionPlanner.plan(inspectorWithAccumulator(metricFactory), combineExpression.get());
        seed = initialCombineValue.get();
        columns = plan.getAnalysis().getRequiredBindingsList();
      }

      bindings = new ExpressionLambdaAggregatorInputBindings(
          ExpressionSelectors.createBindings(metricFactory, columns),
          accumulatorId,
          seed
      );
    }

    public Expr getExpression()
    {
      if (fields == null) {
        return plan.getExpression();
      }
      // for fold expressions, check to see if it needs transformation due to scalar use of multi-valued or unknown
      // inputs
      return plan.getAppliedFoldExpression(accumulatorId);
    }

    public ExprEval<?> getInitialValue()
    {
      return seed;
    }

    public ExpressionLambdaAggregatorInputBindings getBindings()
    {
      return bindings;
    }

    private ColumnInspector inspectorWithAccumulator(ColumnInspector inspector)
    {
      return new ColumnInspector()
      {
        @Nullable
        @Override
        public ColumnCapabilities getColumnCapabilities(String column)
        {
          if (accumulatorId.equals(column)) {
            return ColumnCapabilitiesImpl.createDefault().setType(ExpressionType.toColumnType(initialValue.get().type()));
          }
          return inspector.getColumnCapabilities(column);
        }

        @Nullable
        @Override
        public ExpressionType getType(String name)
        {
          if (accumulatorId.equals(name)) {
            return initialValue.get().type();
          }
          return inspector.getType(name);
        }
      };
    }
  }
}

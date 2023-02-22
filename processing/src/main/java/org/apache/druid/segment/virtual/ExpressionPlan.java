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
import com.google.common.collect.Iterables;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public class ExpressionPlan
{
  public enum Trait
  {
    /**
     * expression has no inputs and can be optimized into a constant selector
      */
    CONSTANT,
    /**
     * expression is a simple identifier expression, do not transform
     */
    IDENTIFIER,
    /**
     * expression has a single, single valued input, and is dictionary encoded if the value is a string, and does
     * not produce non-scalar output
     */
    SINGLE_INPUT_SCALAR,
    /**
     * expression has a single input, which may produce single or multi-valued output, but if so, it must be implicitly
     * mappable  (i.e. the expression is not treating its input as an array and does not produce non-scalar output)
     */
    SINGLE_INPUT_MAPPABLE,
    /**
     * expression must be implicitly mapped across the multiple values per row of known multi-value inputs, the final
     * output will be multi-valued
     */
    NEEDS_APPLIED,
    /**
     * expression has inputs whose type was unresolveable
     */
    UNKNOWN_INPUTS,
    /**
     * expression has inputs whose type was incomplete, such as unknown multi-valuedness, which are not explicitly
     * used as possibly multi-valued/array inputs
     */
    INCOMPLETE_INPUTS,
    /**
     * expression explicitly using multi-valued inputs as array inputs or has array inputs
     */
    NON_SCALAR_INPUTS,
    /**
     * expression produces explict multi-valued output
     */
    NON_SCALAR_OUTPUT,
    /**
     * expression is vectorizable
     */
    VECTORIZABLE
  }

  private final ColumnInspector baseInputInspector;
  private final Expr expression;
  private final Expr.BindingAnalysis analysis;
  private final EnumSet<Trait> traits;

  @Nullable
  private final ExpressionType outputType;
  @Nullable
  private final ColumnType singleInputType;
  private final Set<String> unknownInputs;
  private final List<String> unappliedInputs;

  ExpressionPlan(
      ColumnInspector baseInputInspector,
      Expr expression,
      Expr.BindingAnalysis analysis,
      EnumSet<Trait> traits,
      @Nullable ExpressionType outputType,
      @Nullable ColumnType singleInputType,
      Set<String> unknownInputs,
      List<String> unappliedInputs
  )
  {
    this.baseInputInspector = baseInputInspector;
    this.expression = expression;
    this.analysis = analysis;
    this.traits = traits;
    this.outputType = outputType;
    this.singleInputType = singleInputType;
    this.unknownInputs = unknownInputs;
    this.unappliedInputs = unappliedInputs;
  }

  /**
   * An expression with no inputs is a constant
   */
  public boolean isConstant()
  {
    return analysis.getRequiredBindings().isEmpty();
  }

  /**
   * Gets the original expression that was planned
   */
  public Expr getExpression()
  {
    return expression;
  }

  /**
   * If an expression uses a multi-valued input in a scalar manner, the expression can be automatically transformed
   * to map these values across the expression, applying the original expression to every value.
   *
   * @see Parser#applyUnappliedBindings(Expr, Expr.BindingAnalysis, List)
   */
  public Expr getAppliedExpression()
  {
    if (is(Trait.NEEDS_APPLIED)) {
      return Parser.applyUnappliedBindings(expression, analysis, unappliedInputs);
    }
    return expression;
  }

  /**
   * If an expression uses a multi-valued input in a scalar manner, and the expression contains an accumulator such as
   * for use as part of an aggregator, the expression can be automatically transformed to fold the accumulator across
   * the values of the original expression.
   *
   * @see Parser#foldUnappliedBindings(Expr, Expr.BindingAnalysis, List, String)
   */
  public Expr getAppliedFoldExpression(String accumulatorId)
  {
    if (is(Trait.NEEDS_APPLIED)) {
      Preconditions.checkState(
          !unappliedInputs.contains(accumulatorId),
          "Accumulator cannot be implicitly transformed, if it is an ARRAY or multi-valued type it must"
          + " be used explicitly as such"
      );
      return Parser.foldUnappliedBindings(expression, analysis, unappliedInputs, accumulatorId);
    }
    return expression;
  }

  /**
   * The output type of the original expression.
   *
   * Note that this might not be the true for the expressions provided by {@link #getAppliedExpression()}
   * or {@link #getAppliedFoldExpression(String)}, should the expression have any unapplied inputs
   */
  @Nullable
  public ExpressionType getOutputType()
  {
    return outputType;
  }

  /**
   * If and only if the column has a single input, get the {@link ValueType} of that input
   */
  @Nullable
  public ColumnType getSingleInputType()
  {
    return singleInputType;
  }

  /**
   * If and only if the expression has a single input, get the name of that input
   */
  public String getSingleInputName()
  {
    return Iterables.getOnlyElement(analysis.getRequiredBindings());
  }

  /**
   * Get set of inputs which were completely missing information, possibly a non-existent column or from a column
   * selector factory with incomplete information
   */
  public Set<String> getUnknownInputs()
  {
    return unknownInputs;
  }

  /**
   * Returns basic analysis of the inputs to an {@link Expr} and how they are used
   *
   * @see Expr.BindingAnalysis
   */
  public Expr.BindingAnalysis getAnalysis()
  {
    return analysis;
  }

  /**
   * Tries to construct the most appropriate {@link ColumnCapabilities} for this plan given the {@link #outputType} and
   * {@link #traits} inferred by the {@link ExpressionPlanner}, optionally with the help of hint {@link ValueType}.
   *
   * If no output type was able to be inferred during planning, returns null
   */
  @Nullable
  public ColumnCapabilities inferColumnCapabilities(@Nullable ColumnType outputTypeHint)
  {
    if (outputType != null) {
      final ColumnType inferredValueType = ExpressionType.toColumnType(outputType);

      if (inferredValueType.is(ValueType.COMPLEX)) {
        return ColumnCapabilitiesImpl.createDefault().setHasNulls(true).setType(inferredValueType);
      }

      if (inferredValueType.isNumeric()) {
        // if float was explicitly specified preserve it, because it will currently never be the computed output type
        // since there is no float expression type
        if (Types.is(outputTypeHint, ValueType.FLOAT)) {
          return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.FLOAT);
        }
        return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(inferredValueType);
      }

      // null constants can sometimes trip up the type inference to report STRING, so check if explicitly supplied
      // output type is numeric and stick with that if so
      if (outputTypeHint != null && outputTypeHint.isNumeric()) {
        return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(outputTypeHint);
      }

      // fancy string stuffs
      if (inferredValueType.is(ValueType.STRING)) {
        // constant strings are supported as dimension selectors, set them as dictionary encoded and unique for all the
        // bells and whistles the engines have to offer
        if (isConstant()) {
          return ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities()
                                       .setDictionaryEncoded(true)
                                       .setDictionaryValuesUnique(true)
                                       .setDictionaryValuesSorted(true)
                                       .setHasNulls(expression.isNullLiteral());
        }

        // single input strings also have an optimization which allow defering evaluation time until dictionary encoded
        // column lookup, so if the underlying column is a dictionary encoded string then we can report as such
        if (any(Trait.SINGLE_INPUT_SCALAR, Trait.SINGLE_INPUT_MAPPABLE)) {
          ColumnCapabilities underlyingCapabilities = baseInputInspector.getColumnCapabilities(getSingleInputName());
          if (underlyingCapabilities != null) {
            // since we don't know if the expression is 1:1 or if it retains ordering we can only piggy back only
            // report as dictionary encoded, but it still allows us to use algorithms which work with dictionaryIds
            // to create a dictionary encoded selector instead of an object selector to defer expression evaluation
            // until query time
            return ColumnCapabilitiesImpl.copyOf(underlyingCapabilities)
                                         .setType(ColumnType.STRING)
                                         .setDictionaryValuesSorted(false)
                                         .setDictionaryValuesUnique(false)
                                         .setHasBitmapIndexes(false)
                                         .setHasNulls(true);
          }
        }
      }

      // we don't have to check for unknown input here because output type is unable to be inferred if we don't know
      // the complete set of input types
      if (any(Trait.NON_SCALAR_OUTPUT, Trait.NEEDS_APPLIED)) {
        // if the hint requested a string, use a string
        if (Types.is(outputTypeHint, ValueType.STRING) || inferredValueType.is(ValueType.STRING)) {
          return ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities().setHasMultipleValues(true);
        }
        // maybe something is looking for a little fun and wants arrays? let whatever it is through
        return ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ExpressionType.toColumnType(outputType));
      }

      // if we got here, lets call it single value string output, non-dictionary encoded
      return ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities();
    }
    // we don't know what we don't know
    return null;
  }

  /**
   * Returns true if all of the supplied traits are true in this plan
   */
  public boolean is(Trait... flags)
  {
    return is(traits, flags);
  }

  /**
   * Returns true if any of the supplied traits are true in this plan
   */
  public boolean any(Trait... flags)
  {
    return any(traits, flags);
  }

  /**
   * Returns true if all of the supplied traits are true in the supplied set
   */
  static boolean is(EnumSet<Trait> traits, Trait... args)
  {
    return Arrays.stream(args).allMatch(traits::contains);
  }

  /**
   * Returns true if any of the supplied traits are true in the supplied set
   */
  static boolean any(EnumSet<Trait> traits, Trait... args)
  {
    return Arrays.stream(args).anyMatch(traits::contains);
  }

  /**
   * Returns true if none of the supplied traits are true in the supplied set
   */
  static boolean none(EnumSet<Trait> traits, Trait... args)
  {
    return Arrays.stream(args).noneMatch(traits::contains);
  }
}

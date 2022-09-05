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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ExpressionPlanner
{
  private ExpressionPlanner()
  {
    // No instantiation.
  }

  /**
   * Druid tries to be chill to expressions to make up for not having a well defined table schema across segments. This
   * method performs some analysis to determine what sort of selectors can be constructed on top of an expression,
   * whether or not the expression will need implicitly mapped across multi-valued inputs, if the expression produces
   * multi-valued outputs, is vectorizable, and everything else interesting when making a selector.
   *
   * Results are stored in a {@link ExpressionPlan}, which can be examined to do whatever is necessary to make things
   * function properly.
   */
  public static ExpressionPlan plan(ColumnInspector inspector, Expr expression)
  {
    final Expr.BindingAnalysis analysis = expression.analyzeInputs();
    Parser.validateExpr(expression, analysis);

    EnumSet<ExpressionPlan.Trait> traits = EnumSet.noneOf(ExpressionPlan.Trait.class);
    Set<String> noCapabilities = new HashSet<>();
    Set<String> maybeMultiValued = new HashSet<>();
    List<String> needsApplied = ImmutableList.of();
    ColumnType singleInputType = null;
    ExpressionType outputType = null;

    final Set<String> columns = analysis.getRequiredBindings();

    // check and set traits which allow optimized selectors to be created
    if (columns.isEmpty()) {
      traits.add(ExpressionPlan.Trait.CONSTANT);
    } else if (expression.isIdentifier()) {
      traits.add(ExpressionPlan.Trait.IDENTIFIER);
    } else if (columns.size() == 1) {
      final String column = Iterables.getOnlyElement(columns);
      final ColumnCapabilities capabilities = inspector.getColumnCapabilities(column);

      // These flags allow for selectors that wrap a single underlying column to be optimized, through caching results
      // and via allowing deferred execution in the case of building dimension selectors.
      //    SINGLE_INPUT_SCALAR
      // is set if an input is single valued, and the output is definitely single valued, with an additional requirement
      // for strings that the column is dictionary encoded.
      //    SINGLE_INPUT_MAPPABLE
      // is set when a single input string column, which can be multi-valued, but if so, it must be implicitly mappable
      // (i.e. the expression is not treating its input as an array and not wanting to output an array)
      if (capabilities != null && !analysis.hasInputArrays() && !analysis.isOutputArray()) {
        boolean isSingleInputMappable = false;
        boolean isSingleInputScalar = capabilities.hasMultipleValues().isFalse();
        if (capabilities.is(ValueType.STRING)) {
          isSingleInputScalar &= capabilities.isDictionaryEncoded().isTrue();
          isSingleInputMappable = capabilities.isDictionaryEncoded().isTrue() &&
                                  !capabilities.hasMultipleValues().isUnknown();
        }

        // if satisfied, set single input output type and flags
        if (isSingleInputScalar || isSingleInputMappable) {
          singleInputType = capabilities.toColumnType();
          if (isSingleInputScalar) {
            traits.add(ExpressionPlan.Trait.SINGLE_INPUT_SCALAR);
          }
          if (isSingleInputMappable) {
            traits.add(ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE);
          }
        }
      }
    }

    // if we didn't eliminate this expression as a single input scalar or mappable expression, it might need
    // automatic transformation to map across multi-valued inputs (or row by row detection in the worst case)
    if (
        ExpressionPlan.none(
            traits,
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.CONSTANT,
            ExpressionPlan.Trait.IDENTIFIER
        )
    ) {
      final Set<String> definitelyMultiValued = new HashSet<>();
      final Set<String> definitelyArray = new HashSet<>();
      for (String column : analysis.getRequiredBindings()) {
        final ColumnCapabilities capabilities = inspector.getColumnCapabilities(column);
        if (capabilities != null) {
          if (capabilities.isArray()) {
            definitelyArray.add(column);
          } else if (capabilities.is(ValueType.STRING) && capabilities.hasMultipleValues().isTrue()) {
            definitelyMultiValued.add(column);
          } else if (capabilities.is(ValueType.STRING) &&
                     capabilities.hasMultipleValues().isMaybeTrue() &&
                     !analysis.getArrayBindings().contains(column)
          ) {
            maybeMultiValued.add(column);
          }
        } else {
          noCapabilities.add(column);
        }
      }

      // find any inputs which will need implicitly mapped across multi-valued rows
      needsApplied =
          columns.stream()
                 .filter(
                     c -> !definitelyArray.contains(c)
                          && definitelyMultiValued.contains(c)
                          && !analysis.getArrayBindings().contains(c)
                 )
                 .collect(Collectors.toList());

      // if any multi-value inputs, set flag for non-scalar inputs
      if (analysis.hasInputArrays()) {
        traits.add(ExpressionPlan.Trait.NON_SCALAR_INPUTS);
      }

      if (!noCapabilities.isEmpty()) {
        traits.add(ExpressionPlan.Trait.UNKNOWN_INPUTS);
      }

      if (!maybeMultiValued.isEmpty()) {
        traits.add(ExpressionPlan.Trait.INCOMPLETE_INPUTS);
      }

      // if expression needs transformed, lets do it
      if (!needsApplied.isEmpty()) {
        traits.add(ExpressionPlan.Trait.NEEDS_APPLIED);
      }
    }

    // only set output type if we are pretty confident about input types
    final boolean shouldComputeOutput = ExpressionPlan.none(
        traits,
        ExpressionPlan.Trait.UNKNOWN_INPUTS,
        ExpressionPlan.Trait.INCOMPLETE_INPUTS
    );

    if (shouldComputeOutput) {
      outputType = expression.getOutputType(inspector);
    }

    // if analysis predicts output, or inferred output type, is array, output will be arrays
    if (analysis.isOutputArray() || (outputType != null && outputType.isArray())) {
      traits.add(ExpressionPlan.Trait.NON_SCALAR_OUTPUT);

      // single input mappable may not produce array output explicitly, only through implicit mapping
      traits.remove(ExpressionPlan.Trait.SINGLE_INPUT_SCALAR);
      traits.remove(ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE);
    }

    // vectorized expressions do not support incomplete, multi-valued inputs or outputs, or implicit mapping
    // they also do not support unknown inputs, but they also do not currently have to deal with them, as missing
    // capabilites is indicative of a non-existent column instead of an unknown schema. If this ever changes,
    // this check should also change
    boolean supportsVector = ExpressionPlan.none(
        traits,
        ExpressionPlan.Trait.INCOMPLETE_INPUTS,
        ExpressionPlan.Trait.NEEDS_APPLIED,
        ExpressionPlan.Trait.NON_SCALAR_INPUTS,
        ExpressionPlan.Trait.NON_SCALAR_OUTPUT
    );

    if (supportsVector && expression.canVectorize(inspector)) {
      // make sure to compute the output type for a vector expression though, because we might have skipped it earlier
      // due to unknown inputs, but that's ok here since it just means it doesnt exist
      outputType = expression.getOutputType(inspector);
      traits.add(ExpressionPlan.Trait.VECTORIZABLE);
    }

    return new ExpressionPlan(
        inspector,
        expression,
        analysis,
        traits,
        outputType,
        singleInputType,
        Sets.union(noCapabilities, maybeMultiValued),
        needsApplied
    );
  }
}

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

package org.apache.druid.query.groupby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.virtual.ExpressionPlan;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;

import java.util.List;

/**
 * Controls deferral of {@link ExpressionVirtualColumn} in {@link GroupByQuery}.
 */
public enum DeferExpressionDimensions
{
  SINGLE_STRING("singleString") {
    @Override
    public boolean useDeferredGroupBySelector(
        ExpressionPlan plan,
        List<String> requiredBindingsList,
        ColumnInspector inspector
    )
    {
      return false;
    }
  },

  /**
   * Defer expressions when their input variables are all fixed-width types (primitive numbers, or dictionary encoded).
   */
  FIXED_WIDTH("fixedWidth") {
    @Override
    public boolean useDeferredGroupBySelector(
        ExpressionPlan plan,
        List<String> requiredBindingsList,
        ColumnInspector inspector
    )
    {
      if (isInnatelyDeferrable(plan, requiredBindingsList, inspector)) {
        return false;
      }

      for (final String requiredBinding : requiredBindingsList) {
        final ColumnCapabilities capabilities = inspector.getColumnCapabilities(requiredBinding);
        if (capabilities == null) {
          return false;
        }

        if (!capabilities.isNumeric() && !isDictionaryEncodedScalarString(capabilities)) {
          // Not fixed-width.
          return false;
        }
      }

      return true;
    }
  },

  /**
   * Defer expressions when their input variables are all fixed-width types (primitive numbers, or dictionary encoded).
   */
  FIXED_WIDTH_NON_NUMERIC("fixedWidthNonNumeric") {
    @Override
    public boolean useDeferredGroupBySelector(
        ExpressionPlan plan,
        List<String> requiredBindingsList,
        ColumnInspector inspector
    )
    {
      if (isInnatelyDeferrable(plan, requiredBindingsList, inspector)) {
        return false;
      }

      boolean allNumericInputs = true;

      for (final String requiredBinding : requiredBindingsList) {
        final ColumnCapabilities capabilities = inspector.getColumnCapabilities(requiredBinding);
        if (capabilities == null) {
          return false;
        }

        allNumericInputs = allNumericInputs && capabilities.isNumeric();

        if (!capabilities.isNumeric() && !isDictionaryEncodedScalarString(capabilities)) {
          // Not fixed-width.
          return false;
        }
      }

      return !allNumericInputs || (plan.getOutputType() != null && !plan.getOutputType().isNumeric());
    }
  },

  ALWAYS("always") {
    @Override
    public boolean useDeferredGroupBySelector(
        ExpressionPlan plan,
        List<String> requiredBindingsList,
        ColumnInspector inspector
    )
    {
      return !isInnatelyDeferrable(plan, requiredBindingsList, inspector);
    }
  };

  public static final String JSON_KEY = "deferExpressionDimensions";

  private final String jsonName;

  DeferExpressionDimensions(String jsonName)
  {
    this.jsonName = jsonName;
  }

  @JsonCreator
  public static DeferExpressionDimensions fromString(final String jsonName)
  {
    for (final DeferExpressionDimensions value : values()) {
      if (value.jsonName.equals(jsonName)) {
        return value;
      }
    }

    throw new IAE("Invalid value[%s] for[%s]", jsonName, JSON_KEY);
  }

  public abstract boolean useDeferredGroupBySelector(
      ExpressionPlan plan,
      List<String> requiredBindingsList,
      ColumnInspector inspector
  );

  @Override
  @JsonValue
  public String toString()
  {
    return jsonName;
  }


  /**
   * {@link VectorColumnSelectorFactory} currently can only make dictionary encoded selectors for string types, so
   * we can only consider them as fixed width. Additionally, to err on the side of safety, multi-value string columns
   * are also not considered fixed width because expressions process multi-value dimensions as single rows, so we would
   * need all dictionary ids to be present in the combined key.
   *
   * At the time of this javadoc, vector group by does not support multi-value dimensions anyway, so this isn't really
   * a problem, but if it did, we could consider allowing them if we ensure that all multi-value inputs are used as
   * scalars and so the expression can be applied separately to each individual dictionary id (e.g. the equivalent of
   * {@link ExpressionPlan.Trait#SINGLE_INPUT_MAPPABLE} but for all multi-value string inputs of the expression).
   */
  private static boolean isDictionaryEncodedScalarString(ColumnCapabilities capabilities)
  {
    return capabilities.isDictionaryEncoded().isTrue() &&
           capabilities.is(ValueType.STRING) &&
           capabilities.hasMultipleValues().isFalse();
  }

  /**
   * Whether the given expression can be deferred innately by the selector created by
   * {@link ExpressionVirtualColumn#makeSingleValueVectorDimensionSelector(DimensionSpec, VectorColumnSelectorFactory)}.
   *
   * In this case, all options for this enum return false from
   * {@link #useDeferredGroupBySelector(ExpressionPlan, List, ColumnInspector)}, because there is no need to defer
   * redundantly.
   */
  private static boolean isInnatelyDeferrable(
      ExpressionPlan plan,
      List<String> requiredBindingsList,
      ColumnInspector inspector
  )
  {
    if (plan.getOutputType() != null
        && plan.getOutputType().is(ExprType.STRING)
        && requiredBindingsList.size() <= 1) {
      for (final String requiredBinding : requiredBindingsList) {
        final ColumnCapabilities requiredBindingCapabilities = inspector.getColumnCapabilities(requiredBinding);

        if (requiredBindingCapabilities == null
            || !requiredBindingCapabilities.is(ValueType.STRING)
            || !requiredBindingCapabilities.isDictionaryEncoded().isTrue()) {
          return false;
        }
      }

      return true;
    } else {
      return false;
    }
  }
}

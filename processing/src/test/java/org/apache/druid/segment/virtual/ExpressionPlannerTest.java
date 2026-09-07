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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.DeferExpressionDimensions;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

public class ExpressionPlannerTest extends InitializedNullHandlingTest
{
  private static ColumnType DICTIONARY_COMPLEX = ColumnType.ofComplex("dictionaryComplex");
  private static final ColumnInspector SYNTHETIC_INSPECTOR = new ColumnInspector()
  {
    private final Map<String, ColumnCapabilities> capabilitiesMap =
        ImmutableMap.<String, ColumnCapabilities>builder()
                    .put(
                        "long1",
                        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG)
                    )
                    .put(
                        "long2",
                        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG)
                    )
                    .put(
                        "float1",
                        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.FLOAT)
                    )
                    .put(
                        "float2",
                        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.FLOAT)
                    )
                    .put(
                        "double1",
                        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE)
                    )
                    .put(
                        "double2",
                        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE)
                    )
                    .put(
                        "scalar_string",
                        ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities()
                    )
                    .put(
                        // segment style single value dictionary encoded with unique sorted dictionary
                        "scalar_dictionary_string",
                        new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                    .setDictionaryEncoded(true)
                                                    .setHasBitmapIndexes(true)
                                                    .setDictionaryValuesSorted(true)
                                                    .setDictionaryValuesUnique(true)
                                                    .setHasMultipleValues(false)
                    )
                    .put(
                        // dictionary encoded but not unique or sorted, maybe an indexed table from a join result
                        "scalar_dictionary_string_nonunique",
                        new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                    .setDictionaryEncoded(true)
                                                    .setHasBitmapIndexes(false)
                                                    .setDictionaryValuesSorted(false)
                                                    .setDictionaryValuesUnique(false)
                                                    .setHasMultipleValues(false)
                    )
                    .put(
                        // string with unknown multi-valuedness
                        "string_unknown",
                        new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                    )
                    .put(
                        // dictionary encoded multi valued string dimension
                        "multi_dictionary_string",
                        new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                    .setDictionaryEncoded(true)
                                                    .setHasBitmapIndexes(true)
                                                    .setDictionaryValuesUnique(true)
                                                    .setDictionaryValuesSorted(true)
                                                    .setHasMultipleValues(true)
                    )
                    .put(
                        // simple multi valued string dimension unsorted
                        "multi_dictionary_string_nonunique",
                        new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                    .setDictionaryEncoded(false)
                                                    .setHasBitmapIndexes(false)
                                                    .setDictionaryValuesUnique(false)
                                                    .setDictionaryValuesSorted(false)
                                                    .setHasMultipleValues(true)
                    )
                    .put(
                        "string_array_1",
                        ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ColumnType.STRING_ARRAY)
                    )
                    .put(
                        "string_array_2",
                        ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ColumnType.STRING_ARRAY)
                    )
                    .put(
                        "long_array_1",
                        ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ColumnType.LONG_ARRAY)
                    )
                    .put(
                        "long_array_2",
                        ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ColumnType.LONG_ARRAY)
                    )
                    .put(
                        "double_array_1",
                        ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ColumnType.DOUBLE_ARRAY)
                    )
                    .put(
                        "double_array_2",
                        ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ColumnType.DOUBLE_ARRAY)
                    )
                    .put(
                        "dictionary_complex",
                        ColumnCapabilitiesImpl.createDefault()
                                              .setDictionaryEncoded(true)
                                              .setType(DICTIONARY_COMPLEX)
                    )
                    .build();

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return capabilitiesMap.get(column);
    }
  };

  private static final TestMacroTable MACRO_TABLE = new TestMacroTable();

  @Test
  public void testUnknown()
  {
    // column has no capabilities
    // the vectorize query engine contracts is such that the lack of column capabilities is indicative of a nil column
    // so this is vectorizable
    // for non-vectorized expression processing, this will probably end up using a selector that examines inputs on a
    // row by row basis to determine if the expression needs applied to multi-valued inputs

    ExpressionPlan thePlan = plan("concat(x, 'x')");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assertions.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.CONSTANT
        )
    );
    // this expression has no "unapplied bindings", nothing to apply
    Assertions.assertEquals("concat(\"x\", 'x')", thePlan.getAppliedExpression().stringify());
    Assertions.assertEquals("concat(\"x\", 'x')", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assertions.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(null);
    Assertions.assertNotNull(inferred);
    Assertions.assertEquals(ValueType.STRING, inferred.getType());
    Assertions.assertTrue(inferred.hasNulls().isTrue());
    Assertions.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assertions.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assertions.assertFalse(inferred.hasBitmapIndexes());
    Assertions.assertFalse(inferred.hasSpatialIndexes());

    // what if both inputs are unknown, can we know things?
    thePlan = plan("x * y");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.UNKNOWN_INPUTS
        )
    );
    Assertions.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.VECTORIZABLE,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.CONSTANT
        )
    );

    Assertions.assertEquals("(\"x\" * \"y\")", thePlan.getAppliedExpression().stringify());
    Assertions.assertEquals("(\"x\" * \"y\")", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assertions.assertNull(thePlan.getOutputType());
    Assertions.assertNull(thePlan.inferColumnCapabilities(null));
    // no we cannot

    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
  }

  @Test
  public void testScalarStringNondictionaryEncoded()
  {
    ExpressionPlan thePlan = plan("concat(scalar_string, 'x')");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assertions.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assertions.assertEquals("concat(\"scalar_string\", 'x')", thePlan.getAppliedExpression().stringify());
    Assertions.assertEquals("concat(\"scalar_string\", 'x')", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assertions.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(null);
    Assertions.assertNotNull(inferred);
    Assertions.assertEquals(ValueType.STRING, inferred.getType());
    Assertions.assertTrue(inferred.hasNulls().isTrue());
    Assertions.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assertions.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assertions.assertFalse(inferred.hasBitmapIndexes());
    Assertions.assertFalse(inferred.hasSpatialIndexes());

    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
  }

  @Test
  public void testScalarNumeric()
  {
    ExpressionPlan thePlan = plan("long1 + 5");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assertions.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assertions.assertEquals("(\"long1\" + 5)", thePlan.getAppliedExpression().stringify());
    Assertions.assertEquals("(\"long1\" + 5)", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assertions.assertEquals("(\"long1\" + 5)", thePlan.getAppliedFoldExpression("long1").stringify());
    Assertions.assertEquals(ExpressionType.LONG, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(null);
    Assertions.assertNotNull(inferred);
    Assertions.assertEquals(ValueType.LONG, inferred.getType());
    Assertions.assertTrue(inferred.hasNulls().isMaybeTrue());
    Assertions.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assertions.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assertions.assertFalse(inferred.hasBitmapIndexes());
    Assertions.assertFalse(inferred.hasSpatialIndexes());

    thePlan = plan("long1 + 5.0");
    Assertions.assertEquals(ExpressionType.DOUBLE, thePlan.getOutputType());

    thePlan = plan("double1 * double2");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assertions.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assertions.assertEquals("(\"double1\" * \"double2\")", thePlan.getAppliedExpression().stringify());
    Assertions.assertEquals("(\"double1\" * \"double2\")", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assertions.assertEquals("(\"double1\" * \"double2\")", thePlan.getAppliedFoldExpression("double1").stringify());
    Assertions.assertEquals(ExpressionType.DOUBLE, thePlan.getOutputType());
    inferred = thePlan.inferColumnCapabilities(null);
    Assertions.assertNotNull(inferred);
    Assertions.assertEquals(ValueType.DOUBLE, inferred.getType());
    Assertions.assertTrue(inferred.hasNulls().isMaybeTrue());
    Assertions.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assertions.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assertions.assertFalse(inferred.hasBitmapIndexes());
    Assertions.assertFalse(inferred.hasSpatialIndexes());
    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertTrue(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
  }

  @Test
  public void testScalarStringDictionaryEncoded()
  {
    ExpressionPlan thePlan = plan("concat(scalar_dictionary_string, 'x')");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assertions.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assertions.assertEquals("concat(\"scalar_dictionary_string\", 'x')", thePlan.getAppliedExpression().stringify());
    Assertions.assertEquals(
        "concat(\"scalar_dictionary_string\", 'x')",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    Assertions.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(null);
    Assertions.assertNotNull(inferred);
    Assertions.assertEquals(ValueType.STRING, inferred.getType());
    Assertions.assertTrue(inferred.hasNulls().isTrue());
    Assertions.assertTrue(inferred.isDictionaryEncoded().isTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assertions.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assertions.assertTrue(inferred.hasBitmapIndexes());
    Assertions.assertFalse(inferred.hasSpatialIndexes());

    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    // innately deferrable
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );


    // multiple input columns
    thePlan = plan("concat(scalar_dictionary_string, scalar_dictionary_string_nonunique)");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assertions.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assertions.assertEquals(
        "concat(\"scalar_dictionary_string\", \"scalar_dictionary_string_nonunique\")",
        thePlan.getAppliedExpression().stringify()
    );
    Assertions.assertEquals(
        "concat(\"scalar_dictionary_string\", \"scalar_dictionary_string_nonunique\")",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    // what if scalar_dictionary_string_nonunique is an accumulator instead? nope, still no NEEDS_APPLIED so nothing to do
    Assertions.assertEquals(
        "concat(\"scalar_dictionary_string\", \"scalar_dictionary_string_nonunique\")",
        thePlan.getAppliedFoldExpression("scalar_dictionary_string_nonunique").stringify()
    );
    Assertions.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    inferred = thePlan.inferColumnCapabilities(null);
    Assertions.assertNotNull(inferred);
    Assertions.assertEquals(ValueType.STRING, inferred.getType());
    Assertions.assertTrue(inferred.hasNulls().isTrue());
    Assertions.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assertions.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assertions.assertFalse(inferred.hasBitmapIndexes());
    Assertions.assertFalse(inferred.hasSpatialIndexes());

    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertTrue(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertTrue(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );


    // array output of dictionary encoded string are not considered single scalar/mappable, nor vectorizable
    thePlan = plan("array(scalar_dictionary_string)");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assertions.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertTrue(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertTrue(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
  }

  @Test
  public void testMultiValueStringDictionaryEncoded()
  {
    ExpressionPlan thePlan = plan("concat(multi_dictionary_string, 'x')");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE
        )
    );
    Assertions.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assertions.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(null);
    Assertions.assertNotNull(inferred);
    Assertions.assertEquals(ValueType.STRING, inferred.getType());
    Assertions.assertTrue(inferred.hasNulls().isMaybeTrue());
    Assertions.assertTrue(inferred.isDictionaryEncoded().isTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assertions.assertTrue(inferred.hasMultipleValues().isTrue());
    Assertions.assertTrue(inferred.hasBitmapIndexes());
    Assertions.assertFalse(inferred.hasSpatialIndexes());

    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );


    thePlan = plan("concat(scalar_string, multi_dictionary_string_nonunique)");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED
        )
    );
    Assertions.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assertions.assertEquals(
        "map((\"multi_dictionary_string_nonunique\") -> concat(\"scalar_string\", \"multi_dictionary_string_nonunique\"), \"multi_dictionary_string_nonunique\")",
        thePlan.getAppliedExpression().stringify()
    );
    Assertions.assertEquals(
        "fold((\"multi_dictionary_string_nonunique\", \"scalar_string\") -> concat(\"scalar_string\", \"multi_dictionary_string_nonunique\"), \"multi_dictionary_string_nonunique\", \"scalar_string\")",
        thePlan.getAppliedFoldExpression("scalar_string").stringify()
    );
    Assertions.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    inferred = thePlan.inferColumnCapabilities(null);
    Assertions.assertNotNull(inferred);
    Assertions.assertEquals(ValueType.STRING, inferred.getType());
    Assertions.assertTrue(inferred.hasMultipleValues().isTrue());

    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );

    thePlan = plan("concat(multi_dictionary_string, multi_dictionary_string_nonunique)");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED
        )
    );
    Assertions.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assertions.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    // whoa
    Assertions.assertEquals(
        "cartesian_map((\"multi_dictionary_string\", \"multi_dictionary_string_nonunique\") -> concat(\"multi_dictionary_string\", \"multi_dictionary_string_nonunique\"), \"multi_dictionary_string\", \"multi_dictionary_string_nonunique\")",
        thePlan.getAppliedExpression().stringify()
    );
    // sort of funny, but technically correct
    Assertions.assertEquals(
        "cartesian_fold((\"multi_dictionary_string\", \"multi_dictionary_string_nonunique\", \"__acc\") -> concat(\"multi_dictionary_string\", \"multi_dictionary_string_nonunique\"), \"multi_dictionary_string\", \"multi_dictionary_string_nonunique\", \"__acc\")",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    inferred = thePlan.inferColumnCapabilities(null);
    Assertions.assertNotNull(inferred);
    Assertions.assertEquals(ValueType.STRING, inferred.getType());
    Assertions.assertTrue(inferred.hasMultipleValues().isTrue());

    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );

    thePlan = plan("array_append(multi_dictionary_string, 'foo')");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assertions.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
  }

  @Test
  public void testMultiValueStringDictionaryEncodedIllegalAccumulator()
  {
    ExpressionPlan thePlan = plan("concat(multi_dictionary_string, 'x')");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE
        )
    );
    Assertions.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assertions.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );

    thePlan = plan("concat(multi_dictionary_string, multi_dictionary_string_nonunique)");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED
        )
    );
    Assertions.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    // what happens if we try to use a multi-valued input that was not explicitly used as multi-valued as the
    // accumulator?
    final ExpressionPlan illegalAccumulatorPlan = thePlan;
    final IllegalStateException exception = Assertions.assertThrows(
        IllegalStateException.class,
        () -> illegalAccumulatorPlan.getAppliedFoldExpression("multi_dictionary_string")
    );
    Assertions.assertEquals(
        "Accumulator cannot be implicitly transformed, if it is an ARRAY or multi-valued type it must be used explicitly as such",
        exception.getMessage()
    );
  }

  @Test
  public void testIncompleteString()
  {
    ExpressionPlan thePlan = plan("concat(string_unknown, 'x')");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS
        )
    );
    Assertions.assertFalse(
        thePlan.any(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    // incomplete inputs are not transformed either, rather this will need to be detected and handled on a row-by-row
    // basis
    Assertions.assertEquals("concat(\"string_unknown\", 'x')", thePlan.getAppliedExpression().stringify());
    Assertions.assertEquals("concat(\"string_unknown\", 'x')", thePlan.getAppliedFoldExpression("__acc").stringify());
    // incomplete and unknown skip output type since we don't reliably know
    Assertions.assertNull(thePlan.getOutputType());
    Assertions.assertNull(thePlan.inferColumnCapabilities(null));

    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
  }

  @Test
  public void testArrayOutput()
  {
    // its ok to use scalar inputs to array expressions, string columns cant help it if sometimes they are single
    // valued and sometimes they are multi-valued
    ExpressionPlan thePlan = plan("array_append(scalar_string, 'x')");
    assertArrayInAndOut(thePlan);
    // with a string hint, it should look like a multi-valued string
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(ColumnType.STRING);
    Assertions.assertNotNull(inferred);
    Assertions.assertEquals(ValueType.STRING, inferred.getType());
    Assertions.assertTrue(inferred.hasNulls().isMaybeTrue());
    Assertions.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assertions.assertTrue(inferred.hasMultipleValues().isTrue());
    Assertions.assertFalse(inferred.hasBitmapIndexes());
    Assertions.assertFalse(inferred.hasSpatialIndexes());
    // with no hint though, let the array free
    inferred = thePlan.inferColumnCapabilities(ColumnType.STRING_ARRAY);
    Assertions.assertNotNull(inferred);
    Assertions.assertEquals(ColumnType.STRING_ARRAY, inferred.toColumnType());
    Assertions.assertTrue(inferred.hasNulls().isMaybeTrue());
    Assertions.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assertions.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assertions.assertFalse(inferred.hasBitmapIndexes());
    Assertions.assertFalse(inferred.hasSpatialIndexes());

    Assertions.assertEquals("array_append(\"scalar_string\", 'x')", thePlan.getAppliedExpression().stringify());
    Assertions.assertEquals("array_append(\"scalar_string\", 'x')", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, thePlan.getOutputType());

    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );

    // multi-valued are cool too
    thePlan = plan("array_append(multi_dictionary_string, 'x')");
    assertArrayInAndOut(thePlan);
    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );

    // what about incomplete inputs with arrays? they are not reported as incomplete because they are treated as arrays
    thePlan = plan("array_append(string_unknown, 'x')");
    assertArrayInAndOut(thePlan);
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, thePlan.getOutputType());
    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );

    // what about if it is the scalar argument? there it is
    thePlan = plan("array_append(multi_dictionary_string, string_unknown)");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assertions.assertFalse(
        thePlan.any(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    // incomplete and unknown skip output type since we don't reliably know
    Assertions.assertNull(thePlan.getOutputType());
    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );

    // array types are cool too
    thePlan = plan("array_append(string_array_1, 'x')");
    assertArrayInAndOut(thePlan);
    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );

    thePlan = plan("array_append(string_array_1, 'x')");
    assertArrayInAndOut(thePlan);
    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
  }


  @Test
  public void testScalarOutputMultiValueInput()
  {
    ExpressionPlan thePlan = plan("array_to_string(array_append(scalar_string, 'x'), ',')");
    assertArrayInput(thePlan);
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(ColumnType.STRING);
    Assertions.assertNotNull(inferred);
    Assertions.assertEquals(ValueType.STRING, inferred.getType());
    Assertions.assertTrue(inferred.hasNulls().isTrue());
    Assertions.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assertions.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assertions.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assertions.assertFalse(inferred.hasBitmapIndexes());
    Assertions.assertFalse(inferred.hasSpatialIndexes());

    Assertions.assertEquals(
        "array_to_string(array_append(\"scalar_string\", 'x'), ',')",
        thePlan.getAppliedExpression().stringify()
    );
    Assertions.assertEquals(
        "array_to_string(array_append(\"scalar_string\", 'x'), ',')",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    Assertions.assertEquals(ExpressionType.STRING, thePlan.getOutputType());

    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );

    // what about a multi-valued input
    thePlan = plan("array_to_string(array_append(scalar_string, multi_dictionary_string), ',')");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED
        )
    );
    Assertions.assertFalse(
        thePlan.any(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );

    Assertions.assertEquals(
        "array_to_string(map((\"multi_dictionary_string\") -> array_append(\"scalar_string\", \"multi_dictionary_string\"), \"multi_dictionary_string\"), ',')",
        thePlan.getAppliedExpression().stringify()
    );
    Assertions.assertEquals(
        "array_to_string(fold((\"multi_dictionary_string\", \"scalar_string\") -> array_append(\"scalar_string\", \"multi_dictionary_string\"), \"multi_dictionary_string\", \"scalar_string\"), ',')",
        thePlan.getAppliedFoldExpression("scalar_string").stringify()
    );
    // why is this null
    Assertions.assertEquals(ExpressionType.STRING, thePlan.getOutputType());

    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
  }

  @Test
  public void testScalarOutputArrayInput()
  {
    ExpressionPlan thePlan = plan("array_to_string(array_append(string_array_1, 'x'), ',')");
    assertArrayInput(thePlan);

    Assertions.assertEquals(
        "array_to_string(array_append(\"string_array_1\", 'x'), ',')",
        thePlan.getAppliedExpression().stringify()
    );
    Assertions.assertEquals(
        "array_to_string(array_append(\"string_array_1\", 'x'), ',')",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    Assertions.assertEquals(ExpressionType.STRING, thePlan.getOutputType());


    thePlan = plan("array_to_string(array_concat(string_array_1, string_array_2), ',')");
    assertArrayInput(thePlan);
    Assertions.assertEquals(ExpressionType.STRING, thePlan.getOutputType());

    thePlan = plan("fold((x, acc) -> acc + x, array_concat(long_array_1, long_array_2), 0)");
    assertArrayInput(thePlan);
    Assertions.assertEquals(
        "fold((\"x\", \"acc\") -> (\"acc\" + \"x\"), array_concat(\"long_array_1\", \"long_array_2\"), 0)",
        thePlan.getAppliedExpression().stringify()
    );
    Assertions.assertEquals(
        "fold((\"x\", \"acc\") -> (\"acc\" + \"x\"), array_concat(\"long_array_1\", \"long_array_2\"), 0)",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    Assertions.assertEquals(ExpressionType.LONG, thePlan.getOutputType());

    thePlan = plan("fold((x, acc) -> acc * x, array_concat(double_array_1, double_array_2), 0.0)");
    assertArrayInput(thePlan);
    Assertions.assertEquals(
        "fold((\"x\", \"acc\") -> (\"acc\" * \"x\"), array_concat(\"double_array_1\", \"double_array_2\"), 0.0)",
        thePlan.getAppliedExpression().stringify()
    );
    Assertions.assertEquals(
        "fold((\"x\", \"acc\") -> (\"acc\" * \"x\"), array_concat(\"double_array_1\", \"double_array_2\"), 0.0)",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    Assertions.assertEquals(ExpressionType.DOUBLE, thePlan.getOutputType());
  }

  @Test
  public void testArrayConstruction()
  {
    ExpressionPlan thePlan = plan("array(long1, long2)");
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assertions.assertFalse(
        thePlan.any(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS
        )
    );
    assertFallbackVectorizable(thePlan);
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, thePlan.getOutputType());

    thePlan = plan("array(long1, double1)");
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, thePlan.getOutputType());
    thePlan = plan("array(long1, double1, scalar_string)");
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, thePlan.getOutputType());
  }

  @Test
  public void testNestedColumnExpression()
  {
    ExpressionPlan thePlan = plan("json_object('long1', long1, 'long2', long2)");
    Assertions.assertFalse(
        thePlan.any(
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS
        )
    );
    Assertions.assertEquals(ExpressionType.NESTED_DATA, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(
        ExpressionType.toColumnType(thePlan.getOutputType())
    );
    Assertions.assertEquals(
        ColumnType.NESTED_DATA.getType(),
        inferred.getType()
    );
    Assertions.assertEquals(
        ColumnType.NESTED_DATA.getComplexTypeName(),
        inferred.getComplexTypeName()
    );

    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    // all numeric inputs so these are true
    Assertions.assertTrue(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertTrue(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
  }

  @Test
  public void testDictionaryComplexStringOutput()
  {
    ExpressionPlan thePlan = plan("dict_complex_to_string(dictionary_complex)");
    Assertions.assertFalse(
        thePlan.any(
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS
        )
    );
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR
        )
    );
    assertFallbackVectorizable(thePlan);

    Assertions.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(
        ExpressionType.toColumnType(thePlan.getOutputType())
    );
    Assertions.assertEquals(
        ColumnType.STRING.getType(),
        inferred.getType()
    );
    Assertions.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());

    Assertions.assertFalse(
        DeferExpressionDimensions.SINGLE_STRING.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
    Assertions.assertFalse(
        DeferExpressionDimensions.FIXED_WIDTH.useDeferredGroupBySelector(
            thePlan,
            thePlan.getAnalysis().getRequiredBindingsList(),
            SYNTHETIC_INSPECTOR
        )
    );
  }

  @Test
  public void testNowIsNotConstant()
  {
    ExpressionPlan thePlan = plan("now()");
    Assertions.assertFalse(
        thePlan.is(ExpressionPlan.Trait.CONSTANT)
    );
    Assertions.assertFalse(thePlan.isConstant());
  }

  @Test
  public void testNowWrappedInExpressionIsNotConstant()
  {
    ExpressionPlan thePlan = plan("now() + 1000");
    Assertions.assertFalse(
        thePlan.is(ExpressionPlan.Trait.CONSTANT)
    );
    Assertions.assertFalse(thePlan.isConstant());
  }

  private static ExpressionPlan plan(String expression)
  {
    return ExpressionPlanner.plan(SYNTHETIC_INSPECTOR, Parser.parse(expression, MACRO_TABLE));
  }

  private static void assertArrayInput(ExpressionPlan thePlan)
  {
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NON_SCALAR_INPUTS
        )
    );
    Assertions.assertFalse(
        thePlan.any(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED
        )
    );
    assertFallbackVectorizable(thePlan);
  }

  private static void assertArrayInAndOut(ExpressionPlan thePlan)
  {
    Assertions.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assertions.assertFalse(
        thePlan.any(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED
        )
    );
    assertFallbackVectorizable(thePlan);
  }


  private static void assertFallbackVectorizable(ExpressionPlan thePlan)
  {
    if (ExpressionProcessing.allowVectorizeFallback()) {
      Assertions.assertTrue(
          thePlan.is(
              ExpressionPlan.Trait.VECTORIZABLE
          )
      );
    } else {
      Assertions.assertFalse(
          thePlan.is(
              ExpressionPlan.Trait.VECTORIZABLE
          )
      );
    }
  }

  private static class TestMacroTable extends ExprMacroTable
  {
    public TestMacroTable()
    {
      super(
          ImmutableList.<ExprMacroTable.ExprMacro>builder()
                       .addAll(TestExprMacroTable.INSTANCE.getMacros())
                       .add(new ExprMacroTable.ExprMacro()
                       {
                         @Override
                         public Expr apply(List<Expr> args)
                         {
                           return new ExprMacroTable.BaseScalarMacroFunctionExpr(this, args)
                           {
                             @Override
                             public ExprEval eval(ObjectBinding bindings)
                             {
                               throw DruidException.defensive("just for planner test");
                             }

                             @Nullable
                             @Override
                             public ExpressionType getOutputType(InputBindingInspector inspector)
                             {
                               return ExpressionType.STRING;
                             }
                           };
                         }

                         @Override
                         public String name()
                         {
                           return "dict_complex_to_string";
                         }
                       })
                       .build()
      );
    }
  }
}

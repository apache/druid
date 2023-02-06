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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.expression.NestedDataExpressions;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.util.Map;

public class ExpressionPlannerTest extends InitializedNullHandlingTest
{
  public static final ColumnInspector SYNTHETIC_INSPECTOR = new ColumnInspector()
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
                    .build();

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return capabilitiesMap.get(column);
    }
  };

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testUnknown()
  {
    // column has no capabilities
    // the vectorize query engine contracts is such that the lack of column capabilities is indicative of a nil column
    // so this is vectorizable
    // for non-vectorized expression processing, this will probably end up using a selector that examines inputs on a
    // row by row basis to determine if the expression needs applied to multi-valued inputs

    ExpressionPlan thePlan = plan("concat(x, 'x')");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertFalse(
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
    Assert.assertEquals("concat(\"x\", 'x')", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals("concat(\"x\", 'x')", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assert.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertTrue(inferred.hasNulls().isTrue());
    Assert.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());

    // what if both inputs are unknown, can we know things?
    thePlan = plan("x * y");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.UNKNOWN_INPUTS
        )
    );
    Assert.assertFalse(
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

    Assert.assertEquals("(\"x\" * \"y\")", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals("(\"x\" * \"y\")", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assert.assertNull(thePlan.getOutputType());
    Assert.assertNull(thePlan.inferColumnCapabilities(null));
    // no we cannot
  }

  @Test
  public void testScalarStringNondictionaryEncoded()
  {
    ExpressionPlan thePlan = plan("concat(scalar_string, 'x')");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertFalse(
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
    Assert.assertEquals("concat(\"scalar_string\", 'x')", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals("concat(\"scalar_string\", 'x')", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assert.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertTrue(inferred.hasNulls().isTrue());
    Assert.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());
  }

  @Test
  public void testScalarNumeric()
  {
    ExpressionPlan thePlan = plan("long1 + 5");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assert.assertEquals("(\"long1\" + 5)", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals("(\"long1\" + 5)", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assert.assertEquals("(\"long1\" + 5)", thePlan.getAppliedFoldExpression("long1").stringify());
    Assert.assertEquals(ExpressionType.LONG, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.LONG, inferred.getType());
    if (NullHandling.sqlCompatible()) {
      Assert.assertTrue(inferred.hasNulls().isMaybeTrue());
    } else {
      Assert.assertFalse(inferred.hasNulls().isMaybeTrue());
    }
    Assert.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());

    thePlan = plan("long1 + 5.0");
    Assert.assertEquals(ExpressionType.DOUBLE, thePlan.getOutputType());

    thePlan = plan("double1 * double2");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertFalse(
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
    Assert.assertEquals("(\"double1\" * \"double2\")", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals("(\"double1\" * \"double2\")", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assert.assertEquals("(\"double1\" * \"double2\")", thePlan.getAppliedFoldExpression("double1").stringify());
    Assert.assertEquals(ExpressionType.DOUBLE, thePlan.getOutputType());
    inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.DOUBLE, inferred.getType());
    if (NullHandling.sqlCompatible()) {
      Assert.assertTrue(inferred.hasNulls().isMaybeTrue());
    } else {
      Assert.assertFalse(inferred.hasNulls().isMaybeTrue());
    }
    Assert.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());
  }

  @Test
  public void testScalarStringDictionaryEncoded()
  {
    ExpressionPlan thePlan = plan("concat(scalar_dictionary_string, 'x')");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assert.assertEquals("concat(\"scalar_dictionary_string\", 'x')", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals(
        "concat(\"scalar_dictionary_string\", 'x')",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    Assert.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertTrue(inferred.hasNulls().isTrue());
    Assert.assertTrue(inferred.isDictionaryEncoded().isTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());

    // multiple input columns
    thePlan = plan("concat(scalar_dictionary_string, scalar_dictionary_string_nonunique)");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertFalse(
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
    Assert.assertEquals(
        "concat(\"scalar_dictionary_string\", \"scalar_dictionary_string_nonunique\")",
        thePlan.getAppliedExpression().stringify()
    );
    Assert.assertEquals(
        "concat(\"scalar_dictionary_string\", \"scalar_dictionary_string_nonunique\")",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    // what if scalar_dictionary_string_nonunique is an accumulator instead? nope, still no NEEDS_APPLIED so nothing to do
    Assert.assertEquals(
        "concat(\"scalar_dictionary_string\", \"scalar_dictionary_string_nonunique\")",
        thePlan.getAppliedFoldExpression("scalar_dictionary_string_nonunique").stringify()
    );
    Assert.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertTrue(inferred.hasNulls().isTrue());
    Assert.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());

    // array output of dictionary encoded string are not considered single scalar/mappable, nor vectorizable
    thePlan = plan("array(scalar_dictionary_string)");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assert.assertFalse(
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
  }

  @Test
  public void testMultiValueStringDictionaryEncoded()
  {
    ExpressionPlan thePlan = plan("concat(multi_dictionary_string, 'x')");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertTrue(inferred.hasNulls().isMaybeTrue());
    Assert.assertTrue(inferred.isDictionaryEncoded().isTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertTrue(inferred.hasMultipleValues().isTrue());
    Assert.assertFalse(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());

    thePlan = plan("concat(scalar_string, multi_dictionary_string_nonunique)");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertEquals(
        "map((\"multi_dictionary_string_nonunique\") -> concat(\"scalar_string\", \"multi_dictionary_string_nonunique\"), \"multi_dictionary_string_nonunique\")",
        thePlan.getAppliedExpression().stringify()
    );
    Assert.assertEquals(
        "fold((\"multi_dictionary_string_nonunique\", \"scalar_string\") -> concat(\"scalar_string\", \"multi_dictionary_string_nonunique\"), \"multi_dictionary_string_nonunique\", \"scalar_string\")",
        thePlan.getAppliedFoldExpression("scalar_string").stringify()
    );
    Assert.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertTrue(inferred.hasMultipleValues().isTrue());

    thePlan = plan("concat(multi_dictionary_string, multi_dictionary_string_nonunique)");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
    // whoa
    Assert.assertEquals(
        "cartesian_map((\"multi_dictionary_string\", \"multi_dictionary_string_nonunique\") -> concat(\"multi_dictionary_string\", \"multi_dictionary_string_nonunique\"), \"multi_dictionary_string\", \"multi_dictionary_string_nonunique\")",
        thePlan.getAppliedExpression().stringify()
    );
    // sort of funny, but technically correct
    Assert.assertEquals(
        "cartesian_fold((\"multi_dictionary_string\", \"multi_dictionary_string_nonunique\", \"__acc\") -> concat(\"multi_dictionary_string\", \"multi_dictionary_string_nonunique\"), \"multi_dictionary_string\", \"multi_dictionary_string_nonunique\", \"__acc\")",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertTrue(inferred.hasMultipleValues().isTrue());

    thePlan = plan("array_append(multi_dictionary_string, 'foo')");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
  }

  @Test
  public void testMultiValueStringDictionaryEncodedIllegalAccumulator()
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage(
        "Accumulator cannot be implicitly transformed, if it is an ARRAY or multi-valued type it must be used explicitly as such"
    );
    ExpressionPlan thePlan = plan("concat(multi_dictionary_string, 'x')");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertEquals(ExpressionType.STRING, thePlan.getOutputType());

    thePlan = plan("concat(multi_dictionary_string, multi_dictionary_string_nonunique)");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED
        )
    );
    Assert.assertFalse(
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
    thePlan.getAppliedFoldExpression("multi_dictionary_string");
    Assert.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
  }

  @Test
  public void testIncompleteString()
  {
    ExpressionPlan thePlan = plan("concat(string_unknown, 'x')");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS
        )
    );
    Assert.assertFalse(
        thePlan.is(
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
    Assert.assertEquals("concat(\"string_unknown\", 'x')", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals("concat(\"string_unknown\", 'x')", thePlan.getAppliedFoldExpression("__acc").stringify());
    // incomplete and unknown skip output type since we don't reliably know
    Assert.assertNull(thePlan.getOutputType());
    Assert.assertNull(thePlan.inferColumnCapabilities(null));
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
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertTrue(inferred.hasNulls().isMaybeTrue());
    Assert.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertTrue(inferred.hasMultipleValues().isTrue());
    Assert.assertFalse(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());
    // with no hint though, let the array free
    inferred = thePlan.inferColumnCapabilities(ColumnType.STRING_ARRAY);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ColumnType.STRING_ARRAY, inferred.toColumnType());
    Assert.assertTrue(inferred.hasNulls().isMaybeTrue());
    Assert.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());

    Assert.assertEquals("array_append(\"scalar_string\", 'x')", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals("array_append(\"scalar_string\", 'x')", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assert.assertEquals(ExpressionType.STRING_ARRAY, thePlan.getOutputType());

    // multi-valued are cool too
    thePlan = plan("array_append(multi_dictionary_string, 'x')");
    assertArrayInAndOut(thePlan);

    // what about incomplete inputs with arrays? they are not reported as incomplete because they are treated as arrays
    thePlan = plan("array_append(string_unknown, 'x')");
    assertArrayInAndOut(thePlan);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, thePlan.getOutputType());

    // what about if it is the scalar argument? there it is
    thePlan = plan("array_append(multi_dictionary_string, string_unknown)");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    // incomplete and unknown skip output type since we don't reliably know
    Assert.assertNull(thePlan.getOutputType());

    // array types are cool too
    thePlan = plan("array_append(string_array_1, 'x')");
    assertArrayInAndOut(thePlan);

    thePlan = plan("array_append(string_array_1, 'x')");
    assertArrayInAndOut(thePlan);
  }


  @Test
  public void testScalarOutputMultiValueInput()
  {
    ExpressionPlan thePlan = plan("array_to_string(array_append(scalar_string, 'x'), ',')");
    assertArrayInput(thePlan);
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(ColumnType.STRING);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertTrue(inferred.hasNulls().isTrue());
    Assert.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());

    Assert.assertEquals(
        "array_to_string(array_append(\"scalar_string\", 'x'), ',')",
        thePlan.getAppliedExpression().stringify()
    );
    Assert.assertEquals(
        "array_to_string(array_append(\"scalar_string\", 'x'), ',')",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    Assert.assertEquals(ExpressionType.STRING, thePlan.getOutputType());

    // what about a multi-valued input
    thePlan = plan("array_to_string(array_append(scalar_string, multi_dictionary_string), ',')");
    assertArrayInput(thePlan);

    Assert.assertEquals(
        "array_to_string(map((\"multi_dictionary_string\") -> array_append(\"scalar_string\", \"multi_dictionary_string\"), \"multi_dictionary_string\"), ',')",
        thePlan.getAppliedExpression().stringify()
    );
    Assert.assertEquals(
        "array_to_string(fold((\"multi_dictionary_string\", \"scalar_string\") -> array_append(\"scalar_string\", \"multi_dictionary_string\"), \"multi_dictionary_string\", \"scalar_string\"), ',')",
        thePlan.getAppliedFoldExpression("scalar_string").stringify()
    );
    // why is this null
    Assert.assertEquals(ExpressionType.STRING, thePlan.getOutputType());
  }

  @Test
  public void testScalarOutputArrayInput()
  {
    ExpressionPlan thePlan = plan("array_to_string(array_append(string_array_1, 'x'), ',')");
    assertArrayInput(thePlan);

    Assert.assertEquals(
        "array_to_string(array_append(\"string_array_1\", 'x'), ',')",
        thePlan.getAppliedExpression().stringify()
    );
    Assert.assertEquals(
        "array_to_string(array_append(\"string_array_1\", 'x'), ',')",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    Assert.assertEquals(ExpressionType.STRING, thePlan.getOutputType());


    thePlan = plan("array_to_string(array_concat(string_array_1, string_array_2), ',')");
    assertArrayInput(thePlan);
    Assert.assertEquals(ExpressionType.STRING, thePlan.getOutputType());

    thePlan = plan("fold((x, acc) -> acc + x, array_concat(long_array_1, long_array_2), 0)");
    assertArrayInput(thePlan);
    Assert.assertEquals(
        "fold((\"x\", \"acc\") -> (\"acc\" + \"x\"), array_concat(\"long_array_1\", \"long_array_2\"), 0)",
        thePlan.getAppliedExpression().stringify()
    );
    Assert.assertEquals(
        "fold((\"x\", \"acc\") -> (\"acc\" + \"x\"), array_concat(\"long_array_1\", \"long_array_2\"), 0)",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    Assert.assertEquals(ExpressionType.LONG, thePlan.getOutputType());

    thePlan = plan("fold((x, acc) -> acc * x, array_concat(double_array_1, double_array_2), 0.0)");
    assertArrayInput(thePlan);
    Assert.assertEquals(
        "fold((\"x\", \"acc\") -> (\"acc\" * \"x\"), array_concat(\"double_array_1\", \"double_array_2\"), 0.0)",
        thePlan.getAppliedExpression().stringify()
    );
    Assert.assertEquals(
        "fold((\"x\", \"acc\") -> (\"acc\" * \"x\"), array_concat(\"double_array_1\", \"double_array_2\"), 0.0)",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    Assert.assertEquals(ExpressionType.DOUBLE, thePlan.getOutputType());
  }

  @Test
  public void testArrayConstruction()
  {
    ExpressionPlan thePlan = plan("array(long1, long2)");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertEquals(ExpressionType.LONG_ARRAY, thePlan.getOutputType());

    thePlan = plan("array(long1, double1)");
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, thePlan.getOutputType());
    thePlan = plan("array(long1, double1, scalar_string)");
    Assert.assertEquals(ExpressionType.STRING_ARRAY, thePlan.getOutputType());
  }

  @Test
  public void testNestedColumnExpression()
  {
    ExpressionPlan thePlan = plan("json_object('long1', long1, 'long2', long2)");
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertEquals(NestedDataExpressions.TYPE, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(
        ExpressionType.toColumnType(thePlan.getOutputType())
    );
    Assert.assertEquals(
        NestedDataComplexTypeSerde.TYPE.getType(),
        inferred.getType()
    );
    Assert.assertEquals(
        NestedDataExpressions.TYPE.getComplexTypeName(),
        inferred.getComplexTypeName()
    );
  }

  private static ExpressionPlan plan(String expression)
  {
    return ExpressionPlanner.plan(SYNTHETIC_INSPECTOR, Parser.parse(expression, TestExprMacroTable.INSTANCE));
  }

  private static void assertArrayInput(ExpressionPlan thePlan)
  {
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NON_SCALAR_INPUTS
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
  }

  private static void assertArrayInAndOut(ExpressionPlan thePlan)
  {
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.SINGLE_INPUT_SCALAR,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
  }
}

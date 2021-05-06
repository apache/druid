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
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
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
                        "l1",
                        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.LONG)
                    )
                    .put(
                        "l2",
                        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.LONG)
                    )
                    .put(
                        "f1",
                        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.FLOAT)
                    )
                    .put(
                        "f2",
                        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.FLOAT)
                    )
                    .put(
                        "d1",
                        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.DOUBLE)
                    )
                    .put(
                        "d2",
                        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.DOUBLE)
                    )
                    .put(
                        "s1",
                        ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities()
                    )
                    .put(
                        // segment style single value dictionary encoded with unique sorted dictionary
                        "s2",
                        new ColumnCapabilitiesImpl().setType(ValueType.STRING)
                                                    .setDictionaryEncoded(true)
                                                    .setHasBitmapIndexes(true)
                                                    .setDictionaryValuesSorted(true)
                                                    .setDictionaryValuesUnique(true)
                                                    .setHasMultipleValues(false)
                    )
                    .put(
                        // dictionary encoded but not unique or sorted, maybe an indexed table from a join result
                        "s3",
                        new ColumnCapabilitiesImpl().setType(ValueType.STRING)
                                                    .setDictionaryEncoded(true)
                                                    .setHasBitmapIndexes(false)
                                                    .setDictionaryValuesSorted(false)
                                                    .setDictionaryValuesUnique(false)
                                                    .setHasMultipleValues(false)
                    )
                    .put(
                        // string with unknown multi-valuedness
                        "s4",
                        new ColumnCapabilitiesImpl().setType(ValueType.STRING)
                    )
                    .put(
                        // dictionary encoded multi valued string dimension f
                        "m1",
                        new ColumnCapabilitiesImpl().setType(ValueType.STRING)
                                                    .setDictionaryEncoded(true)
                                                    .setHasBitmapIndexes(true)
                                                    .setDictionaryValuesUnique(true)
                                                    .setDictionaryValuesSorted(true)
                                                    .setHasMultipleValues(true)
                    )
                    .put(
                        // simple multi valued string dimension f
                        "m2",
                        new ColumnCapabilitiesImpl().setType(ValueType.STRING)
                                                    .setDictionaryEncoded(false)
                                                    .setHasBitmapIndexes(false)
                                                    .setDictionaryValuesUnique(false)
                                                    .setDictionaryValuesSorted(false)
                                                    .setHasMultipleValues(true)
                    )
                    .put(
                        "sa1",
                        ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ValueType.STRING_ARRAY)
                    )
                    .put(
                        "sa2",
                        ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ValueType.STRING_ARRAY)
                    )
                    .put(
                        "la1",
                        ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ValueType.LONG_ARRAY)
                    )
                    .put(
                        "la2",
                        ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ValueType.LONG_ARRAY)
                    )
                    .put(
                        "da1",
                        ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ValueType.DOUBLE_ARRAY)
                    )
                    .put(
                        "da2",
                        ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ValueType.DOUBLE_ARRAY)
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
    Assert.assertEquals(ExprType.STRING, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertNull(inferred.getComplexTypeName());
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
    ExpressionPlan thePlan = plan("concat(s1, 'x')");
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
    Assert.assertEquals("concat(\"s1\", 'x')", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals("concat(\"s1\", 'x')", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assert.assertEquals(ExprType.STRING, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertNull(inferred.getComplexTypeName());
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
    ExpressionPlan thePlan = plan("l1 + 5");
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
    Assert.assertEquals("(\"l1\" + 5)", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals("(\"l1\" + 5)", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assert.assertEquals("(\"l1\" + 5)", thePlan.getAppliedFoldExpression("l1").stringify());
    Assert.assertEquals(ExprType.LONG, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.LONG, inferred.getType());
    Assert.assertNull(inferred.getComplexTypeName());
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

    thePlan = plan("l1 + 5.0");
    Assert.assertEquals(ExprType.DOUBLE, thePlan.getOutputType());

    thePlan = plan("d1 * d2");
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
    Assert.assertEquals("(\"d1\" * \"d2\")", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals("(\"d1\" * \"d2\")", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assert.assertEquals("(\"d1\" * \"d2\")", thePlan.getAppliedFoldExpression("d1").stringify());
    Assert.assertEquals(ExprType.DOUBLE, thePlan.getOutputType());
    inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.DOUBLE, inferred.getType());
    Assert.assertNull(inferred.getComplexTypeName());
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
    ExpressionPlan thePlan = plan("concat(s2, 'x')");
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
    Assert.assertEquals("concat(\"s2\", 'x')", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals("concat(\"s2\", 'x')", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assert.assertEquals(ExprType.STRING, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertNull(inferred.getComplexTypeName());
    Assert.assertTrue(inferred.hasNulls().isTrue());
    Assert.assertTrue(inferred.isDictionaryEncoded().isTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assert.assertTrue(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());

    // multiple input columns
    thePlan = plan("concat(s2, s3)");
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
    Assert.assertEquals("concat(\"s2\", \"s3\")", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals("concat(\"s2\", \"s3\")", thePlan.getAppliedFoldExpression("__acc").stringify());
    // what if s3 is an accumulator instead? nope, still no NEEDS_APPLIED so nothing to do
    Assert.assertEquals(
        "concat(\"s2\", \"s3\")",
        thePlan.getAppliedFoldExpression("s3").stringify()
    );
    Assert.assertEquals(ExprType.STRING, thePlan.getOutputType());
    inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertNull(inferred.getComplexTypeName());
    Assert.assertTrue(inferred.hasNulls().isTrue());
    Assert.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());
  }

  @Test
  public void testMultiValueStringDictionaryEncoded()
  {
    ExpressionPlan thePlan = plan("concat(m1, 'x')");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertEquals(ExprType.STRING, thePlan.getOutputType());
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertNull(inferred.getComplexTypeName());
    Assert.assertTrue(inferred.hasNulls().isMaybeTrue());
    Assert.assertTrue(inferred.isDictionaryEncoded().isTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertTrue(inferred.hasMultipleValues().isTrue());
    Assert.assertTrue(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());

    thePlan = plan("concat(s1, m2)");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertEquals(
        "map((\"m2\") -> concat(\"s1\", \"m2\"), \"m2\")",
        thePlan.getAppliedExpression().stringify()
    );
    Assert.assertEquals(
        "fold((\"m2\", \"s1\") -> concat(\"s1\", \"m2\"), \"m2\", \"s1\")",
        thePlan.getAppliedFoldExpression("s1").stringify()
    );
    Assert.assertEquals(ExprType.STRING, thePlan.getOutputType());
    inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertTrue(inferred.hasMultipleValues().isTrue());

    thePlan = plan("concat(m1, m2)");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertEquals(ExprType.STRING, thePlan.getOutputType());
    // whoa
    Assert.assertEquals(
        "cartesian_map((\"m1\", \"m2\") -> concat(\"m1\", \"m2\"), \"m1\", \"m2\")",
        thePlan.getAppliedExpression().stringify()
    );
    // sort of funny, but technically correct
    Assert.assertEquals(
        "cartesian_fold((\"m1\", \"m2\", \"__acc\") -> concat(\"m1\", \"m2\"), \"m1\", \"m2\", \"__acc\")",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    inferred = thePlan.inferColumnCapabilities(null);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertTrue(inferred.hasMultipleValues().isTrue());
  }

  @Test
  public void testMultiValueStringDictionaryEncodedIllegalAccumulator()
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage(
        "Accumulator cannot be implicitly transformed, if it is an ARRAY or multi-valued type it must be used explicitly as such"
    );
    ExpressionPlan thePlan = plan("concat(m1, 'x')");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.SINGLE_INPUT_MAPPABLE,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    Assert.assertEquals(ExprType.STRING, thePlan.getOutputType());

    thePlan = plan("concat(m1, m2)");
    Assert.assertTrue(
        thePlan.is(
            ExpressionPlan.Trait.NEEDS_APPLIED,
            ExpressionPlan.Trait.NON_SCALAR_OUTPUT
        )
    );
    Assert.assertFalse(
        thePlan.is(
            ExpressionPlan.Trait.INCOMPLETE_INPUTS,
            ExpressionPlan.Trait.UNKNOWN_INPUTS,
            ExpressionPlan.Trait.NON_SCALAR_INPUTS,
            ExpressionPlan.Trait.VECTORIZABLE
        )
    );
    // what happens if we try to use a multi-valued input that was not explicitly used as multi-valued as the
    // accumulator?
    thePlan.getAppliedFoldExpression("m1");
    Assert.assertEquals(ExprType.STRING, thePlan.getOutputType());
  }

  @Test
  public void testIncompleteString()
  {
    ExpressionPlan thePlan = plan("concat(s4, 'x')");
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
    Assert.assertEquals("concat(\"s4\", 'x')", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals("concat(\"s4\", 'x')", thePlan.getAppliedFoldExpression("__acc").stringify());
    // incomplete and unknown skip output type since we don't reliably know
    Assert.assertNull(thePlan.getOutputType());
    Assert.assertNull(thePlan.inferColumnCapabilities(null));
  }

  @Test
  public void testArrayOutput()
  {
    // its ok to use scalar inputs to array expressions, string columns cant help it if sometimes they are single
    // valued and sometimes they are multi-valued
    ExpressionPlan thePlan = plan("array_append(s1, 'x')");
    assertArrayInAndOut(thePlan);
    // with a string hint, it should look like a multi-valued string
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(ValueType.STRING);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertNull(inferred.getComplexTypeName());
    Assert.assertTrue(inferred.hasNulls().isMaybeTrue());
    Assert.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertTrue(inferred.hasMultipleValues().isTrue());
    Assert.assertFalse(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());
    // with no hint though, let the array free
    inferred = thePlan.inferColumnCapabilities(ValueType.STRING_ARRAY);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING_ARRAY, inferred.getType());
    Assert.assertNull(inferred.getComplexTypeName());
    Assert.assertTrue(inferred.hasNulls().isMaybeTrue());
    Assert.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertTrue(inferred.hasMultipleValues().isTrue());
    Assert.assertFalse(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());

    Assert.assertEquals("array_append(\"s1\", 'x')", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals("array_append(\"s1\", 'x')", thePlan.getAppliedFoldExpression("__acc").stringify());
    Assert.assertEquals(ExprType.STRING_ARRAY, thePlan.getOutputType());

    // multi-valued are cool too
    thePlan = plan("array_append(m1, 'x')");
    assertArrayInAndOut(thePlan);

    // what about incomplete inputs with arrays? they are not reported as incomplete because they are treated as arrays
    thePlan = plan("array_append(s4, 'x')");
    assertArrayInAndOut(thePlan);
    Assert.assertEquals(ExprType.STRING_ARRAY, thePlan.getOutputType());

    // what about if it is the scalar argument? there it is
    thePlan = plan("array_append(m1, s4)");
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
    thePlan = plan("array_append(sa1, 'x')");
    assertArrayInAndOut(thePlan);

    thePlan = plan("array_append(sa1, 'x')");
    assertArrayInAndOut(thePlan);
  }


  @Test
  public void testScalarOutputMultiValueInput()
  {
    ExpressionPlan thePlan = plan("array_to_string(array_append(s1, 'x'), ',')");
    assertArrayInput(thePlan);
    ColumnCapabilities inferred = thePlan.inferColumnCapabilities(ValueType.STRING);
    Assert.assertNotNull(inferred);
    Assert.assertEquals(ValueType.STRING, inferred.getType());
    Assert.assertNull(inferred.getComplexTypeName());
    Assert.assertTrue(inferred.hasNulls().isTrue());
    Assert.assertFalse(inferred.isDictionaryEncoded().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesSorted().isMaybeTrue());
    Assert.assertFalse(inferred.areDictionaryValuesUnique().isMaybeTrue());
    Assert.assertFalse(inferred.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(inferred.hasBitmapIndexes());
    Assert.assertFalse(inferred.hasSpatialIndexes());

    Assert.assertEquals("array_to_string(array_append(\"s1\", 'x'), ',')", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals(
        "array_to_string(array_append(\"s1\", 'x'), ',')",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    Assert.assertEquals(ExprType.STRING, thePlan.getOutputType());

    // what about a multi-valued input
    thePlan = plan("array_to_string(array_append(s1, m1), ',')");
    assertArrayInput(thePlan);

    Assert.assertEquals(
        "array_to_string(map((\"m1\") -> array_append(\"s1\", \"m1\"), \"m1\"), ',')",
        thePlan.getAppliedExpression().stringify()
    );
    Assert.assertEquals(
        "array_to_string(fold((\"m1\", \"s1\") -> array_append(\"s1\", \"m1\"), \"m1\", \"s1\"), ',')",
        thePlan.getAppliedFoldExpression("s1").stringify()
    );
    // why is this null
    Assert.assertEquals(ExprType.STRING, thePlan.getOutputType());
  }

  @Test
  public void testScalarOutputArrayInput()
  {
    ExpressionPlan thePlan = plan("array_to_string(array_append(sa1, 'x'), ',')");
    assertArrayInput(thePlan);

    Assert.assertEquals("array_to_string(array_append(\"sa1\", 'x'), ',')", thePlan.getAppliedExpression().stringify());
    Assert.assertEquals(
        "array_to_string(array_append(\"sa1\", 'x'), ',')",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    Assert.assertEquals(ExprType.STRING, thePlan.getOutputType());


    thePlan = plan("array_to_string(array_concat(sa1, sa2), ',')");
    assertArrayInput(thePlan);
    Assert.assertEquals(ExprType.STRING, thePlan.getOutputType());

    thePlan = plan("fold((x, acc) -> acc + x, array_concat(la1, la2), 0)");
    assertArrayInput(thePlan);
    Assert.assertEquals(
        "fold((\"x\", \"acc\") -> (\"acc\" + \"x\"), array_concat(\"la1\", \"la2\"), 0)",
        thePlan.getAppliedExpression().stringify()
    );
    Assert.assertEquals(
        "fold((\"x\", \"acc\") -> (\"acc\" + \"x\"), array_concat(\"la1\", \"la2\"), 0)",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    Assert.assertEquals(ExprType.LONG, thePlan.getOutputType());

    thePlan = plan("fold((x, acc) -> acc * x, array_concat(da1, da2), 0.0)");
    assertArrayInput(thePlan);
    Assert.assertEquals(
        "fold((\"x\", \"acc\") -> (\"acc\" * \"x\"), array_concat(\"da1\", \"da2\"), 0.0)",
        thePlan.getAppliedExpression().stringify()
    );
    Assert.assertEquals(
        "fold((\"x\", \"acc\") -> (\"acc\" * \"x\"), array_concat(\"da1\", \"da2\"), 0.0)",
        thePlan.getAppliedFoldExpression("__acc").stringify()
    );
    Assert.assertEquals(ExprType.DOUBLE, thePlan.getOutputType());
  }

  @Test
  public void testArrayConstruction()
  {
    ExpressionPlan thePlan = plan("array(l1, l2)");
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
    Assert.assertEquals(ExprType.LONG_ARRAY, thePlan.getOutputType());

    thePlan = plan("array(l1, d1)");
    Assert.assertEquals(ExprType.DOUBLE_ARRAY, thePlan.getOutputType());
    thePlan = plan("array(l1, d1, s1)");
    Assert.assertEquals(ExprType.STRING_ARRAY, thePlan.getOutputType());
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

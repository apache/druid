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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ComparableList;
import org.apache.druid.segment.data.ComparableStringArray;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DimensionHandlerUtilsTest extends InitializedNullHandlingTest
{

  private static final String DIM_NAME = "dim";
  private static final String TYPE = "testType";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final ComparableList<Long> LONG_COMPARABLE_LIST = new ComparableList<>(ImmutableList.of(1L, 2L));
  private static final ComparableList<Double> DOUBLE_COMPARABLE_LIST = new ComparableList<>(ImmutableList.of(1.0, 2.0));
  private static final ComparableList<Float> FLOAT_COMPARABLE_LIST = new ComparableList<>(ImmutableList.of(1F, 2F));
  private static final ComparableStringArray COMPARABLE_STRING_ARRAY = ComparableStringArray.of("1", "2");
  private static final ComparableStringArray COMPARABLE_STRING_ARRAY_DECIMAL = ComparableStringArray.of("1.0", "2.0");

  @BeforeClass
  public static void setupTests()
  {
    DimensionHandlerUtils.registerDimensionHandlerProvider(
        TYPE,
        d -> new DoubleDimensionHandler(d)
        {
          @Override
          public DimensionSchema getDimensionSchema(ColumnCapabilities capabilities)
          {
            return new TestDimensionSchema(d, null, capabilities.hasBitmapIndexes());
          }
        }
    );
  }

  @Test
  public void testGetHandlerFromComplexCapabilities()
  {
    ColumnCapabilities capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.ofComplex(TYPE));
    DimensionHandler dimensionHandler = DimensionHandlerUtils.getHandlerFromCapabilities(
        DIM_NAME,
        capabilities,
        null
    );

    Assert.assertEquals(DIM_NAME, dimensionHandler.getDimensionName());
    Assert.assertTrue(dimensionHandler instanceof DoubleDimensionHandler);
    Assert.assertTrue(dimensionHandler.getDimensionSchema(capabilities) instanceof TestDimensionSchema);
  }

  @Test
  public void testGetHandlerFromUnknownComplexCapabilities()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Can't find DimensionHandlerProvider for typeName [unknown]");
    ColumnCapabilities capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.ofComplex("unknown"));
    DimensionHandlerUtils.getHandlerFromCapabilities(
        DIM_NAME,
        capabilities,
        null
    );
  }

  @Test
  public void testGetHandlerFromStringCapabilities()
  {
    ColumnCapabilities stringCapabilities = ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities()
                                                                  .setHasBitmapIndexes(true)
                                                                  .setDictionaryEncoded(true)
                                                                  .setDictionaryValuesUnique(true)
                                                                  .setDictionaryValuesUnique(true);
    DimensionHandler stringHandler = DimensionHandlerUtils.getHandlerFromCapabilities(
        DIM_NAME,
        stringCapabilities,
        DimensionSchema.MultiValueHandling.SORTED_SET
    );
    Assert.assertTrue(stringHandler instanceof StringDimensionHandler);
    Assert.assertTrue(stringHandler.getDimensionSchema(stringCapabilities) instanceof StringDimensionSchema);
  }

  @Test
  public void testGetHandlerFromFloatCapabilities()
  {
    ColumnCapabilities capabilities =
        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.FLOAT);
    DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(
        DIM_NAME,
        capabilities,
        null
    );
    Assert.assertTrue(handler instanceof FloatDimensionHandler);
    Assert.assertTrue(handler.getDimensionSchema(capabilities) instanceof FloatDimensionSchema);
  }

  @Test
  public void testGetHandlerFromDoubleCapabilities()
  {
    ColumnCapabilities capabilities =
        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE);
    DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(
        DIM_NAME,
        capabilities,
        null
    );
    Assert.assertTrue(handler instanceof DoubleDimensionHandler);
    Assert.assertTrue(handler.getDimensionSchema(capabilities) instanceof DoubleDimensionSchema);
  }

  @Test
  public void testGetHandlerFromLongCapabilities()
  {
    ColumnCapabilities capabilities = ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG);
    DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(
        DIM_NAME,
        capabilities,
        null
    );
    Assert.assertTrue(handler instanceof LongDimensionHandler);
    Assert.assertTrue(handler.getDimensionSchema(capabilities) instanceof LongDimensionSchema);
  }

  @Test
  public void testComparableLongList()
  {
    Assert.assertEquals(null, DimensionHandlerUtils.convertToList(null, ValueType.LONG));
    Assert.assertEquals(
        LONG_COMPARABLE_LIST,
        DimensionHandlerUtils.convertToList(ImmutableList.of(1L, 2L), ValueType.LONG)
    );
    Assert.assertEquals(
        LONG_COMPARABLE_LIST,
        DimensionHandlerUtils.convertToList(
            new ComparableList(ImmutableList.of(1L, 2L)),
            ValueType.LONG
        )
    );

    assertArrayCases(LONG_COMPARABLE_LIST, ValueType.LONG);

    Assert.assertThrows(
        "Unable to convert object of type[Long] to [ComparableList]",
        ISE.class,
        () -> DimensionHandlerUtils.convertToList(1L, ValueType.LONG)
    );

    Assert.assertThrows(
        "Unable to convert object of type[Long] to [ComparableList]",
        ISE.class,
        () -> DimensionHandlerUtils.convertToList(1L, ValueType.ARRAY)
    );

    Assert.assertThrows(
        "Unable to convert object of type[Long] to [ComparableList]",
        ISE.class,
        () -> DimensionHandlerUtils.convertToList(1L, ValueType.STRING)
    );
  }

  @Test
  public void testComparableFloatList()
  {
    Assert.assertEquals(null, DimensionHandlerUtils.convertToList(null, ValueType.FLOAT));
    Assert.assertEquals(
        FLOAT_COMPARABLE_LIST,
        DimensionHandlerUtils.convertToList(ImmutableList.of(1.0F, 2.0F), ValueType.FLOAT)
    );
    Assert.assertEquals(
        FLOAT_COMPARABLE_LIST,
        DimensionHandlerUtils.convertToList(
            new ComparableList(ImmutableList.of(1.0F, 2.0F)),
            ValueType.FLOAT
        )
    );

    assertArrayCases(FLOAT_COMPARABLE_LIST, ValueType.FLOAT);

    Assert.assertThrows(
        "Unable to convert object of type[Float] to [ComparableList]",
        ISE.class,
        () -> DimensionHandlerUtils.convertToList(1.0F, ValueType.FLOAT)
    );

    Assert.assertThrows(
        "Unable to convert object of type[Float] to [ComparableList]",
        ISE.class,
        () -> DimensionHandlerUtils.convertToList(1.0F, ValueType.ARRAY)
    );

    Assert.assertThrows(
        "Unable to convert object of type[Float] to [ComparableList]",
        ISE.class,
        () -> DimensionHandlerUtils.convertToList(1.0F, ValueType.STRING)
    );
  }

  @Test
  public void testComparableDoubleList()
  {
    Assert.assertEquals(null, DimensionHandlerUtils.convertToList(null, ValueType.DOUBLE));
    Assert.assertEquals(
        DOUBLE_COMPARABLE_LIST,
        DimensionHandlerUtils.convertToList(ImmutableList.of(1.0D, 2.0D), ValueType.DOUBLE)
    );
    Assert.assertEquals(
        DOUBLE_COMPARABLE_LIST,
        DimensionHandlerUtils.convertToList(
            new ComparableList(ImmutableList.of(1.0D, 2.0D)),
            ValueType.DOUBLE
        )
    );

    assertArrayCases(DOUBLE_COMPARABLE_LIST, ValueType.DOUBLE);

    Assert.assertThrows(
        "Unable to convert object of type[Double] to [ComparableList]",
        ISE.class,
        () -> DimensionHandlerUtils.convertToList(1.0D, ValueType.DOUBLE)
    );

    Assert.assertThrows(
        "Unable to convert object of type[Double] to [ComparableList]",
        ISE.class,
        () -> DimensionHandlerUtils.convertToList(1.0D, ValueType.ARRAY)
    );

    Assert.assertThrows(
        "Unable to convert object of type[Double] to [ComparableList]",
        ISE.class,
        () -> DimensionHandlerUtils.convertToList(1.0D, ValueType.STRING)
    );
  }

  @Test
  public void testComparableStringArrayList()
  {
    Assert.assertEquals(null, DimensionHandlerUtils.convertToComparableStringArray(null));
    Assert.assertEquals(
        COMPARABLE_STRING_ARRAY,
        DimensionHandlerUtils.convertToComparableStringArray(ImmutableList.of("1", "2"))
    );

    Assert.assertEquals(
        COMPARABLE_STRING_ARRAY,
        DimensionHandlerUtils.convertToComparableStringArray(new Object[]{1L, 2L})
    );
    Assert.assertEquals(
        COMPARABLE_STRING_ARRAY,
        DimensionHandlerUtils.convertToComparableStringArray(new Long[]{1L, 2L})
    );
    Assert.assertEquals(
        COMPARABLE_STRING_ARRAY_DECIMAL,
        DimensionHandlerUtils.convertToComparableStringArray(new String[]{"1.0", "2.0"})
    );
    Assert.assertEquals(
        COMPARABLE_STRING_ARRAY_DECIMAL,
        DimensionHandlerUtils.convertToComparableStringArray(new Double[]{1.0, 2.0})
    );
    Assert.assertEquals(
        COMPARABLE_STRING_ARRAY_DECIMAL,
        DimensionHandlerUtils.convertToComparableStringArray(new Float[]{1F, 2F})
    );

    Assert.assertThrows(
        "Unable to convert object of type[String] to [ComparablComparableStringArray]",
        ISE.class,
        () -> DimensionHandlerUtils.convertToComparableStringArray("1")
    );
  }

  private static void assertArrayCases(ComparableList expectedComparableList, ValueType elementType)
  {
    Assert.assertEquals(expectedComparableList, DimensionHandlerUtils.convertToList(new Object[]{1L, 2L}, elementType));
    Assert.assertEquals(expectedComparableList, DimensionHandlerUtils.convertToList(new Long[]{1L, 2L}, elementType));
    Assert.assertEquals(
        expectedComparableList,
        DimensionHandlerUtils.convertToList(new String[]{"1.0", "2.0"}, elementType)
    );
    Assert.assertEquals(
        expectedComparableList,
        DimensionHandlerUtils.convertToList(new Double[]{1.0, 2.0}, elementType)
    );
    Assert.assertEquals(expectedComparableList, DimensionHandlerUtils.convertToList(new Float[]{1F, 2F}, elementType));
  }

  private static class TestDimensionSchema extends DimensionSchema
  {

    protected TestDimensionSchema(
        String name,
        MultiValueHandling multiValueHandling,
        boolean createBitmapIndex
    )
    {
      super(name, multiValueHandling, createBitmapIndex);
    }

    @Override
    public String getTypeName()
    {
      return TYPE;
    }

    @Override
    public ColumnType getColumnType()
    {
      return ColumnType.ofComplex(TYPE);
    }
  }
}

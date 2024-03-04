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
import org.apache.druid.data.input.impl.NewSpatialDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
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

  private static final Object[] LONG_OBJECT_ARRAY = new Object[]{1L, 2L};
  private static final Object[] DOUBLE_OBJECT_ARRAY = new Object[]{1.0, 2.0};
  private static final Object[] FLOAT_OBJECT_ARRAY = new Object[]{1F, 2F};
  private static final Object[] STRING_OBJECT_ARRAY = new Object[]{"1", "2"};
  private static final Object[] DECIMAL_STRING_OBJECT_ARRAY = new Object[]{"1.0", "2.0"};

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
  public void testGetHandlerFromStringCapabilitiesSpatialIndexes()
  {
    ColumnCapabilities stringCapabilities = ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities()
                                                                  .setHasBitmapIndexes(true)
                                                                  .setDictionaryEncoded(true)
                                                                  .setDictionaryValuesUnique(true)
                                                                  .setDictionaryValuesUnique(true)
                                                                  .setHasSpatialIndexes(true);
    DimensionHandler spatialHandler = DimensionHandlerUtils.getHandlerFromCapabilities(
        DIM_NAME,
        stringCapabilities,
        DimensionSchema.MultiValueHandling.SORTED_SET
    );
    Assert.assertTrue(spatialHandler instanceof StringDimensionHandler);
    Assert.assertTrue(spatialHandler.getDimensionSchema(stringCapabilities) instanceof NewSpatialDimensionSchema);
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
    Assert.assertArrayEquals(null, DimensionHandlerUtils.convertToArray(null, ColumnType.LONG));
    Assert.assertArrayEquals(
        LONG_OBJECT_ARRAY,
        DimensionHandlerUtils.convertToArray(ImmutableList.of(1L, 2L), ColumnType.LONG)
    );
    Assert.assertArrayEquals(
        LONG_OBJECT_ARRAY,
        DimensionHandlerUtils.convertToArray(
            FLOAT_OBJECT_ARRAY,
            ColumnType.LONG
        )
    );

    assertArrayCases(LONG_OBJECT_ARRAY, ColumnType.LONG);

    Assert.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1L, ColumnType.LONG)
    );

    Assert.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1L, ColumnType.LONG_ARRAY)
    );

    Assert.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1L, ColumnType.STRING)
    );
  }

  @Test
  public void testComparableFloatList()
  {
    Assert.assertArrayEquals(null, DimensionHandlerUtils.convertToArray(null, ColumnType.FLOAT));
    Assert.assertArrayEquals(
        FLOAT_OBJECT_ARRAY,
        DimensionHandlerUtils.convertToArray(ImmutableList.of(1.0F, 2.0F), ColumnType.FLOAT)
    );
    Assert.assertArrayEquals(
        FLOAT_OBJECT_ARRAY,
        DimensionHandlerUtils.convertToArray(
            LONG_OBJECT_ARRAY,
            ColumnType.FLOAT
        )
    );

    assertArrayCases(FLOAT_OBJECT_ARRAY, ColumnType.FLOAT);

    Assert.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1.0F, ColumnType.FLOAT)
    );

    Assert.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1.0F, ColumnType.FLOAT_ARRAY)
    );

    Assert.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1.0F, ColumnType.STRING)
    );
  }

  @Test
  public void testComparableDoubleList()
  {
    Assert.assertArrayEquals(null, DimensionHandlerUtils.convertToArray(null, ColumnType.DOUBLE));
    Assert.assertArrayEquals(
        DOUBLE_OBJECT_ARRAY,
        DimensionHandlerUtils.convertToArray(ImmutableList.of(1.0D, 2.0D), ColumnType.DOUBLE)
    );
    Assert.assertArrayEquals(
        DOUBLE_OBJECT_ARRAY,
        DimensionHandlerUtils.convertToArray(
            FLOAT_OBJECT_ARRAY,
            ColumnType.DOUBLE
        )
    );

    assertArrayCases(DOUBLE_OBJECT_ARRAY, ColumnType.DOUBLE);

    Assert.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1.0D, ColumnType.DOUBLE)
    );

    Assert.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1.0D, ColumnType.DOUBLE_ARRAY)
    );

    Assert.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1.0D, ColumnType.STRING)
    );
  }

  @Test
  public void testComparableStringArrayList()
  {
    Assert.assertArrayEquals(null, DimensionHandlerUtils.coerceToStringArray(null));
    Assert.assertArrayEquals(
        STRING_OBJECT_ARRAY,
        DimensionHandlerUtils.coerceToStringArray(ImmutableList.of("1", "2"))
    );

    Assert.assertArrayEquals(
        STRING_OBJECT_ARRAY,
        DimensionHandlerUtils.coerceToStringArray(new Object[]{1L, 2L})
    );
    Assert.assertArrayEquals(
        STRING_OBJECT_ARRAY,
        DimensionHandlerUtils.coerceToStringArray(new Long[]{1L, 2L})
    );
    Assert.assertArrayEquals(
        DECIMAL_STRING_OBJECT_ARRAY,
        DimensionHandlerUtils.coerceToStringArray(new String[]{"1.0", "2.0"})
    );
    Assert.assertArrayEquals(
        DECIMAL_STRING_OBJECT_ARRAY,
        DimensionHandlerUtils.coerceToStringArray(new Double[]{1.0, 2.0})
    );
    Assert.assertArrayEquals(
        DECIMAL_STRING_OBJECT_ARRAY,
        DimensionHandlerUtils.coerceToStringArray(new Float[]{1F, 2F})
    );

    Assert.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.coerceToStringArray("1")
    );
  }

  private static void assertArrayCases(Object[] expectedObject, ColumnType elementType)
  {
    Assert.assertArrayEquals(expectedObject, DimensionHandlerUtils.convertToArray(new Object[]{1L, 2L}, elementType));
    Assert.assertArrayEquals(expectedObject, DimensionHandlerUtils.convertToArray(new Long[]{1L, 2L}, elementType));
    Assert.assertArrayEquals(
        expectedObject,
        DimensionHandlerUtils.convertToArray(new String[]{"1.0", "2.0"}, elementType)
    );
    Assert.assertArrayEquals(
        expectedObject,
        DimensionHandlerUtils.convertToArray(new Double[]{1.0, 2.0}, elementType)
    );
    Assert.assertArrayEquals(expectedObject, DimensionHandlerUtils.convertToArray(new Float[]{1F, 2F}, elementType));
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

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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DimensionHandlerUtilsTest extends InitializedNullHandlingTest
{

  private static final String DIM_NAME = "dim";
  private static final String TYPE = "testType";

  private static final Object[] LONG_OBJECT_ARRAY = new Object[]{1L, 2L};
  private static final Object[] DOUBLE_OBJECT_ARRAY = new Object[]{1.0, 2.0};
  private static final Object[] FLOAT_OBJECT_ARRAY = new Object[]{1F, 2F};
  private static final Object[] STRING_OBJECT_ARRAY = new Object[]{"1", "2"};
  private static final Object[] DECIMAL_STRING_OBJECT_ARRAY = new Object[]{"1.0", "2.0"};

  @BeforeAll
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

    Assertions.assertEquals(DIM_NAME, dimensionHandler.getDimensionName());
    Assertions.assertTrue(dimensionHandler instanceof DoubleDimensionHandler);
    Assertions.assertTrue(dimensionHandler.getDimensionSchema(capabilities) instanceof TestDimensionSchema);
  }

  @Test
  public void testGetHandlerFromUnknownComplexCapabilities()
  {
    ColumnCapabilities capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.ofComplex("unknown"));
    ISE ex = Assertions.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.getHandlerFromCapabilities(DIM_NAME, capabilities, null)
    );
    Assertions.assertTrue(ex.getMessage().contains("Can't find DimensionHandlerProvider for typeName [unknown]"));
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
    Assertions.assertTrue(stringHandler instanceof StringDimensionHandler);
    Assertions.assertTrue(stringHandler.getDimensionSchema(stringCapabilities) instanceof StringDimensionSchema);
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
    Assertions.assertTrue(spatialHandler instanceof StringDimensionHandler);
    Assertions.assertTrue(spatialHandler.getDimensionSchema(stringCapabilities) instanceof NewSpatialDimensionSchema);
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
    Assertions.assertTrue(handler instanceof FloatDimensionHandler);
    Assertions.assertTrue(handler.getDimensionSchema(capabilities) instanceof FloatDimensionSchema);
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
    Assertions.assertTrue(handler instanceof DoubleDimensionHandler);
    Assertions.assertTrue(handler.getDimensionSchema(capabilities) instanceof DoubleDimensionSchema);
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
    Assertions.assertTrue(handler instanceof LongDimensionHandler);
    Assertions.assertTrue(handler.getDimensionSchema(capabilities) instanceof LongDimensionSchema);
  }

  @Test
  public void testComparableLongList()
  {
    Assertions.assertArrayEquals(null, DimensionHandlerUtils.convertToArray(null, ColumnType.LONG));
    Assertions.assertArrayEquals(
        LONG_OBJECT_ARRAY,
        DimensionHandlerUtils.convertToArray(ImmutableList.of(1L, 2L), ColumnType.LONG)
    );
    Assertions.assertArrayEquals(
        LONG_OBJECT_ARRAY,
        DimensionHandlerUtils.convertToArray(
            FLOAT_OBJECT_ARRAY,
            ColumnType.LONG
        )
    );

    assertArrayCases(LONG_OBJECT_ARRAY, ColumnType.LONG);

    Assertions.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1L, ColumnType.LONG)
    );

    Assertions.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1L, ColumnType.LONG_ARRAY)
    );

    Assertions.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1L, ColumnType.STRING)
    );
  }

  @Test
  public void testComparableFloatList()
  {
    Assertions.assertArrayEquals(null, DimensionHandlerUtils.convertToArray(null, ColumnType.FLOAT));
    Assertions.assertArrayEquals(
        FLOAT_OBJECT_ARRAY,
        DimensionHandlerUtils.convertToArray(ImmutableList.of(1.0F, 2.0F), ColumnType.FLOAT)
    );
    Assertions.assertArrayEquals(
        FLOAT_OBJECT_ARRAY,
        DimensionHandlerUtils.convertToArray(
            LONG_OBJECT_ARRAY,
            ColumnType.FLOAT
        )
    );

    assertArrayCases(FLOAT_OBJECT_ARRAY, ColumnType.FLOAT);

    Assertions.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1.0F, ColumnType.FLOAT)
    );

    Assertions.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1.0F, ColumnType.FLOAT_ARRAY)
    );

    Assertions.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1.0F, ColumnType.STRING)
    );
  }

  @Test
  public void testComparableDoubleList()
  {
    Assertions.assertArrayEquals(null, DimensionHandlerUtils.convertToArray(null, ColumnType.DOUBLE));
    Assertions.assertArrayEquals(
        DOUBLE_OBJECT_ARRAY,
        DimensionHandlerUtils.convertToArray(ImmutableList.of(1.0D, 2.0D), ColumnType.DOUBLE)
    );
    Assertions.assertArrayEquals(
        DOUBLE_OBJECT_ARRAY,
        DimensionHandlerUtils.convertToArray(
            FLOAT_OBJECT_ARRAY,
            ColumnType.DOUBLE
        )
    );

    assertArrayCases(DOUBLE_OBJECT_ARRAY, ColumnType.DOUBLE);

    Assertions.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1.0D, ColumnType.DOUBLE)
    );

    Assertions.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1.0D, ColumnType.DOUBLE_ARRAY)
    );

    Assertions.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.convertToArray(1.0D, ColumnType.STRING)
    );
  }

  @Test
  public void testComparableStringArrayList()
  {
    Assertions.assertArrayEquals(null, DimensionHandlerUtils.coerceToStringArray(null));
    Assertions.assertArrayEquals(
        STRING_OBJECT_ARRAY,
        DimensionHandlerUtils.coerceToStringArray(ImmutableList.of("1", "2"))
    );

    Assertions.assertArrayEquals(
        STRING_OBJECT_ARRAY,
        DimensionHandlerUtils.coerceToStringArray(new Object[]{1L, 2L})
    );
    Assertions.assertArrayEquals(
        STRING_OBJECT_ARRAY,
        DimensionHandlerUtils.coerceToStringArray(new Long[]{1L, 2L})
    );
    Assertions.assertArrayEquals(
        DECIMAL_STRING_OBJECT_ARRAY,
        DimensionHandlerUtils.coerceToStringArray(new String[]{"1.0", "2.0"})
    );
    Assertions.assertArrayEquals(
        DECIMAL_STRING_OBJECT_ARRAY,
        DimensionHandlerUtils.coerceToStringArray(new Double[]{1.0, 2.0})
    );
    Assertions.assertArrayEquals(
        DECIMAL_STRING_OBJECT_ARRAY,
        DimensionHandlerUtils.coerceToStringArray(new Float[]{1F, 2F})
    );

    Assertions.assertThrows(
        ISE.class,
        () -> DimensionHandlerUtils.coerceToStringArray("1")
    );
  }

  private static void assertArrayCases(Object[] expectedObject, ColumnType elementType)
  {
    Assertions.assertArrayEquals(expectedObject, DimensionHandlerUtils.convertToArray(new Object[]{1L, 2L}, elementType));
    Assertions.assertArrayEquals(expectedObject, DimensionHandlerUtils.convertToArray(new Long[]{1L, 2L}, elementType));
    Assertions.assertArrayEquals(
        expectedObject,
        DimensionHandlerUtils.convertToArray(new String[]{"1.0", "2.0"}, elementType)
    );
    Assertions.assertArrayEquals(
        expectedObject,
        DimensionHandlerUtils.convertToArray(new Double[]{1.0, 2.0}, elementType)
    );
    Assertions.assertArrayEquals(expectedObject, DimensionHandlerUtils.convertToArray(new Float[]{1F, 2F}, elementType));
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

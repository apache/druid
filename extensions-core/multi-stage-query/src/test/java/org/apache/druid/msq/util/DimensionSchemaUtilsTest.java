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

package org.apache.druid.msq.util;

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

public class DimensionSchemaUtilsTest
{

  @Test
  public void testSchemaScalars()
  {
    for (ArrayIngestMode mode : ArrayIngestMode.values()) {
      DimensionSchema dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
          "x",
          ColumnType.LONG,
          false,
          mode
      );
      DimensionSchema expected = new LongDimensionSchema("x");
      Assert.assertEquals(expected, dimensionSchema);

      dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
          "x",
          ColumnType.DOUBLE,
          false,
          mode
      );
      expected = new DoubleDimensionSchema("x");
      Assert.assertEquals(expected, dimensionSchema);

      dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
          "x",
          ColumnType.FLOAT,
          false,
          mode
      );
      expected = new FloatDimensionSchema("x");
      Assert.assertEquals(expected, dimensionSchema);

      dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
          "x",
          ColumnType.STRING,
          false,
          mode
      );
      expected = new StringDimensionSchema("x");
      Assert.assertEquals(expected, dimensionSchema);
    }
  }

  @Test
  public void testSchemaForceAuto()
  {
    for (ArrayIngestMode mode : ArrayIngestMode.values()) {
      DimensionSchema dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
          "x",
          ColumnType.LONG,
          true,
          mode
      );
      DimensionSchema expected = new AutoTypeColumnSchema("x", ColumnType.LONG);
      Assert.assertEquals(expected, dimensionSchema);

      dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
          "x",
          ColumnType.DOUBLE,
          true,
          mode
      );
      expected = new AutoTypeColumnSchema("x", ColumnType.DOUBLE);
      Assert.assertEquals(expected, dimensionSchema);

      dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
          "x",
          ColumnType.FLOAT,
          true,
          mode
      );
      expected = new AutoTypeColumnSchema("x", ColumnType.FLOAT);
      Assert.assertEquals(expected, dimensionSchema);

      dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
          "x",
          ColumnType.STRING,
          true,
          mode
      );
      expected = new AutoTypeColumnSchema("x", ColumnType.STRING);
      Assert.assertEquals(expected, dimensionSchema);


      dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
          "x",
          ColumnType.LONG_ARRAY,
          true,
          mode
      );
      expected = new AutoTypeColumnSchema("x", ColumnType.LONG_ARRAY);
      Assert.assertEquals(expected, dimensionSchema);

      dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
          "x",
          ColumnType.DOUBLE_ARRAY,
          true,
          mode
      );
      expected = new AutoTypeColumnSchema("x", ColumnType.DOUBLE_ARRAY);
      Assert.assertEquals(expected, dimensionSchema);

      dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
          "x",
          ColumnType.FLOAT_ARRAY,
          true,
          mode
      );
      expected = new AutoTypeColumnSchema("x", ColumnType.FLOAT_ARRAY);
      Assert.assertEquals(expected, dimensionSchema);

      dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
          "x",
          ColumnType.STRING_ARRAY,
          true,
          mode
      );
      expected = new AutoTypeColumnSchema("x", ColumnType.STRING_ARRAY);
      Assert.assertEquals(expected, dimensionSchema);

      dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
          "x",
          ColumnType.NESTED_DATA,
          true,
          mode
      );
      expected = new AutoTypeColumnSchema("x", null);
      Assert.assertEquals(expected, dimensionSchema);
    }
  }

  @Test
  public void testSchemaMvdMode()
  {
    DimensionSchema dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
        "x",
        ColumnType.STRING_ARRAY,
        false,
        ArrayIngestMode.MVD
    );
    DimensionSchema expected = new StringDimensionSchema("x", DimensionSchema.MultiValueHandling.ARRAY, null);
    Assert.assertEquals(expected, dimensionSchema);

    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> DimensionSchemaUtils.createDimensionSchema("x", ColumnType.LONG_ARRAY, false, ArrayIngestMode.MVD)
    );
    Assert.assertEquals("Numeric arrays can only be ingested when 'arrayIngestMode' is set to 'array' in the MSQ query's context. Current value of the parameter [mvd]", t.getMessage());

    t = Assert.assertThrows(
        DruidException.class,
        () -> DimensionSchemaUtils.createDimensionSchema("x", ColumnType.DOUBLE_ARRAY, false, ArrayIngestMode.MVD)
    );
    Assert.assertEquals("Numeric arrays can only be ingested when 'arrayIngestMode' is set to 'array' in the MSQ query's context. Current value of the parameter [mvd]", t.getMessage());

    t = Assert.assertThrows(
        DruidException.class,
        () -> DimensionSchemaUtils.createDimensionSchema("x", ColumnType.FLOAT_ARRAY, false, ArrayIngestMode.MVD)
    );
    Assert.assertEquals("Numeric arrays can only be ingested when 'arrayIngestMode' is set to 'array' in the MSQ query's context. Current value of the parameter [mvd]", t.getMessage());
  }

  @Test
  public void testSchemaArrayMode()
  {
    DimensionSchema dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
        "x",
        ColumnType.STRING_ARRAY,
        false,
        ArrayIngestMode.ARRAY
    );
    DimensionSchema expected = new AutoTypeColumnSchema("x", ColumnType.STRING_ARRAY);
    Assert.assertEquals(expected, dimensionSchema);

    dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
        "x",
        ColumnType.LONG_ARRAY,
        false,
        ArrayIngestMode.ARRAY
    );
    expected = new AutoTypeColumnSchema("x", ColumnType.LONG_ARRAY);
    Assert.assertEquals(expected, dimensionSchema);

    dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
        "x",
        ColumnType.DOUBLE_ARRAY,
        false,
        ArrayIngestMode.ARRAY
    );
    expected = new AutoTypeColumnSchema("x", ColumnType.DOUBLE_ARRAY);
    Assert.assertEquals(expected, dimensionSchema);

    dimensionSchema = DimensionSchemaUtils.createDimensionSchema(
        "x",
        ColumnType.FLOAT_ARRAY,
        false,
        ArrayIngestMode.ARRAY
    );
    expected = new AutoTypeColumnSchema("x", ColumnType.FLOAT_ARRAY);
    Assert.assertEquals(expected, dimensionSchema);
  }
}

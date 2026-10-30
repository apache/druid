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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.column.ColumnType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DimensionSchemaTest
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testStringDimensionSchemaSerde() throws Exception
  {
    final StringDimensionSchema schema1 = new StringDimensionSchema("foo");
    Assertions.assertEquals(
        schema1,
        OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(schema1), DimensionSchema.class)
    );

    final StringDimensionSchema schema2 = new StringDimensionSchema(
        "foo",
        DimensionSchema.MultiValueHandling.ARRAY,
        false
    );
    Assertions.assertEquals(
        schema2,
        OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(schema2), DimensionSchema.class)
    );
  }

  @Test
  public void testDeserializeStrictTypeId() throws Exception
  {
    final String invalidType = "{\"type\":\"invalid\",\"name\":\"foo\",\"multiValueHandling\":\"ARRAY\",\"createBitmapIndex\":false}";
    InvalidTypeIdException e = Assertions.assertThrows(
        InvalidTypeIdException.class,
        () -> OBJECT_MAPPER.readValue(invalidType, DimensionSchema.class)
    );
    Assertions.assertTrue(e.getMessage().contains("Could not resolve type id"));
    Assertions.assertTrue(e.getMessage().contains("invalid"));
  }

  @Test
  public void testDeserializeDefaultAsString() throws Exception
  {
    final String noType = "{\"name\":\"foo\",\"multiValueHandling\":\"ARRAY\",\"createBitmapIndex\":false}";
    Assertions.assertEquals(
        new StringDimensionSchema("foo", DimensionSchema.MultiValueHandling.ARRAY, false),
        OBJECT_MAPPER.readValue(noType, DimensionSchema.class)
    );
  }

  @Test
  public void testGetDefaultSchemaForBuiltInType()
  {
    Assertions.assertEquals(
        new StringDimensionSchema("foo"),
        DimensionSchema.getDefaultSchemaForBuiltInType("foo", ColumnType.STRING)
    );
    Assertions.assertEquals(
        new LongDimensionSchema("foo"),
        DimensionSchema.getDefaultSchemaForBuiltInType("foo", ColumnType.LONG)
    );
    Assertions.assertEquals(
        new FloatDimensionSchema("foo"),
        DimensionSchema.getDefaultSchemaForBuiltInType("foo", ColumnType.FLOAT)
    );
    Assertions.assertEquals(
        new DoubleDimensionSchema("foo"),
        DimensionSchema.getDefaultSchemaForBuiltInType("foo", ColumnType.DOUBLE)
    );
    // Primitive arrays cast an auto column to the given type rather than leaving the physical type to be inferred
    // from the ingested values (an all-null batch has no values to infer the array type from). The auto schema
    // stores ARRAY<FLOAT> as ARRAY<DOUBLE>.
    Assertions.assertEquals(
        new AutoTypeColumnSchema("foo", ColumnType.STRING_ARRAY, null),
        DimensionSchema.getDefaultSchemaForBuiltInType("foo", ColumnType.STRING_ARRAY)
    );
    Assertions.assertEquals(
        new AutoTypeColumnSchema("foo", ColumnType.LONG_ARRAY, null),
        DimensionSchema.getDefaultSchemaForBuiltInType("foo", ColumnType.LONG_ARRAY)
    );
    Assertions.assertEquals(
        new AutoTypeColumnSchema("foo", ColumnType.DOUBLE_ARRAY, null),
        DimensionSchema.getDefaultSchemaForBuiltInType("foo", ColumnType.FLOAT_ARRAY)
    );
    // Complex types remain untyped auto columns.
    Assertions.assertEquals(
        AutoTypeColumnSchema.of("foo"),
        DimensionSchema.getDefaultSchemaForBuiltInType("foo", ColumnType.NESTED_DATA)
    );
    Assertions.assertEquals(
        AutoTypeColumnSchema.of("foo"),
        DimensionSchema.getDefaultSchemaForBuiltInType("foo", ColumnType.ofComplex("hyperUnique"))
    );
  }
}

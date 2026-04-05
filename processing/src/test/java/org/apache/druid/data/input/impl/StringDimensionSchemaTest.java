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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import org.apache.druid.guice.DruidSecondaryModule;
import org.apache.druid.guice.GuiceAnnotationIntrospector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.StringColumnFormatSpec;
import org.apache.druid.segment.column.StringBitmapIndexType;
import org.junit.Assert;
import org.junit.Test;

public class StringDimensionSchemaTest
{
  private final ObjectMapper jsonMapper;

  public StringDimensionSchemaTest()
  {
    jsonMapper = new ObjectMapper();
    AnnotationIntrospector introspector = new GuiceAnnotationIntrospector();
    DruidSecondaryModule.setupAnnotationIntrospector(jsonMapper, introspector);
    jsonMapper.registerSubtypes(StringDimensionSchema.class);
  }

  @Test
  public void testDeserializeFromSimpleString() throws JsonProcessingException
  {
    final String json = "\"dim\"";
    final StringDimensionSchema schema = (StringDimensionSchema) jsonMapper.readValue(json, DimensionSchema.class);
    Assert.assertEquals(new StringDimensionSchema("dim"), schema);
  }

  @Test
  public void testDeserializeFromJson() throws JsonProcessingException
  {
    final String json = "{\n"
                        + "  \"name\" : \"dim\",\n"
                        + "  \"multiValueHandling\" : \"SORTED_SET\",\n"
                        + "  \"createBitmapIndex\" : false\n"
                        + "}";
    final StringDimensionSchema schema = (StringDimensionSchema) jsonMapper.readValue(json, DimensionSchema.class);
    Assert.assertEquals(new StringDimensionSchema("dim", MultiValueHandling.SORTED_SET, false), schema);
  }

  @Test
  public void testDeserializeFromJsonWithColumnFormatSpec() throws JsonProcessingException
  {
    final String json = "{\n"
                        + "  \"name\" : \"dim\",\n"
                        + "  \"multiValueHandling\" : \"SORTED_SET\",\n"
                        + "  \"createBitmapIndex\" : false,\n"
                        + "  \"columnFormatSpec\" : { \"maxStringLength\" : 200 }\n"
                        + "}";
    final StringDimensionSchema schema = (StringDimensionSchema) jsonMapper.readValue(json, DimensionSchema.class);
    final StringColumnFormatSpec expectedSpec = StringColumnFormatSpec.builder()
        .setMaxStringLength(200)
        .build();
    Assert.assertEquals(
        new StringDimensionSchema("dim", MultiValueHandling.SORTED_SET, false, expectedSpec),
        schema
    );
    Assert.assertEquals(Integer.valueOf(200), schema.getColumnFormatSpec().getMaxStringLength());
  }

  @Test
  public void testGetEffectiveSchemaResolvesMaxStringLengthFromIndexSpec()
  {
    final StringDimensionSchema schema = new StringDimensionSchema("dim");
    final IndexSpec indexSpec = IndexSpec.builder()
        .withStringColumnFormatSpec(
            StringColumnFormatSpec.builder().setMaxStringLength(50).build()
        )
        .build();

    final StringDimensionSchema effective = (StringDimensionSchema) schema.getEffectiveSchema(indexSpec);

    Assert.assertEquals(Integer.valueOf(50), effective.getColumnFormatSpec().getMaxStringLength());
    Assert.assertEquals("dim", effective.getName());
  }

  @Test
  public void testGetEffectiveSchemaPreservesPerColumnMaxStringLength()
  {
    final StringColumnFormatSpec columnSpec = StringColumnFormatSpec.builder()
        .setMaxStringLength(20)
        .build();
    final StringDimensionSchema schema = new StringDimensionSchema("dim", null, true, columnSpec);
    final IndexSpec indexSpec = IndexSpec.builder()
        .withStringColumnFormatSpec(
            StringColumnFormatSpec.builder().setMaxStringLength(50).build()
        )
        .build();

    final StringDimensionSchema effective = (StringDimensionSchema) schema.getEffectiveSchema(indexSpec);

    // Per-column maxStringLength=20 should not be overridden by job level 50
    Assert.assertEquals(Integer.valueOf(20), effective.getColumnFormatSpec().getMaxStringLength());
  }

  @Test
  public void testGetEffectiveSchemaPreservesCreateBitmapIndex()
  {
    final StringDimensionSchema schema = new StringDimensionSchema("dim", null, false);
    final IndexSpec indexSpec = IndexSpec.builder().build();

    final StringDimensionSchema effective = (StringDimensionSchema) schema.getEffectiveSchema(indexSpec);

    Assert.assertFalse(effective.hasBitmapIndex());
  }

  @Test
  public void testGetEffectiveSchemaPreservesMultiValueHandling()
  {
    final StringDimensionSchema schema = new StringDimensionSchema("dim", MultiValueHandling.ARRAY, true);
    final IndexSpec indexSpec = IndexSpec.builder().build();

    final StringDimensionSchema effective = (StringDimensionSchema) schema.getEffectiveSchema(indexSpec);

    // multiValueHandling=ARRAY must not be overridden by the DEFAULT (SORTED_ARRAY)
    Assert.assertEquals(MultiValueHandling.ARRAY, effective.getMultiValueHandling());
  }

  @Test
  public void testGetEffectiveSchemaNoChangeWithoutStringColumnFormatSpec()
  {
    final StringDimensionSchema schema = new StringDimensionSchema("dim");
    final IndexSpec indexSpec = IndexSpec.builder().build();

    final StringDimensionSchema effective = (StringDimensionSchema) schema.getEffectiveSchema(indexSpec);

    // With no stringColumnFormatSpec, should return same object
    Assert.assertSame(schema, effective);
    Assert.assertEquals(schema.hasBitmapIndex(), effective.hasBitmapIndex());
    Assert.assertEquals(schema.getMultiValueHandling(), effective.getMultiValueHandling());
  }

  @Test
  public void testGetEffectiveSchemaResolvesIndexTypeFromIndexSpec()
  {
    final StringDimensionSchema schema = new StringDimensionSchema("dim");
    final IndexSpec indexSpec = IndexSpec.builder()
        .withStringColumnFormatSpec(
            StringColumnFormatSpec.builder()
                .setIndexType(StringBitmapIndexType.NoIndex.INSTANCE)
                .build()
        )
        .build();

    final StringDimensionSchema effective = (StringDimensionSchema) schema.getEffectiveSchema(indexSpec);

    Assert.assertEquals(
        StringBitmapIndexType.NoIndex.INSTANCE,
        effective.getColumnFormatSpec().getIndexType()
    );
  }
}

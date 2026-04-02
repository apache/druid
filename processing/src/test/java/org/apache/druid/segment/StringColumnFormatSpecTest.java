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

import com.fasterxml.jackson.core.JsonProcessingException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import org.junit.Assert;
import org.junit.Test;

public class StringColumnFormatSpecTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    StringColumnFormatSpec spec = StringColumnFormatSpec.builder()
        .setCreateBitmapIndex(false)
        .setMultiValueHandling(MultiValueHandling.SORTED_SET)
        .setMaxStringLength(100)
        .build();

    StringColumnFormatSpec roundTripped = TestHelper.JSON_MAPPER.readValue(
        TestHelper.JSON_MAPPER.writeValueAsString(spec),
        StringColumnFormatSpec.class
    );
    Assert.assertEquals(spec, roundTripped);
  }

  @Test
  public void testSerdeNullFields() throws JsonProcessingException
  {
    StringColumnFormatSpec spec = StringColumnFormatSpec.builder().build();

    String json = TestHelper.JSON_MAPPER.writeValueAsString(spec);
    Assert.assertEquals("{}", json);

    StringColumnFormatSpec roundTripped = TestHelper.JSON_MAPPER.readValue(json, StringColumnFormatSpec.class);
    Assert.assertEquals(spec, roundTripped);
  }

  @Test
  public void testGetEffectiveFormatSpecDefaults()
  {
    StringColumnFormatSpec effective = StringColumnFormatSpec.getEffectiveFormatSpec(
        null,
        IndexSpec.builder().build()
    );

    Assert.assertEquals(Boolean.TRUE, effective.getCreateBitmapIndex());
    Assert.assertEquals(MultiValueHandling.SORTED_ARRAY, effective.getMultiValueHandling());
    Assert.assertNull(effective.getMaxStringLength());
  }

  @Test
  public void testGetEffectiveFormatSpecJobLevelOverride()
  {
    IndexSpec indexSpec = IndexSpec.builder()
        .withStringColumnFormatSpec(
            StringColumnFormatSpec.builder()
                .setMaxStringLength(50)
                .build()
        )
        .build();

    StringColumnFormatSpec effective = StringColumnFormatSpec.getEffectiveFormatSpec(null, indexSpec);

    Assert.assertEquals(Boolean.TRUE, effective.getCreateBitmapIndex());
    Assert.assertEquals(MultiValueHandling.SORTED_ARRAY, effective.getMultiValueHandling());
    Assert.assertEquals(Integer.valueOf(50), effective.getMaxStringLength());
  }

  @Test
  public void testGetEffectiveFormatSpecColumnOverridesJobLevel()
  {
    StringColumnFormatSpec columnSpec = StringColumnFormatSpec.builder()
        .setMaxStringLength(20)
        .build();

    IndexSpec indexSpec = IndexSpec.builder()
        .withStringColumnFormatSpec(
            StringColumnFormatSpec.builder()
                .setMaxStringLength(50)
                .build()
        )
        .build();

    StringColumnFormatSpec effective = StringColumnFormatSpec.getEffectiveFormatSpec(columnSpec, indexSpec);

    Assert.assertEquals(Integer.valueOf(20), effective.getMaxStringLength());
  }

  @Test
  public void testGetEffectiveFormatSpecColumnFallsBackToJobLevel()
  {
    StringColumnFormatSpec columnSpec = StringColumnFormatSpec.builder()
        .setCreateBitmapIndex(false)
        .build();

    IndexSpec indexSpec = IndexSpec.builder()
        .withStringColumnFormatSpec(
            StringColumnFormatSpec.builder()
                .setMaxStringLength(50)
                .setMultiValueHandling(MultiValueHandling.ARRAY)
                .build()
        )
        .build();

    StringColumnFormatSpec effective = StringColumnFormatSpec.getEffectiveFormatSpec(columnSpec, indexSpec);

    Assert.assertEquals(Boolean.FALSE, effective.getCreateBitmapIndex());
    Assert.assertEquals(MultiValueHandling.ARRAY, effective.getMultiValueHandling());
    Assert.assertEquals(Integer.valueOf(50), effective.getMaxStringLength());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(StringColumnFormatSpec.class).usingGetClass().verify();
  }
}

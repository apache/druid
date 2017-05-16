/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.data.input.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.druid.TestObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class DelimitedParseSpecTest
{
  private final ObjectMapper jsonMapper = new TestObjectMapper();

  @Test
  public void testSerde() throws IOException
  {
    DelimitedParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec("abc", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Collections.singletonList("abc")), null, null),
        "\u0001",
        "\u0002",
        Collections.singletonList("abc"),
        false,
        0
    );
    final DelimitedParseSpec serde = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        DelimitedParseSpec.class
    );
    Assert.assertEquals("abc", serde.getTimestampSpec().getTimestampColumn());
    Assert.assertEquals("iso", serde.getTimestampSpec().getTimestampFormat());

    Assert.assertEquals(Collections.singletonList("abc"), serde.getColumns());
    Assert.assertEquals("\u0001", serde.getDelimiter());
    Assert.assertEquals("\u0002", serde.getListDelimiter());
    Assert.assertEquals(Collections.singletonList("abc"), serde.getDimensionsSpec().getDimensionNames());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testColumnMissing() throws Exception
  {
    final ParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("a", "b")),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        " ",
        Collections.singletonList("a"),
        false,
        0
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testComma() throws Exception
  {
    final ParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("a,", "b")),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        null,
        Collections.singletonList("a"),
        false,
        0
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDefaultColumnList()
  {
    final DelimitedParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("a", "b")),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        null,
        null,
        false,
        0
    );
  }
}

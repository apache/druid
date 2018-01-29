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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.TestObjectMapper;
import io.druid.java.util.common.parsers.DelimitedParser;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
        null,
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
    @SuppressWarnings("unused") // expected exception
    final ParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("a", "b")),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        " ",
        null,
        Collections.singletonList("a"),
        false,
        0
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testComma() throws Exception
  {
    @SuppressWarnings("unused") // expected exception
    final ParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("a,", "b")),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        null,
        null,
        Collections.singletonList("a"),
        false,
        0
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDefaultColumnList()
  {
    @SuppressWarnings("unused") // expected exception
    final DelimitedParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("a", "b")),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        null,
        null,
        null,
        false,
        0
    );
  }

  @Test
  public void testDefaultListDelimiter()
  {
    final DelimitedParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("col2")),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        null,
        null,
        Arrays.asList("col1", "col2", "col3")
    );

    DelimitedParser parser = (DelimitedParser) spec.makeParser();
    Map<String, Object> map = parser.parseToMap("1,2,x\u0001y\u0001z");
    Assert.assertEquals(ImmutableMap.of("col1", "1", "col2", "2", "col3", Arrays.asList("x", "y", "z")), map);
  }

  @Test
  public void testDisableListDelimiter()
  {
    final DelimitedParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("col2")),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        null,
        new HashMap<>(),
        Arrays.asList("col1", "col2", "col3")
    );
    DelimitedParser parser = (DelimitedParser) spec.makeParser();
    Map<String, Object> map = parser.parseToMap("1,2,x\u0001y\u0001z");
    Assert.assertEquals(ImmutableMap.of("col1", "1", "col2", "2", "col3", "x\u0001y\u0001z"), map);
  }

  @Test
  public void testMultiValueDelimiter()
  {
    Map<String, String> multiValueDelimiter = new HashMap<>();
    multiValueDelimiter.put("col3", "\u0002");
    multiValueDelimiter.put("col4", "\u0003");

    final DelimitedParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("col2")),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        null,
        multiValueDelimiter,
        Arrays.asList("col1", "col2", "col3", "col4", "col5")
    );

    DelimitedParser parser = (DelimitedParser) spec.makeParser();
    Map<String, Object> map = parser.parseToMap("1,2,x\u0002y\u0002z,a\u0003b\u0003c,e\u0002f\u0002g");
    Assert.assertEquals(
        ImmutableMap.of(
           "col1", "1",
           "col2", "2",
           "col3", Arrays.asList("x", "y", "z"),
           "col4", Arrays.asList("a", "b", "c"),
           "col5", "e\u0002f\u0002g"
        ),
        map
    );
  }
}

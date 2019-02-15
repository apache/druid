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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class ParseSpecTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private ObjectMapper mapper;

  @Before
  public void setUp()
  {
    // Similar to configs from DefaultObjectMapper, which we cannot use here since we're in druid-api.
    mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(MapperFeature.AUTO_DETECT_GETTERS, false);
    mapper.configure(MapperFeature.AUTO_DETECT_FIELDS, false);
    mapper.configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
    mapper.configure(MapperFeature.AUTO_DETECT_SETTERS, false);
    mapper.configure(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, false);
  }

  @Test(expected = ParseException.class)
  public void testDuplicateNames()
  {
    @SuppressWarnings("unused") // expected exception
    final ParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("a", "b", "a")),
            new ArrayList<>(),
            new ArrayList<>()
        ),
        ",",
        " ",
        Arrays.asList("a", "b"),
        false,
        0
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDimAndDimExcluOverlap()
  {
    @SuppressWarnings("unused") // expected exception
    final ParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("a", "B")),
            Collections.singletonList("B"),
            new ArrayList<>()
        ),
        ",",
        null,
        Arrays.asList("a", "B"),
        false,
        0
    );
  }

  @Test
  public void testDimExclusionDuplicate()
  {
    @SuppressWarnings("unused") // expected exception
    final ParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Collections.singletonList("a")),
            Lists.newArrayList("B", "B"),
            new ArrayList<>()
        ),
        ",",
        null,
        Arrays.asList("a", "B"),
        false,
        0
    );
  }

  @Test
  public void testDefaultTimestampSpec()
  {
    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("parseSpec requires timestampSpec");
    @SuppressWarnings("unused") // expected exception
    final ParseSpec spec = new DelimitedParseSpec(
        null,
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Collections.singletonList("a")),
            Lists.newArrayList("B", "B"),
            new ArrayList<>()
        ),
        ",",
        null,
        Arrays.asList("a", "B"),
        false,
        0
    );
  }

  @Test
  public void testDimensionSpecRequired()
  {
    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("parseSpec requires dimensionSpec");
    @SuppressWarnings("unused") // expected exception
    final ParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        null,
        ",",
        null,
        Arrays.asList("a", "B"),
        false,
        0
    );
  }

  @Test
  public void testSerde() throws IOException
  {
    final String json = "{"
                        + "\"format\":\"timeAndDims\", "
                        + "\"timestampSpec\": {\"column\":\"timestamp\"}, "
                        + "\"dimensionsSpec\":{}"
                        + "}";

    final Object mapValue = mapper.readValue(json, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
    final ParseSpec parseSpec = mapper.convertValue(mapValue, ParseSpec.class);

    Assert.assertEquals(TimeAndDimsParseSpec.class, parseSpec.getClass());
    Assert.assertEquals("timestamp", parseSpec.getTimestampSpec().getTimestampColumn());
    Assert.assertEquals(ImmutableList.of(), parseSpec.getDimensionsSpec().getDimensionNames());

    // Test round-trip.
    Assert.assertEquals(
        parseSpec,
        mapper.readValue(mapper.writeValueAsString(parseSpec), ParseSpec.class)
    );
  }

  @Test
  public void testBadTypeSerde() throws IOException
  {
    final String json = "{"
                        + "\"format\":\"foo\", "
                        + "\"timestampSpec\": {\"column\":\"timestamp\"}, "
                        + "\"dimensionsSpec\":{}"
                        + "}";

    final Object mapValue = mapper.readValue(json, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(JsonMappingException.class));
    expectedException.expectMessage("Could not resolve type id 'foo' into a subtype");
    mapper.convertValue(mapValue, ParseSpec.class);
  }
}

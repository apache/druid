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

package io.druid.query.dimension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 */
public class ListFilteredDimensionSpecTest
{

  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = TestHelper.getObjectMapper();

    //isWhitelist = true
    String jsonStr = "{\n"
                     + "  \"type\": \"listFiltered\",\n"
                     + "  \"delegate\": {\n"
                     + "    \"type\": \"default\",\n"
                     + "    \"dimension\": \"foo\",\n"
                     + "    \"outputName\": \"bar\"\n"
                     + "  },\n"
                     + "  \"values\": [\"xxx\"]\n"
                     + "}";

    ListFilteredDimensionSpec actual = (ListFilteredDimensionSpec) mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(jsonStr, DimensionSpec.class)),
        DimensionSpec.class);

    ListFilteredDimensionSpec expected = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableList.of("xxx"),
        true
    );

    Assert.assertEquals(expected, actual);

    //isWhitelist = false
    jsonStr = "{\n"
              + "  \"type\": \"listFiltered\",\n"
              + "  \"delegate\": {\n"
              + "    \"type\": \"default\",\n"
              + "    \"dimension\": \"foo\",\n"
              + "    \"outputName\": \"bar\"\n"
              + "  },\n"
              + "  \"values\": [\"xxx\"],\n"
              + "  \"isWhitelist\": false\n"
              + "}";

    actual = (ListFilteredDimensionSpec) mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(jsonStr, DimensionSpec.class)),
        DimensionSpec.class);

    expected = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableList.of("xxx"),
        false
    );

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetCacheKey()
  {
    ListFilteredDimensionSpec spec1 = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableList.of("xxx"),
        null
    );

    ListFilteredDimensionSpec spec2 = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableList.of("xyz"),
        null
    );

    Assert.assertFalse(Arrays.equals(spec1.getCacheKey(), spec2.getCacheKey()));

    ListFilteredDimensionSpec spec3 = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableList.of("xxx"),
        false
    );

    Assert.assertFalse(Arrays.equals(spec1.getCacheKey(), spec3.getCacheKey()));
  }
}

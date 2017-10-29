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
import io.druid.segment.DimensionSelector;
import io.druid.segment.TestHelper;
import io.druid.segment.data.IndexedInts;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 */
public class RegexFilteredDimensionSpecTest
{

  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = TestHelper.getJsonMapper();

    String jsonStr = "{\n"
                     + "  \"type\": \"regexFiltered\",\n"
                     + "  \"delegate\": {\n"
                     + "    \"type\": \"default\",\n"
                     + "    \"dimension\": \"foo\",\n"
                     + "    \"outputName\": \"bar\"\n"
                     + "  },\n"
                     + "  \"pattern\": \"xxx\"\n"
                     + "}";

    RegexFilteredDimensionSpec actual = (RegexFilteredDimensionSpec) mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(jsonStr, DimensionSpec.class)),
        DimensionSpec.class);

    RegexFilteredDimensionSpec expected = new RegexFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        "xxx"
    );

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetCacheKey()
  {
    RegexFilteredDimensionSpec spec1 = new RegexFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        "xxx"
    );

    RegexFilteredDimensionSpec spec2 = new RegexFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        "xyz"
    );

    Assert.assertFalse(Arrays.equals(spec1.getCacheKey(), spec2.getCacheKey()));
  }

  @Test
  public void testDecorator()
  {
    RegexFilteredDimensionSpec spec = new RegexFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        "[c,g]"
    );

    DimensionSelector selector = spec.decorate(TestDimensionSelector.instance);

    Assert.assertEquals(2, selector.getValueCardinality());

    IndexedInts row = selector.getRow();
    Assert.assertEquals(2, row.size());
    Assert.assertEquals(0, row.get(0));
    Assert.assertEquals(1, row.get(1));

    Assert.assertEquals("c", selector.lookupName(0));
    Assert.assertEquals("g", selector.lookupName(1));

    Assert.assertEquals(0, selector.idLookup().lookupId("c"));
    Assert.assertEquals(1, selector.idLookup().lookupId("g"));
  }
}

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
import com.google.common.collect.ImmutableSet;
import io.druid.segment.DimensionSelector;
import io.druid.segment.TestHelper;
import io.druid.segment.data.IndexedInts;
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
    ObjectMapper mapper = TestHelper.getJsonMapper();

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
        ImmutableSet.of("xxx"),
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
        ImmutableSet.of("xxx"),
        false
    );

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetCacheKey()
  {
    ListFilteredDimensionSpec spec1 = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableSet.of("xxx"),
        null
    );

    ListFilteredDimensionSpec spec2 = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableSet.of("xyz"),
        null
    );

    Assert.assertFalse(Arrays.equals(spec1.getCacheKey(), spec2.getCacheKey()));

    ListFilteredDimensionSpec spec3 = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableSet.of("xxx"),
        false
    );

    Assert.assertFalse(Arrays.equals(spec1.getCacheKey(), spec3.getCacheKey()));
  }

  @Test
  public void testDecoratorWithWhitelist()
  {
    ListFilteredDimensionSpec spec = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableSet.of("c", "g"),
        true
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

  @Test
  public void testDecoratorWithBlacklist()
  {
    ListFilteredDimensionSpec spec = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableSet.of("c", "g"),
        false
    );

    DimensionSelector selector = spec.decorate(TestDimensionSelector.instance);

    Assert.assertEquals(24, selector.getValueCardinality());

    IndexedInts row = selector.getRow();
    Assert.assertEquals(1, row.size());
    Assert.assertEquals(3, row.get(0));

    Assert.assertEquals("a", selector.lookupName(0));
    Assert.assertEquals("z", selector.lookupName(23));

    Assert.assertEquals(0, selector.idLookup().lookupId("a"));
    Assert.assertEquals(23, selector.idLookup().lookupId("z"));
  }

  @Test
  public void testDecoratorWithBlacklistUsingNonPresentValues()
  {
    ListFilteredDimensionSpec spec = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableSet.of("c", "gx"),
        false
    );

    DimensionSelector selector = spec.decorate(TestDimensionSelector.instance);

    Assert.assertEquals(25, selector.getValueCardinality());

    IndexedInts row = selector.getRow();
    Assert.assertEquals(2, row.size());
    Assert.assertEquals(3, row.get(0));
    Assert.assertEquals(5, row.get(1));

    Assert.assertEquals("e", selector.lookupName(row.get(0)));
    Assert.assertEquals("g", selector.lookupName(row.get(1)));

    Assert.assertEquals("a", selector.lookupName(0));
    Assert.assertEquals("z", selector.lookupName(24));

    Assert.assertEquals(0, selector.idLookup().lookupId("a"));
    Assert.assertEquals(24, selector.idLookup().lookupId("z"));
  }
}

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

package org.apache.druid.query.dimension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.data.IndexedInts;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 */
public class ListFilteredDimensionSpecTest
{

  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = TestHelper.makeJsonMapper();

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

    Assertions.assertEquals(expected, actual);

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

    Assertions.assertEquals(expected, actual);
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

    Assertions.assertFalse(Arrays.equals(spec1.getCacheKey(), spec2.getCacheKey()));

    ListFilteredDimensionSpec spec3 = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableSet.of("xxx"),
        false
    );

    ListFilteredDimensionSpec spec4 = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableSet.of("xyz"),
        true
    );

    Assertions.assertFalse(Arrays.equals(spec1.getCacheKey(), spec2.getCacheKey()));
    Assertions.assertFalse(Arrays.equals(spec1.getCacheKey(), spec3.getCacheKey()));
    Assertions.assertFalse(Arrays.equals(spec1.getCacheKey(), spec4.getCacheKey()));
    Assertions.assertFalse(Arrays.equals(spec2.getCacheKey(), spec3.getCacheKey()));
    Assertions.assertArrayEquals(spec2.getCacheKey(), spec4.getCacheKey());
    Assertions.assertFalse(Arrays.equals(spec3.getCacheKey(), spec4.getCacheKey()));
  }

  @Test
  public void testDecoratorWithWhitelist()
  {
    ListFilteredDimensionSpec spec = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableSet.of("c", "g"),
        true
    );

    DimensionSelector selector = spec.decorate(TestDimensionSelector.INSTANCE);

    Assertions.assertEquals(2, selector.getValueCardinality());

    IndexedInts row = selector.getRow();
    Assertions.assertEquals(2, row.size());
    Assertions.assertEquals(0, row.get(0));
    Assertions.assertEquals(1, row.get(1));

    Assertions.assertEquals("c", selector.lookupName(0));
    Assertions.assertEquals("g", selector.lookupName(1));

    Assertions.assertEquals(0, selector.idLookup().lookupId("c"));
    Assertions.assertEquals(1, selector.idLookup().lookupId("g"));
  }

  @Test
  public void testDecoratorWithBlacklist()
  {
    ListFilteredDimensionSpec spec = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableSet.of("c", "g"),
        false
    );

    DimensionSelector selector = spec.decorate(TestDimensionSelector.INSTANCE);

    Assertions.assertEquals(24, selector.getValueCardinality());

    IndexedInts row = selector.getRow();
    Assertions.assertEquals(1, row.size());
    Assertions.assertEquals(3, row.get(0));

    Assertions.assertEquals("a", selector.lookupName(0));
    Assertions.assertEquals("z", selector.lookupName(23));

    Assertions.assertEquals(0, selector.idLookup().lookupId("a"));
    Assertions.assertEquals(23, selector.idLookup().lookupId("z"));
  }

  @Test
  public void testDecoratorWithBlacklistUsingNonPresentValues()
  {
    ListFilteredDimensionSpec spec = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        ImmutableSet.of("c", "gx"),
        false
    );

    DimensionSelector selector = spec.decorate(TestDimensionSelector.INSTANCE);

    Assertions.assertEquals(25, selector.getValueCardinality());

    IndexedInts row = selector.getRow();
    Assertions.assertEquals(2, row.size());
    Assertions.assertEquals(3, row.get(0));
    Assertions.assertEquals(5, row.get(1));

    Assertions.assertEquals("e", selector.lookupName(row.get(0)));
    Assertions.assertEquals("g", selector.lookupName(row.get(1)));

    Assertions.assertEquals("a", selector.lookupName(0));
    Assertions.assertEquals("z", selector.lookupName(24));

    Assertions.assertEquals(0, selector.idLookup().lookupId("a"));
    Assertions.assertEquals(24, selector.idLookup().lookupId("z"));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ListFilteredDimensionSpec.class).withNonnullFields("values").usingGetClass().verify();
  }

  @Test
  public void testListFilteredDimensionSpecWithNullValue() throws Exception
  {
    ObjectMapper mapper = TestHelper.makeJsonMapper();

    String jsonStr = "{\n"
                     + "  \"type\": \"listFiltered\",\n"
                     + "  \"delegate\": {\n"
                     + "    \"type\": \"default\",\n"
                     + "    \"dimension\": \"foo\",\n"
                     + "    \"outputName\": \"bar\"\n"
                     + "  },\n"
                     + "  \"values\": [null, \"A\", \"B\"]\n"
                     + "}";

    ListFilteredDimensionSpec actual = (ListFilteredDimensionSpec) mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(jsonStr, DimensionSpec.class)),
        DimensionSpec.class
    );

    Set<String> expectedValues = new HashSet<>(Arrays.asList(null, "A", "B"));
    ListFilteredDimensionSpec expected = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        expectedValues,
        true
    );

    Assertions.assertEquals(expected, actual);
    Assertions.assertArrayEquals(expected.getCacheKey(), actual.getCacheKey());
  }

  @Test
  public void testListFilteredDimensionSpecValueOrderIsIgnored()
  {
    ListFilteredDimensionSpec spec1 = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        new HashSet<>(Arrays.asList(null, "A", "B")),
        true
    );

    ListFilteredDimensionSpec spec2 = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        new HashSet<>(Arrays.asList("A", "B", null)),
        true
    );

    Assertions.assertArrayEquals(spec1.getCacheKey(), spec2.getCacheKey());
  }
}

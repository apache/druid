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

package org.apache.druid.catalog.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.CatalogTest;
import org.apache.druid.catalog.model.ModelProperties.BooleanPropertyDefn;
import org.apache.druid.catalog.model.ModelProperties.IntPropertyDefn;
import org.apache.druid.catalog.model.ModelProperties.ListPropertyDefn;
import org.apache.druid.catalog.model.ModelProperties.StringListPropertyDefn;
import org.apache.druid.catalog.model.ModelProperties.StringPropertyDefn;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category(CatalogTest.class)
public class PropertyDefnTest
{
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testString()
  {
    StringPropertyDefn prop = new StringPropertyDefn("prop");
    assertEquals("prop", prop.name());
    assertEquals("String", prop.typeName());

    assertNull(prop.decode(null, mapper));
    assertEquals("value", prop.decode("value", mapper));
    prop.validate("value", mapper);

    // Jackson is permissive in its conversions
    assertEquals("10", prop.decode(10, mapper));
    prop.validate(10, mapper);

    // But, it does have its limits.
    assertThrows(Exception.class, () -> prop.decode(Arrays.asList("a", "b"), mapper));
    assertThrows(Exception.class, () -> prop.validate(Arrays.asList("a", "b"), mapper));
  }

  @Test
  public void testBoolean()
  {
    BooleanPropertyDefn prop = new BooleanPropertyDefn("prop");
    assertEquals("prop", prop.name());
    assertEquals("Boolean", prop.typeName());

    assertNull(prop.decode(null, mapper));
    assertTrue(prop.decode("true", mapper));
    assertTrue(prop.decode(true, mapper));
    assertFalse(prop.decode("false", mapper));
    assertFalse(prop.decode(false, mapper));
    assertFalse(prop.decode(0, mapper));
    assertTrue(prop.decode(10, mapper));
  }

  @Test
  public void testInt()
  {
    IntPropertyDefn prop = new IntPropertyDefn("prop");
    assertEquals("prop", prop.name());
    assertEquals("Integer", prop.typeName());

    assertNull(prop.decode(null, mapper));
    assertEquals((Integer) 0, prop.decode(0, mapper));
    assertEquals((Integer) 0, prop.decode("0", mapper));
    assertEquals((Integer) 10, prop.decode(10, mapper));
    assertEquals((Integer) 10, prop.decode("10", mapper));
    assertThrows(Exception.class, () -> prop.decode("foo", mapper));
  }

  @Test
  public void testStringList()
  {
    StringListPropertyDefn prop = new StringListPropertyDefn("prop");
    assertEquals("prop", prop.name());
    assertEquals("string list", prop.typeName());

    assertNull(prop.decode(null, mapper));
    prop.validate(null, mapper);
    List<String> value = Arrays.asList("a", "b");
    assertEquals(value, prop.decode(value, mapper));
    prop.validate(value, mapper);
    assertThrows(Exception.class, () -> prop.decode("foo", mapper));
    assertThrows(Exception.class, () -> prop.validate("foo", mapper));
  }

  @Test
  public void testClusterKeyList()
  {
    ListPropertyDefn<ClusterKeySpec> prop = new ListPropertyDefn<ClusterKeySpec>(
        "prop",
        "cluster key list",
        new TypeReference<List<ClusterKeySpec>>() { }
    );
    assertEquals("prop", prop.name());
    assertEquals("cluster key list", prop.typeName());

    assertNull(prop.decode(null, mapper));
    List<Map<String, Object>> value = Arrays.asList(
        ImmutableMap.of("column", "a"),
        ImmutableMap.of("column", "b", "desc", true)
    );
    List<ClusterKeySpec> expected = Arrays.asList(
        new ClusterKeySpec("a", false),
        new ClusterKeySpec("b", true)
    );
    assertEquals(expected, prop.decode(value, mapper));
    assertThrows(Exception.class, () -> prop.decode("foo", mapper));
  }
}

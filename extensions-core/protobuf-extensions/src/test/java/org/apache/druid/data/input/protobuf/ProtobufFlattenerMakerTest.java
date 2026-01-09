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

package org.apache.druid.data.input.protobuf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProtobufFlattenerMakerTest
{
  private static final ProtobufFlattenerMaker FLATTENER_MAKER_WITH_DISCOVERY =
      new ProtobufFlattenerMaker(true);
  private static final ProtobufFlattenerMaker FLATTENER_MAKER_WITHOUT_DISCOVERY =
      new ProtobufFlattenerMaker(false);

  @Test
  public void testGetRootFieldPrimitiveTypes()
  {
    final Map<String, Object> testData = ImmutableMap.of(
        "stringField", "test string",
        "intField", 42,
        "boolField", true
    );

    assertEquals("test string", FLATTENER_MAKER_WITH_DISCOVERY.getRootField(testData, "stringField"));
    assertEquals(42, FLATTENER_MAKER_WITH_DISCOVERY.getRootField(testData, "intField"));
    assertEquals(true, FLATTENER_MAKER_WITH_DISCOVERY.getRootField(testData, "boolField"));
    assertNull(FLATTENER_MAKER_WITH_DISCOVERY.getRootField(testData, "nonExistentField"));
  }

  @Test
  public void testGetRootFieldNestedTypes()
  {
    final Map<String, Object> nestedData = ImmutableMap.of(
        "nested", ImmutableMap.of("subField", "nested value"),
        "arrayField", ImmutableList.of("item1", "item2")
    );

    final var result = (Map<?, ?>) FLATTENER_MAKER_WITH_DISCOVERY.getRootField(nestedData, "nested");
    assertEquals("nested value", result.get("subField"));

    final var arrayResult = (List<?>) FLATTENER_MAKER_WITH_DISCOVERY.getRootField(nestedData, "arrayField");
    assertEquals(2, arrayResult.size());
    assertEquals("item1", arrayResult.get(0));
  }

  @Test
  public void testDiscoverRootFieldsWithDiscovery()
  {
    final Map<String, Object> testData = ImmutableMap.of(
        "stringField", "test",
        "intField", 42,
        "nestedField", ImmutableMap.of("subField", "value"),
        "arrayField", ImmutableList.of("item1", "item2")
    );

    final Iterable<String> discoveredFields = FLATTENER_MAKER_WITH_DISCOVERY.discoverRootFields(testData);

    // With discovery enabled, should return ALL fields including complex ones
    assertIterableEquals(
        testData.keySet(),
        discoveredFields
    );
  }

  @Test
  public void testDiscoverRootFieldsWithoutDiscovery()
  {
    final Map<String, Object> testData = ImmutableMap.of(
        "stringField", "test",
        "intField", 42,
        "nestedField", ImmutableMap.of("subField", "value"),
        "arrayField", ImmutableList.of("item1", "item2")
    );

    final Iterable<String> discoveredFields = FLATTENER_MAKER_WITHOUT_DISCOVERY.discoverRootFields(testData);

    // Without discovery, should only return primitive fields
    final List<String> fieldList = Lists.newArrayList(discoveredFields);
    assertEquals(2, fieldList.size());
    assertTrue(fieldList.contains("stringField"));
    assertTrue(fieldList.contains("intField"));
  }

  @Test
  public void testMakeJsonPathExtractorSimplePath()
  {
    final Map<String, Object> testData = ImmutableMap.of(
        "user", ImmutableMap.of(
            "name", "john",
            "age", 30
        )
    );

    final Function<Map<String, Object>, Object> nameExtractor =
        FLATTENER_MAKER_WITH_DISCOVERY.makeJsonPathExtractor("$.user.name");
    final Function<Map<String, Object>, Object> ageExtractor =
        FLATTENER_MAKER_WITH_DISCOVERY.makeJsonPathExtractor("$.user.age");

    assertEquals("john", nameExtractor.apply(testData));
    assertEquals(30, ageExtractor.apply(testData));
  }

  @Test
  public void testFlattenWithProtobufConvertedData() throws Exception
  {
    final var nestedStruct = Struct.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge("{\"something\":{\"a\":\"hello\",\"b\":2}}", nestedStruct);

    final Struct struct =
        Struct.newBuilder()
              .putFields("stringField", Value.newBuilder().setStringValue("test").build())
              .putFields("numberField", Value.newBuilder().setNumberValue(123.45).build())
              .putFields("boolField", Value.newBuilder().setBoolValue(true).build())
              .putFields("structField", Value.newBuilder().setStructValue(nestedStruct).build())
              .build();

    final Map<String, Object> convertedData = ProtobufConverter.convertMessage(struct);
    assertNotNull(convertedData);

    // Test field discovery
    final Iterable<String> discoveredFields = FLATTENER_MAKER_WITH_DISCOVERY.discoverRootFields(convertedData);
    final List<String> fieldList = Lists.newArrayList(discoveredFields);
    assertTrue(fieldList.contains("stringField"));
    assertTrue(fieldList.contains("numberField"));
    assertTrue(fieldList.contains("boolField"));

    // Test field access
    final Object stringValue = FLATTENER_MAKER_WITH_DISCOVERY.getRootField(convertedData, "stringField");
    assertEquals("test", stringValue);

    // Test flatten
    final var flattener = ObjectFlatteners.create(JSONPathSpec.DEFAULT, FLATTENER_MAKER_WITH_DISCOVERY);
    final Map<String, Object> flattened = flattener.flatten(convertedData);
    assertTrue(flattened.containsKey("stringField"));
    assertTrue(flattened.containsKey("numberField"));
    assertTrue(flattened.containsKey("boolField"));
  }

  @Test
  public void testFlattenWithJsonSpec()
  {
    final Map<String, Object> testData = ImmutableMap.of(
        "user", ImmutableMap.of(
            "profile", ImmutableMap.of(
                "name", "john",
                "email", "john@example.com"
            ),
            "permissions", ImmutableList.of("read", "write")
        ),
        "timestamp", 1234567890L
    );

    final JSONPathSpec flattenSpec = new JSONPathSpec(
        true,
        Lists.newArrayList(
            new JSONPathFieldSpec(JSONPathFieldType.ROOT, "timestamp", "timestamp"),
            new JSONPathFieldSpec(JSONPathFieldType.PATH, "userName", "$.user.profile.name"),
            new JSONPathFieldSpec(JSONPathFieldType.PATH, "userEmail", "$.user.profile.email"),
            new JSONPathFieldSpec(JSONPathFieldType.PATH, "firstPermission", "$.user.permissions[0]"),
            new JSONPathFieldSpec(JSONPathFieldType.JQ, "permissionCount", ".user.permissions | length")
        )
    );

    final ObjectFlattener<Map<String, Object>> flattener = ObjectFlatteners.create(
        flattenSpec,
        FLATTENER_MAKER_WITH_DISCOVERY
    );

    final Map<String, Object> flattened = flattener.flatten(testData);

    assertEquals(1234567890L, flattened.get("timestamp"));
    assertEquals("john", flattened.get("userName"));
    assertEquals("john@example.com", flattened.get("userEmail"));
    assertEquals("read", flattened.get("firstPermission"));
    assertEquals(2L, flattened.get("permissionCount")); // JQ returns Long
  }

}

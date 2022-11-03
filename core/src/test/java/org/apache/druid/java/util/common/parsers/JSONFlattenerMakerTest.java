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

package org.apache.druid.java.util.common.parsers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class JSONFlattenerMakerTest
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final JSONFlattenerMaker FLATTENER_MAKER = new JSONFlattenerMaker(true);

  @Test
  public void testStrings() throws JsonProcessingException
  {
    JsonNode node;
    Object result;

    String s1 = "hello";
    node = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(s1));
    result = FLATTENER_MAKER.finalizeConversionForMap(node);
    Assert.assertEquals(s1, result);

    String s2 = "hello \uD900";
    String s2Json = "\"hello \uD900\"";
    node = OBJECT_MAPPER.readTree(s2Json);
    result = FLATTENER_MAKER.finalizeConversionForMap(node);
    // normal equals doesn't pass for this, so check using the same
    Assert.assertArrayEquals(StringUtils.toUtf8(s2), StringUtils.toUtf8((String) result));
  }

  @Test
  public void testNumbers() throws JsonProcessingException
  {
    JsonNode node;
    Object result;
    Integer i1 = 123;
    node = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(i1));
    Assert.assertTrue(node.isInt());
    result = FLATTENER_MAKER.finalizeConversionForMap(node);
    Assert.assertEquals(i1.longValue(), result);

    Long l1 = 1L + Integer.MAX_VALUE;
    node = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(l1));
    Assert.assertTrue(node.isLong());
    result = FLATTENER_MAKER.finalizeConversionForMap(node);
    Assert.assertEquals(l1, result);

    Float f1 = 230.333f;
    node = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(f1));
    Assert.assertTrue(node.isNumber());
    result = FLATTENER_MAKER.finalizeConversionForMap(node);
    Assert.assertEquals(230.333, result);

    // float.max value plus some (using float max constant, even in a comment makes checkstyle sad)
    Double d1 = 0x1.fffffeP+127 + 100.0;
    node = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(d1));
    Assert.assertTrue(node.isDouble());
    result = FLATTENER_MAKER.finalizeConversionForMap(node);
    Assert.assertEquals(d1, result);

    BigInteger bigInt = new BigInteger(String.valueOf(Long.MAX_VALUE));
    BigInteger bigInt2 = new BigInteger(String.valueOf(Long.MAX_VALUE));
    node = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(bigInt.add(bigInt2)));
    Assert.assertTrue(node.isBigInteger());
    result = FLATTENER_MAKER.finalizeConversionForMap(node);
    Assert.assertEquals(bigInt.add(bigInt).doubleValue(), result);
  }

  @Test
  public void testBoolean() throws JsonProcessingException
  {
    Boolean bool = true;
    JsonNode node = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(bool));
    Assert.assertTrue(node.isBoolean());
    Object result = FLATTENER_MAKER.finalizeConversionForMap(node);
    Assert.assertEquals(bool, result);
  }

  @Test
  public void testBinary()
  {
    byte[] data = new byte[]{0x01, 0x02, 0x03};
    // make binary node directly for test, object mapper used in tests deserializes to TextNode with base64 string
    JsonNode node = new BinaryNode(data);
    Object result = FLATTENER_MAKER.finalizeConversionForMap(node);
    Assert.assertEquals(data, result);
  }

  @Test
  public void testNested() throws JsonProcessingException
  {
    JsonNode node;
    Object result;
    List<Integer> intArray = ImmutableList.of(1, 2, 3);
    List<Long> expectedIntArray = ImmutableList.of(1L, 2L, 3L);
    node = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(intArray));
    Assert.assertTrue(node.isArray());
    result = FLATTENER_MAKER.finalizeConversionForMap(node);
    Assert.assertEquals(expectedIntArray, result);

    Map<String, Object> theMap =
        ImmutableMap.<String, Object>builder()
                    .put("bool", true)
                    .put("int", 1)
                    .put("long", 1L)
                    .put("float", 0.11f)
                    .put("double", 0.33)
                    .put("binary", new byte[]{0x01, 0x02, 0x03})
                    .put("list", ImmutableList.of("foo", "bar", "baz"))
                    .put("anotherList", intArray)
                    .build();

    Map<String, Object> expectedMap =
        ImmutableMap.<String, Object>builder()
                    .put("bool", true)
                    .put("int", 1L)
                    .put("long", 1L)
                    .put("float", 0.11)
                    .put("double", 0.33)
                    .put("binary", StringUtils.encodeBase64String(new byte[]{0x01, 0x02, 0x03}))
                    .put("list", ImmutableList.of("foo", "bar", "baz"))
                    .put("anotherList", expectedIntArray)
                    .build();
    node = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(theMap));
    Assert.assertTrue(node.isObject());
    result = FLATTENER_MAKER.finalizeConversionForMap(node);
    Assert.assertEquals(expectedMap, result);

    List<?> theList = ImmutableList.of(
        theMap,
        theMap
    );
    List<?> expectedList = ImmutableList.of(
        expectedMap,
        expectedMap
    );
    node = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(theList));
    Assert.assertTrue(node.isArray());
    result = FLATTENER_MAKER.finalizeConversionForMap(node);
    Assert.assertEquals(expectedList, result);
  }
}

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JSONPathParserTest
{
  private static final String JSON =
      "{\"one\": \"foo\", \"two\" : [\"bar\", \"baz\"], \"three\" : \"qux\", \"four\" : null}";
  private static final String NUMBERS_JSON =
      "{\"five\" : 5.0, \"six\" : 6, \"many\" : 1234567878900, \"toomany\" : 1234567890000000000000}";
  private static final String WHACKY_CHARACTER_JSON =
      "{\"one\": \"foo\\uD900\"}";
  private static final String NESTED_JSON =
      "{\"simpleVal\":\"text\", \"ignore_me\":[1, {\"x\":2}], \"blah\":[4,5,6], \"newmet\":5, " +
      "\"foo\":{\"bar1\":\"aaa\", \"bar2\":\"bbb\"}, " +
      "\"baz\":[1,2,3], \"timestamp\":\"2999\", \"foo.bar1\":\"Hello world!\", " +
      "\"testListConvert\":[1234567890000000000000, \"foo\\uD900\"], " +
      "\"testListConvert2\":[1234567890000000000000, \"foo\\uD900\", [1234567890000000000000]], " +
      "\"testMapConvert\":{\"big\": 1234567890000000000000, \"big2\":{\"big2\":1234567890000000000000}}, " +
      "\"testEmptyList\": [], " +
      "\"hey\":[{\"barx\":\"asdf\"}], \"met\":{\"a\":[7,8,9]}}";
  private static final String NOT_JSON = "***@#%R#*(TG@(*H(#@(#@((H#(@TH@(#TH(@SDHGKJDSKJFBSBJK";

  @Test
  public void testSimple()
  {
    List<JSONPathFieldSpec> fields = new ArrayList<>();
    final Parser<String, Object> jsonParser = new JSONPathParser(new JSONPathSpec(true, fields), null, false);
    final Map<String, Object> jsonMap = jsonParser.parseToMap(JSON);
    Assertions.assertEquals(
        ImmutableMap.of("one", "foo", "two", ImmutableList.of("bar", "baz"), "three", "qux"),
        jsonMap,
        "jsonMap"
    );
  }

  @Test
  public void testWithNumbers()
  {
    List<JSONPathFieldSpec> fields = new ArrayList<>();
    final Parser<String, Object> jsonParser = new JSONPathParser(new JSONPathSpec(true, fields), null, false);
    final Map<String, Object> jsonMap = jsonParser.parseToMap(NUMBERS_JSON);
    Assertions.assertEquals(
        ImmutableMap.of("five", 5.0, "six", 6L, "many", 1234567878900L, "toomany", 1.23456789E21),
        jsonMap,
        "jsonMap"
    );
  }

  @Test
  public void testWithWhackyCharacters()
  {
    List<JSONPathFieldSpec> fields = new ArrayList<>();
    final Parser<String, Object> jsonParser = new JSONPathParser(new JSONPathSpec(true, fields), null, false);
    final Map<String, Object> jsonMap = jsonParser.parseToMap(WHACKY_CHARACTER_JSON);
    Assertions.assertEquals(
        ImmutableMap.of("one", "foo?"),
        jsonMap,
        "jsonMap"
    );
  }

  @Test
  public void testNestingWithFieldDiscovery()
  {
    List<JSONPathFieldSpec> fields = new ArrayList<>();
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.ROOT, "baz", "baz"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.PATH, "nested-foo.bar1", "$.foo.bar1"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.PATH, "nested-foo.bar2", "$.foo.bar2"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.PATH, "heybarx0", "$.hey[0].barx"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.PATH, "met-array", "$.met.a"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.ROOT, "testListConvert2", "testListConvert2"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.ROOT, "testMapConvert", "testMapConvert"));

    fields.add(new JSONPathFieldSpec(JSONPathFieldType.ROOT, "INVALID_ROOT", "INVALID_ROOT_EXPR"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.PATH, "INVALID_PATH", "INVALID_PATH_EXPR"));

    fields.add(new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq-nested-foo.bar1", ".foo.bar1"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq-nested-foo.bar2", ".foo.bar2"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq-heybarx0", ".hey[0].barx"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq-met-array", ".met.a"));


    final Parser<String, Object> jsonParser = new JSONPathParser(new JSONPathSpec(true, fields), null, false);
    final Map<String, Object> jsonMap = jsonParser.parseToMap(NESTED_JSON);

    // Root fields
    Assertions.assertEquals(ImmutableList.of(1L, 2L, 3L), jsonMap.get("baz"));
    Assertions.assertEquals(ImmutableList.of(4L, 5L, 6L), jsonMap.get("blah"));
    Assertions.assertEquals("text", jsonMap.get("simpleVal"));
    Assertions.assertEquals(5L, jsonMap.get("newmet"));
    Assertions.assertEquals("2999", jsonMap.get("timestamp"));
    Assertions.assertEquals("Hello world!", jsonMap.get("foo.bar1"));

    List<Object> testListConvert = (List) jsonMap.get("testListConvert");
    Assertions.assertEquals(1.23456789E21, testListConvert.get(0));
    Assertions.assertEquals("foo?", testListConvert.get(1));

    List<Object> testListConvert2 = (List) jsonMap.get("testListConvert2");
    Assertions.assertEquals(1.23456789E21, testListConvert2.get(0));
    Assertions.assertEquals("foo?", testListConvert2.get(1));
    Assertions.assertEquals(1.23456789E21, ((List) testListConvert2.get(2)).get(0));

    Map<String, Object> testMapConvert = (Map) jsonMap.get("testMapConvert");
    Assertions.assertEquals(1.23456789E21, testMapConvert.get("big"));
    Assertions.assertEquals(1.23456789E21, ((Map) testMapConvert.get("big2")).get("big2"));

    Assertions.assertEquals(ImmutableList.of(), jsonMap.get("testEmptyList"));

    // Nested fields
    Assertions.assertEquals("aaa", jsonMap.get("nested-foo.bar1"));
    Assertions.assertEquals("bbb", jsonMap.get("nested-foo.bar2"));
    Assertions.assertEquals("asdf", jsonMap.get("heybarx0"));
    Assertions.assertEquals(ImmutableList.of(7L, 8L, 9L), jsonMap.get("met-array"));

    Assertions.assertEquals("aaa", jsonMap.get("jq-nested-foo.bar1"));
    Assertions.assertEquals("bbb", jsonMap.get("jq-nested-foo.bar2"));
    Assertions.assertEquals("asdf", jsonMap.get("jq-heybarx0"));
    Assertions.assertEquals(ImmutableList.of(7L, 8L, 9L), jsonMap.get("jq-met-array"));

    // Fields that should not be discovered
    Assertions.assertFalse(jsonMap.containsKey("hey"));
    Assertions.assertFalse(jsonMap.containsKey("met"));
    Assertions.assertFalse(jsonMap.containsKey("ignore_me"));
    Assertions.assertFalse(jsonMap.containsKey("foo"));

    // Invalid fields
    Assertions.assertNull(jsonMap.get("INVALID_ROOT"));
    Assertions.assertNull(jsonMap.get("INVALID_PATH"));
  }

  @Test
  public void testNestingNoDiscovery()
  {
    List<JSONPathFieldSpec> fields = new ArrayList<>();
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.ROOT, "simpleVal", "simpleVal"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.ROOT, "timestamp", "timestamp"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.PATH, "nested-foo.bar2", "$.foo.bar2"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.PATH, "heybarx0", "$.hey[0].barx"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.PATH, "met-array", "$.met.a"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq-nested-foo.bar2", ".foo.bar2"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq-heybarx0", ".hey[0].barx"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq-met-array", ".met.a"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.TREE, "tree-simpleVal", null, Collections.singletonList("simpleVal")));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.TREE, "tree-timestamp", null, Collections.singletonList("timestamp")));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.TREE, "tree-nested-foo.bar2", null, Arrays.asList("foo", "bar2")));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.TREE, "tree-met-array", null, Arrays.asList("met", "a")));

    final Parser<String, Object> jsonParser = new JSONPathParser(new JSONPathSpec(false, fields), null, false);
    final Map<String, Object> jsonMap = jsonParser.parseToMap(NESTED_JSON);

    // Root fields
    Assertions.assertEquals("text", jsonMap.get("simpleVal"));
    Assertions.assertEquals("2999", jsonMap.get("timestamp"));
    Assertions.assertEquals("text", jsonMap.get("tree-simpleVal"));
    Assertions.assertEquals("2999", jsonMap.get("tree-timestamp"));

    // Nested fields
    Assertions.assertEquals("bbb", jsonMap.get("nested-foo.bar2"));
    Assertions.assertEquals("asdf", jsonMap.get("heybarx0"));
    Assertions.assertEquals(ImmutableList.of(7L, 8L, 9L), jsonMap.get("met-array"));
    Assertions.assertEquals("bbb", jsonMap.get("jq-nested-foo.bar2"));
    Assertions.assertEquals("asdf", jsonMap.get("jq-heybarx0"));
    Assertions.assertEquals(ImmutableList.of(7L, 8L, 9L), jsonMap.get("jq-met-array"));

    Assertions.assertEquals(ImmutableList.of(7L, 8L, 9L), jsonMap.get("tree-met-array"));
    Assertions.assertEquals("bbb", jsonMap.get("tree-nested-foo.bar2"));

    // Fields that should not be discovered
    Assertions.assertFalse(jsonMap.containsKey("newmet"));
    Assertions.assertFalse(jsonMap.containsKey("foo.bar1"));
    Assertions.assertFalse(jsonMap.containsKey("baz"));
    Assertions.assertFalse(jsonMap.containsKey("blah"));
    Assertions.assertFalse(jsonMap.containsKey("nested-foo.bar1"));
    Assertions.assertFalse(jsonMap.containsKey("hey"));
    Assertions.assertFalse(jsonMap.containsKey("met"));
    Assertions.assertFalse(jsonMap.containsKey("ignore_me"));
    Assertions.assertFalse(jsonMap.containsKey("foo"));
  }

  @Test
  public void testRejectDuplicates()
  {
    List<JSONPathFieldSpec> fields = new ArrayList<>();
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.PATH, "met-array", "$.met.a"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.PATH, "met-array", "$.met.a"));

    final IllegalArgumentException e1 = Assertions.assertThrows(IllegalArgumentException.class, () -> {
      final Parser<String, Object> jsonParser = new JSONPathParser(new JSONPathSpec(false, fields), null, false);
      jsonParser.parseToMap(NESTED_JSON);
    });
    Assertions.assertTrue(e1.getMessage().contains("Cannot have duplicate field definition: met-array"));
  }

  @Test
  public void testRejectDuplicates2()
  {
    List<JSONPathFieldSpec> fields = new ArrayList<>();
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.PATH, "met-array", "$.met.a"));
    fields.add(new JSONPathFieldSpec(JSONPathFieldType.JQ, "met-array", ".met.a"));

    final IllegalArgumentException e2 = Assertions.assertThrows(IllegalArgumentException.class, () -> {
      final Parser<String, Object> jsonParser = new JSONPathParser(new JSONPathSpec(false, fields), null, false);
      jsonParser.parseToMap(NESTED_JSON);
    });
    Assertions.assertTrue(e2.getMessage().contains("Cannot have duplicate field definition: met-array"));
  }

  @Test
  public void testParseFail()
  {
    List<JSONPathFieldSpec> fields = new ArrayList<>();

    final ParseException e = Assertions.assertThrows(ParseException.class, () -> {
      final Parser<String, Object> jsonParser = new JSONPathParser(new JSONPathSpec(true, fields), null, false);
      jsonParser.parseToMap(NOT_JSON);
    });
    Assertions.assertTrue(e.getMessage().contains("Unable to parse row [" + NOT_JSON + "]"));
  }

  @Test
  public void testJSONPathFunctions()
  {
    List<JSONPathFieldSpec> fields = Arrays.asList(
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "met-array-length", "$.met.a.length()"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "met-array-min", "$.met.a.min()"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "met-array-max", "$.met.a.max()"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "met-array-avg", "$.met.a.avg()"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "met-array-sum", "$.met.a.sum()"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "met-array-stddev", "$.met.a.stddev()"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "met-array-append", "$.met.a.append(10)"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "concat", "$.concat($.foo.bar1, $.foo.bar2)")
    );

    final Parser<String, Object> jsonParser = new JSONPathParser(new JSONPathSpec(true, fields), null, false);
    final Map<String, Object> jsonMap = jsonParser.parseToMap(NESTED_JSON);

    // values of met.a array are: 7,8,9
    Assertions.assertEquals(3, jsonMap.get("met-array-length"));
    Assertions.assertEquals(7.0, jsonMap.get("met-array-min"));
    Assertions.assertEquals(9.0, jsonMap.get("met-array-max"));
    Assertions.assertEquals(8.0, jsonMap.get("met-array-avg"));
    Assertions.assertEquals(24.0, jsonMap.get("met-array-sum"));

    //deviation of [7,8,9] is 1/3, stddev is sqrt(1/3), approximately 0.8165
    Assertions.assertEquals(0.8165, (double) jsonMap.get("met-array-stddev"), 0.00001);

    Assertions.assertEquals(ImmutableList.of(7L, 8L, 9L, 10L), jsonMap.get("met-array-append"));
    Assertions.assertEquals("aaabbb", jsonMap.get("concat"));
  }

}

/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.java.util.common.parsers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JSONPathParserTest
{
  private static final String json =
      "{\"one\": \"foo\", \"two\" : [\"bar\", \"baz\"], \"three\" : \"qux\", \"four\" : null}";
  private static final String numbersJson =
      "{\"five\" : 5.0, \"six\" : 6, \"many\" : 1234567878900, \"toomany\" : 1234567890000000000000}";
  private static final String whackyCharacterJson =
      "{\"one\": \"foo\\uD900\"}";
  private static final String nestedJson =
      "{\"simpleVal\":\"text\", \"ignore_me\":[1, {\"x\":2}], \"blah\":[4,5,6], \"newmet\":5, " +
      "\"foo\":{\"bar1\":\"aaa\", \"bar2\":\"bbb\"}, " +
      "\"baz\":[1,2,3], \"timestamp\":\"2999\", \"foo.bar1\":\"Hello world!\", " +
      "\"testListConvert\":[1234567890000000000000, \"foo\\uD900\"], " +
      "\"testListConvert2\":[1234567890000000000000, \"foo\\uD900\", [1234567890000000000000]], " +
      "\"testMapConvert\":{\"big\": 1234567890000000000000, \"big2\":{\"big2\":1234567890000000000000}}, " +
      "\"testEmptyList\": [], " +
      "\"hey\":[{\"barx\":\"asdf\"}], \"met\":{\"a\":[7,8,9]}}";
  private static final String notJson = "***@#%R#*(TG@(*H(#@(#@((H#(@TH@(#TH(@SDHGKJDSKJFBSBJK";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSimple()
  {
    List<JSONPathParser.FieldSpec> fields = new ArrayList<>();
    final Parser<String, Object> jsonParser = new JSONPathParser(fields, true, null);
    final Map<String, Object> jsonMap = jsonParser.parse(json);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("one", "foo", "two", ImmutableList.of("bar", "baz"), "three", "qux"),
        jsonMap
    );
  }

  @Test
  public void testWithNumbers()
  {
    List<JSONPathParser.FieldSpec> fields = new ArrayList<>();
    final Parser<String, Object> jsonParser = new JSONPathParser(fields, true, null);
    final Map<String, Object> jsonMap = jsonParser.parse(numbersJson);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("five", 5.0, "six", 6L, "many", 1234567878900L, "toomany", 1.23456789E21),
        jsonMap
    );
  }

  @Test
  public void testWithWhackyCharacters()
  {
    List<JSONPathParser.FieldSpec> fields = new ArrayList<>();
    final Parser<String, Object> jsonParser = new JSONPathParser(fields, true, null);
    final Map<String, Object> jsonMap = jsonParser.parse(whackyCharacterJson);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("one", "foo?"),
        jsonMap
    );
  }

  @Test
  public void testNestingWithFieldDiscovery()
  {
    List<JSONPathParser.FieldSpec> fields = new ArrayList<>();
    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.ROOT, "baz", "baz"));
    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.PATH, "nested-foo.bar1", "$.foo.bar1"));
    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.PATH, "nested-foo.bar2", "$.foo.bar2"));
    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.PATH, "heybarx0", "$.hey[0].barx"));
    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.PATH, "met-array", "$.met.a"));
    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.ROOT, "testListConvert2", "testListConvert2"));
    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.ROOT, "testMapConvert", "testMapConvert"));

    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.ROOT, "INVALID_ROOT", "INVALID_ROOT_EXPR"));
    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.PATH, "INVALID_PATH", "INVALID_PATH_EXPR"));


    final Parser<String, Object> jsonParser = new JSONPathParser(fields, true, null);
    final Map<String, Object> jsonMap = jsonParser.parse(nestedJson);

    // Root fields
    Assert.assertEquals(ImmutableList.of(1L, 2L, 3L), jsonMap.get("baz"));
    Assert.assertEquals(ImmutableList.of(4L, 5L, 6L), jsonMap.get("blah"));
    Assert.assertEquals("text", jsonMap.get("simpleVal"));
    Assert.assertEquals(5L, jsonMap.get("newmet"));
    Assert.assertEquals("2999", jsonMap.get("timestamp"));
    Assert.assertEquals("Hello world!", jsonMap.get("foo.bar1"));

    List<Object> testListConvert = (List)jsonMap.get("testListConvert");
    Assert.assertEquals(1.23456789E21, testListConvert.get(0));
    Assert.assertEquals("foo?", testListConvert.get(1));

    List<Object> testListConvert2 = (List)jsonMap.get("testListConvert2");
    Assert.assertEquals(1.23456789E21, testListConvert2.get(0));
    Assert.assertEquals("foo?", testListConvert2.get(1));
    Assert.assertEquals(1.23456789E21, ((List) testListConvert2.get(2)).get(0));

    Map<String, Object> testMapConvert = (Map) jsonMap.get("testMapConvert");
    Assert.assertEquals(1.23456789E21, testMapConvert.get("big"));
    Assert.assertEquals(1.23456789E21, ((Map) testMapConvert.get("big2")).get("big2"));

    Assert.assertEquals(ImmutableList.of(), jsonMap.get("testEmptyList"));

    // Nested fields
    Assert.assertEquals("aaa", jsonMap.get("nested-foo.bar1"));
    Assert.assertEquals("bbb", jsonMap.get("nested-foo.bar2"));
    Assert.assertEquals("asdf", jsonMap.get("heybarx0"));
    Assert.assertEquals(ImmutableList.of(7L, 8L, 9L), jsonMap.get("met-array"));

    // Fields that should not be discovered
    Assert.assertNull(jsonMap.get("hey"));
    Assert.assertNull(jsonMap.get("met"));
    Assert.assertNull(jsonMap.get("ignore_me"));
    Assert.assertNull(jsonMap.get("foo"));

    // Invalid fields
    Assert.assertNull(jsonMap.get("INVALID_ROOT"));
    Assert.assertNull(jsonMap.get("INVALID_PATH"));
  }

  @Test
  public void testNestingNoDiscovery()
  {
    List<JSONPathParser.FieldSpec> fields = new ArrayList<>();
    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.ROOT, "simpleVal", "simpleVal"));
    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.ROOT, "timestamp", "timestamp"));
    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.PATH, "nested-foo.bar2", "$.foo.bar2"));
    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.PATH, "heybarx0", "$.hey[0].barx"));
    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.PATH, "met-array", "$.met.a"));

    final Parser<String, Object> jsonParser = new JSONPathParser(fields, false, null);
    final Map<String, Object> jsonMap = jsonParser.parse(nestedJson);

    // Root fields
    Assert.assertEquals("text", jsonMap.get("simpleVal"));
    Assert.assertEquals("2999", jsonMap.get("timestamp"));

    // Nested fields
    Assert.assertEquals("bbb", jsonMap.get("nested-foo.bar2"));
    Assert.assertEquals("asdf", jsonMap.get("heybarx0"));
    Assert.assertEquals(ImmutableList.of(7L, 8L, 9L), jsonMap.get("met-array"));

    // Fields that should not be discovered
    Assert.assertNull(jsonMap.get("newmet"));
    Assert.assertNull(jsonMap.get("foo.bar1"));
    Assert.assertNull(jsonMap.get("baz"));
    Assert.assertNull(jsonMap.get("blah"));
    Assert.assertNull(jsonMap.get("nested-foo.bar1"));
    Assert.assertNull(jsonMap.get("hey"));
    Assert.assertNull(jsonMap.get("met"));
    Assert.assertNull(jsonMap.get("ignore_me"));
    Assert.assertNull(jsonMap.get("foo"));
  }

  @Test
  public void testRejectDuplicates()
  {
    List<JSONPathParser.FieldSpec> fields = new ArrayList<>();
    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.PATH, "met-array", "$.met.a"));
    fields.add(new JSONPathParser.FieldSpec(JSONPathParser.FieldType.PATH, "met-array", "$.met.a"));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Cannot have duplicate field definition: met-array");

    final Parser<String, Object> jsonParser = new JSONPathParser(fields, false, null);
    final Map<String, Object> jsonMap = jsonParser.parse(nestedJson);
  }

  @Test
  public void testParseFail()
  {
    List<JSONPathParser.FieldSpec> fields = new ArrayList<>();

    thrown.expect(ParseException.class);
    thrown.expectMessage("Unable to parse row [" + notJson + "]");

    final Parser<String, Object> jsonParser = new JSONPathParser(fields, true, null);
    final Map<String, Object> jsonMap = jsonParser.parse(notJson);
  }
}

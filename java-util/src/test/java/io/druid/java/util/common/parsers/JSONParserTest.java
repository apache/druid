/*
 * Copyright 2011 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.java.util.common.parsers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Map;

public class JSONParserTest
{
  private static final String json = "{\"one\": \"foo\", \"two\" : [\"bar\", \"baz\"], \"three\" : \"qux\", \"four\" : null}";
  private static final String numbersJson = "{\"five\" : 5.0, \"six\" : 6, \"many\" : 1234567878900, \"toomany\" : 1234567890000000000000}";
  private static final String whackyCharacterJson = "{\"one\": \"foo\\uD900\"}";

  @Test
  public void testSimple()
  {
    final Parser<String, Object> jsonParser = new JSONParser();
    final Map<String, Object> jsonMap = jsonParser.parse(json);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("one", "foo", "two", ImmutableList.of("bar", "baz"), "three", "qux"),
        jsonMap
    );
  }

  @Test
  public void testSimpleWithFields()
  {
    final Parser<String, Object> jsonParser = new JSONParser(new ObjectMapper(), Lists.newArrayList("two"));
    final Map<String, Object> jsonMap = jsonParser.parse(json);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("two", ImmutableList.of("bar", "baz")),
        jsonMap
    );
  }

  @Test
  public void testSimpleWithExclude()
  {
    final Parser<String, Object> jsonParser = new JSONParser(new ObjectMapper(), null, Lists.newArrayList("two"));
    final Map<String, Object> jsonMap = jsonParser.parse(json);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("one", "foo", "three", "qux"),
        jsonMap
    );
  }

  @Test
  public void testWithWhackyCharacters()
  {
    final Parser<String, Object> jsonParser = new JSONParser();
    final Map<String, Object> jsonMap = jsonParser.parse(whackyCharacterJson);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("one", "foo?"),
        jsonMap
    );
  }

  @Test
  public void testWithFields()
  {
    final Parser<String, Object> jsonParser = new JSONParser();
    jsonParser.setFieldNames(ImmutableList.of("two", "three", "five"));
    final Map<String, Object> jsonMap = jsonParser.parse(json);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("two", ImmutableList.of("bar", "baz"), "three", "qux"),
        jsonMap
    );
  }

  @Test
  public void testWithNumbers()
  {
    final Parser<String, Object> jsonParser = new JSONParser();
    final Map<String, Object> jsonMap = jsonParser.parse(numbersJson);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("five", 5.0, "six", 6L, "many", 1234567878900L, "toomany", 1.23456789E21),
        jsonMap
    );
  }
}

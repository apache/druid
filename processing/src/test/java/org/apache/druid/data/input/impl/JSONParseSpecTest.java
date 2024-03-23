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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.Parser;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JSONParseSpecTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testParseRow()
  {
    final JSONParseSpec parseSpec = new JSONParseSpec(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz", "baz"),
                new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz2", "baz2"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg", "$.o.mg"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg2", "$.o.mg2"),
                new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg", ".o.mg"),
                new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg2", ".o.mg2")
            )
        ),
        null,
        false
    );

    final Map<String, Object> expected = new HashMap<>();
    expected.put("foo", "x");
    expected.put("baz", 4L);
    expected.put("root_baz", 4L);
    expected.put("root_baz2", null);
    expected.put("path_omg", 1L);
    expected.put("path_omg2", null);
    expected.put("jq_omg", 1L);
    expected.put("jq_omg2", null);

    final Parser<String, Object> parser = parseSpec.makeParser();
    final Map<String, Object> parsedRow = parser.parseToMap("{\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}");
    Assert.assertNotNull(parsedRow);
    Assert.assertEquals(expected, parsedRow);
    Assert.assertNull(parsedRow.get("bar"));
    Assert.assertNull(parsedRow.get("buzz"));
    Assert.assertNull(parsedRow.get("root_baz2"));
    Assert.assertNull(parsedRow.get("jq_omg2"));
    Assert.assertNull(parsedRow.get("path_omg2"));
  }

  @Test
  public void testParseRowWithConditional()
  {
    final JSONParseSpec parseSpec = new JSONParseSpec(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("foo"))),
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "foo", "$.[?(@.maybe_object)].maybe_object.foo.test"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "baz", "$.maybe_object_2.foo.test"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bar", "$.[?(@.something_else)].something_else.foo")
            )
        ),
        null,
        false
    );

    final Map<String, Object> expected = new HashMap<>();
    expected.put("foo", Collections.singletonList(null));
    expected.put("baz", null);
    expected.put("bar", Collections.singletonList("test"));

    final Parser<String, Object> parser = parseSpec.makeParser();
    final Map<String, Object> parsedRow = parser.parseToMap("{\"something_else\": {\"foo\": \"test\"}}");

    Assert.assertNotNull(parsedRow);
    Assert.assertEquals(expected, parsedRow);
  }

  @Test
  public void testParseRowWithNullsInArrays()
  {
    final JSONParseSpec parseSpec = new JSONParseSpec(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("foo"))),
        new JSONPathSpec(
            true,
            ImmutableList.of(
                // https://github.com/apache/druid/issues/6653 $.x.y.z where y is missing
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "foo", "$.baz.[?(@.maybe_object)].maybe_object"),
                // https://github.com/apache/druid/issues/6653 $.x.y.z where y is from an array and is null
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "nullFoo", "$.nullFoo.[?(@.value)][0].foo"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "baz", "$.baz"),
                // $.x.y.z where x is from an array and is null
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "nullBaz", "$.baz[1].foo.maybe_object"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bar", "$.[?(@.something_else)].something_else.foo")
            )
        ),
        null,
        false
    );

    final Map<String, Object> expected = new HashMap<>();
    expected.put("foo", new ArrayList<>());
    expected.put("baz", Arrays.asList("1", null, "2", null));
    expected.put("bar", Collections.singletonList("test"));
    expected.put("nullFoo", new ArrayList<>());
    expected.put("nullBaz", null);

    final Parser<String, Object> parser = parseSpec.makeParser();
    final Map<String, Object> parsedRow = parser.parseToMap("{\"baz\":[\"1\",null,\"2\",null],\"nullFoo\":{\"value\":[null,null]},\"something_else\": {\"foo\": \"test\"}}");

    Assert.assertNotNull(parsedRow);
    Assert.assertEquals(expected, parsedRow);
  }

  @Test
  public void testSerde() throws IOException
  {
    HashMap<String, Boolean> feature = new HashMap<String, Boolean>();
    feature.put("ALLOW_UNQUOTED_CONTROL_CHARS", true);
    JSONParseSpec spec = new JSONParseSpec(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
        null,
        feature,
        false
    );

    final JSONParseSpec serde = (JSONParseSpec) jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        ParseSpec.class
    );
    Assert.assertEquals("timestamp", serde.getTimestampSpec().getTimestampColumn());
    Assert.assertEquals("iso", serde.getTimestampSpec().getTimestampFormat());

    Assert.assertEquals(Arrays.asList("bar", "foo"), serde.getDimensionsSpec().getDimensionNames());
    Assert.assertEquals(feature, serde.getFeatureSpec());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(JSONParseSpec.class)
              .usingGetClass()
              .withPrefabValues(
                DimensionsSpec.class,
                new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
                new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("baz", "buzz")))
              )
              .withPrefabValues(
              ObjectMapper.class,
              new ObjectMapper(),
              new ObjectMapper()
              )
              .withIgnoredFields("objectMapper")
              .verify();
  }
}

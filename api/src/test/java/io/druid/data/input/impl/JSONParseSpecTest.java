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

package io.druid.data.input.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.druid.TestObjectMapper;
import io.druid.java.util.common.parsers.JSONPathFieldSpec;
import io.druid.java.util.common.parsers.JSONPathFieldType;
import io.druid.java.util.common.parsers.JSONPathSpec;
import io.druid.java.util.common.parsers.Parser;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class JSONParseSpecTest
{
  private final ObjectMapper jsonMapper = new TestObjectMapper();

  @Test
  public void testParseRow()
  {
    final JSONParseSpec parseSpec = new JSONParseSpec(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo")), null, null),
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
        null
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
    final Map<String, Object> parsedRow = parser.parse("{\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}");
    Assert.assertNotNull(parsedRow);
    Assert.assertEquals(expected, parsedRow);
    Assert.assertNull(parsedRow.get("bar"));
    Assert.assertNull(parsedRow.get("buzz"));
    Assert.assertNull(parsedRow.get("root_baz2"));
    Assert.assertNull(parsedRow.get("jq_omg2"));
    Assert.assertNull(parsedRow.get("path_omg2"));
  }

  @Test
  public void testSerde() throws IOException
  {
    HashMap<String, Boolean> feature = new HashMap<String, Boolean>();
    feature.put("ALLOW_UNQUOTED_CONTROL_CHARS", true);
    JSONParseSpec spec = new JSONParseSpec(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo")), null, null),
        null,
        feature
    );

    final JSONParseSpec serde = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        JSONParseSpec.class
    );
    Assert.assertEquals("timestamp", serde.getTimestampSpec().getTimestampColumn());
    Assert.assertEquals("iso", serde.getTimestampSpec().getTimestampFormat());

    Assert.assertEquals(Arrays.asList("bar", "foo"), serde.getDimensionsSpec().getDimensionNames());
    Assert.assertEquals(feature, serde.getFeatureSpec());
  }
}

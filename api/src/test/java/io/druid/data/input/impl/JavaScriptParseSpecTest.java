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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import io.druid.TestObjectMapper;
import io.druid.java.util.common.parsers.Parser;
import io.druid.js.JavaScriptConfig;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 */
public class JavaScriptParseSpecTest
{
  private final ObjectMapper jsonMapper = new TestObjectMapper();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws IOException
  {
    jsonMapper.setInjectableValues(
        new InjectableValues.Std().addValue(
            JavaScriptConfig.class,
            JavaScriptConfig.getDefault()
        )
    );
    JavaScriptParseSpec spec = new JavaScriptParseSpec(
        new TimestampSpec("abc", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("abc")), null, null),
        "abc",
        JavaScriptConfig.getDefault()
    );
    final JavaScriptParseSpec serde = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        JavaScriptParseSpec.class
    );
    Assert.assertEquals("abc", serde.getTimestampSpec().getTimestampColumn());
    Assert.assertEquals("iso", serde.getTimestampSpec().getTimestampFormat());

    Assert.assertEquals("abc", serde.getFunction());
    Assert.assertEquals(Arrays.asList("abc"), serde.getDimensionsSpec().getDimensionNames());
  }

  @Test
  public void testMakeParser()
  {
    final JavaScriptConfig config = JavaScriptConfig.getDefault();
    JavaScriptParseSpec spec = new JavaScriptParseSpec(
        new TimestampSpec("abc", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("abc")), null, null),
        "function(str) { var parts = str.split(\"-\"); return { one: parts[0], two: parts[1] } }",
        config
    );

    final Parser<String, Object> parser = spec.makeParser();
    final Map<String, Object> obj = parser.parse("x-y");
    Assert.assertEquals(ImmutableMap.of("one", "x", "two", "y"), obj);
  }

  @Test
  public void testMakeParserNotAllowed()
  {
    final JavaScriptConfig config = new JavaScriptConfig(true);
    JavaScriptParseSpec spec = new JavaScriptParseSpec(
        new TimestampSpec("abc", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("abc")), null, null),
        "abc",
        config
    );

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("JavaScript is disabled");
    spec.makeParser();
  }
}

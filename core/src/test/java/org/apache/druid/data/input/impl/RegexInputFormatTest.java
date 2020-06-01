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
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputFormat;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class RegexInputFormatTest
{
  private final ObjectMapper mapper;

  public RegexInputFormatTest()
  {
    mapper = new ObjectMapper();
    mapper.registerSubtypes(new NamedType(RegexInputFormat.class, "regex"));
  }

  @Test
  public void testSerde() throws IOException
  {
    final RegexInputFormat expected = new RegexInputFormat(
        "//[^\\r\\n]*[\\r\\n]",
        "|",
        ImmutableList.of("col1", "col2", "col3")
    );

    final byte[] json = mapper.writeValueAsBytes(expected);
    final RegexInputFormat fromJson = (RegexInputFormat) mapper.readValue(json, InputFormat.class);

    Assert.assertEquals(expected.getPattern(), fromJson.getPattern());
    Assert.assertEquals(expected.getListDelimiter(), fromJson.getListDelimiter());
    Assert.assertEquals(expected.getColumns(), fromJson.getColumns());
  }

  @Test
  public void testIgnoreCompiledPatternInJson() throws IOException
  {
    final RegexInputFormat expected = new RegexInputFormat(
        "//[^\\r\\n]*[\\r\\n]",
        "|",
        ImmutableList.of("col1", "col2", "col3")
    );

    final byte[] json = mapper.writeValueAsBytes(expected);
    final Map<String, Object> map = mapper.readValue(json, Map.class);
    Assert.assertFalse(map.containsKey("compiledPattern"));
  }
}

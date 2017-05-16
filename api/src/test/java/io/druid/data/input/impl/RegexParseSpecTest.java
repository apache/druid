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
import io.druid.TestObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

/**
 */
public class RegexParseSpecTest
{
  private final ObjectMapper jsonMapper = new TestObjectMapper();

  @Test
  public void testSerde() throws IOException
  {
    RegexParseSpec spec = new RegexParseSpec(
        new TimestampSpec("abc", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Collections.singletonList("abc")), null, null),
        "\u0001",
        Collections.singletonList("abc"),
        "abc"
    );
    final RegexParseSpec serde = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        RegexParseSpec.class
    );
    Assert.assertEquals("abc", serde.getTimestampSpec().getTimestampColumn());
    Assert.assertEquals("iso", serde.getTimestampSpec().getTimestampFormat());

    Assert.assertEquals("abc", serde.getPattern());
    Assert.assertEquals("\u0001", serde.getListDelimiter());
    Assert.assertEquals(Collections.singletonList("abc"), serde.getDimensionsSpec().getDimensionNames());
  }
}
